package cc

import (
  "time"

  "./op_log"
)


func (cc *CrowdControl) Set(args *SetArgs, response *SetResponse) error {
  key, value := args.Key, args.Value
  setMutex := cc.acquireSetMutex(key)
  cc.mutex.Lock()

  if cc.primary != cc.me {
    // only the primary can handle set requests
    response.Status = SET_REFUSED
    cc.mutex.Unlock()
    cc.releaseSetMutex(key, setMutex)
    return nil
  }

  curValue, exists := cc.cache.Get(key)
  if exists && curValue == value {
    // already set this value (set likely in progress from earlier)
    response.Status = SET_SUCCESS
    cc.mutex.Unlock()
    cc.releaseSetMutex(key, setMutex)
    return nil
  }

  cc.cache.Set(key, value)

  // don't worry about leases changed during this set() operation; they don't
  // compromise safety, since they will happen after the Prep() RPC
  savedLeasesUntil := append([]time.Time(nil), cc.grantedLeasesUntil...)

  op := op_log.AddOperation{Key: key}
  cc.appendOp(op)
  op.Perform(&cc.filters)

  majority := false
  waitTime := WAIT_TIME_INITIAL
  var nextOpNum int

  for !majority {
    numPrepped := 0
    refused := false
    nextOpNum = cc.ol.GetNextOpNum()

    rpcCh := makeParallelRPCs(cc.nodes,
      // sends a Prep RPC to the given peer
      func(node int) chan *RPCReply {
        invalid, ops := cc.ol.GetPending(node)
        args := &PrepArgs{
          View: cc.view,
          Invalid: invalid,
          StartOpNum: nextOpNum - len(ops),
          NextOpNum: nextOpNum,
          Key: key,
          Ops: ops,
        }

        return cc.rts[node].MakeRPCRetry("CrowdControl.Prep", args, &PrepResponse{},
          int(PREP_RPCS_TIMEOUT / RPC_TIMEOUT))
      },

      // aggregates Prep replies; determines when to stop collecting them
      func(reply *RPCReply) bool {
        cc.mutex.Lock()
        defer cc.mutex.Unlock()

        if reply.Success {
          // compute number of granted and already granted votes
          prepResponse := reply.Data.(*PrepResponse)

          if prepResponse.Status == PREP_SUCCESS {
            // this node is up-to-date, so we can ignore its lease
            savedLeasesUntil[reply.Node] = time.Now()
            numPrepped += 1

            if numPrepped > cc.numPeers / 2 {
              return true
            }
          } else if prepResponse.Status == PREP_REFUSED {
            refused = true
            return true
          }
        }
        return false
      }, PREP_RPCS_TIMEOUT)


    originalView := cc.view
    cc.mutex.Unlock()

    // wait for replies; don't block other requests
    replies := <-rpcCh

    cc.mutex.Lock()

    if cc.view > originalView || refused {
      // we've made a transition to a new view; don't proceed
      cc.mutex.Unlock()
      cc.releaseSetMutex(key, setMutex)
      return ErrViewChange
    }

    majority = (numPrepped > cc.numPeers / 2)
    if majority {
      for _, reply := range replies {
        if reply.Success && reply.Data.(*PrepResponse).Status == PREP_SUCCESS &&
            reply.Node != cc.me {
          cc.ol.FastForward(reply.Node, nextOpNum)
        }
      }

      // successfully prepped
      // log.Printf("CC[%v] set by %v/%v peers\n", cc.me, numPrepped, cc.numPeers)
    } else {
      cc.mutex.Unlock()

      // throttle requests
      time.Sleep(waitTime)
      if waitTime < WAIT_TIME_MAX {
        waitTime *= WAIT_TIME_MULTIPLICATIVE_INCREASE
      }

      cc.mutex.Lock()
      if cc.view > originalView {
        // we've made a transition to a new view; don't proceed
        cc.mutex.Unlock()
        cc.releaseSetMutex(key, setMutex)
        return ErrViewChange
      }
    }
  }

  commitDoneCh := make(chan bool, 1)
  go func() {
    cc.mutex.Lock()
    committedNodes := make([]int, 0, cc.numPeers)

    rpcCh := makeParallelRPCs(cc.nodes,
      // sends a Commit RPC to the given peer
      func(node int) chan *RPCReply {
        args := &CommitArgs{
          View: cc.view,
          Key: key,
          Value: value,
          NextOpNum: nextOpNum,
        }

        response := &CommitResponse{}
        return cc.rts[node].MakeRPCRetry("CrowdControl.Commit", args, response,
          int(COMMIT_RPCS_TIMEOUT / RPC_TIMEOUT))
      },

      // aggregates Commit replies; determines when to stop collecting them
      func(reply *RPCReply) bool {
        cc.mutex.Lock()
        defer cc.mutex.Unlock()
        // note: view change doesn't compromise safety here

        if reply.Success {
          // compute number of granted and already granted votes
          commitResponse := reply.Data.(*CommitResponse)
          if commitResponse.Success {
            cc.filters[reply.Node].Remove([]byte(key))
            committedNodes = append(committedNodes, reply.Node)
          }
        }
        return false
      }, COMMIT_RPCS_TIMEOUT)

    originalView := cc.view
    cc.mutex.Unlock()

    <-rpcCh

    cc.mutex.Lock()

    if cc.view > originalView {
      cc.mutex.Unlock()
      commitDoneCh <- true
      return
    }

    op := op_log.RemoveOperation{Key: key, Nodes: committedNodes}
    cc.appendOp(op)

    cc.mutex.Unlock()
    commitDoneCh <- true
  }()

  // before returning response, we must ensure leases expire or are revoked
  leasedNodes, timeout := cc.getLeasedNodes(savedLeasesUntil)
  numLeased := len(leasedNodes)

  originalView := cc.view
  // must unlock *after* processing savedLeasesUntil, since it could be
  // modified by Commit() RPC reply handler
  cc.mutex.Unlock()

  if numLeased == 0 {
    // no leases to wait for
  } else if timeout <= RPC_TIMEOUT {
    // timeout is short enough that it's not worth making RPC calls
    <-time.After(timeout)
  } else {
    <-cc.revokeLeases_mu(leasedNodes, originalView)

    // if we couldn't revoke leases, we've waited long enough such that the
    // other nodes would start elections, clearing their leases
  }

  go func() {
    // commit needs to finish before releasing set mutex
    <-commitDoneCh
    cc.releaseSetMutex(key, setMutex)
  }()

  cc.mutex.Lock()
  if cc.view > originalView {
    return ErrViewChange
  }
  cc.mutex.Unlock()

  response.Status = SET_SUCCESS
  return nil
}


func (cc *CrowdControl) acquireSetMutex(key string) *SetMutex {
  cc.mutex.Lock()
  setMutex, exists := cc.setMutexes[key]

  if !exists {
    setMutex = &SetMutex{NumUsers: 1}
    cc.setMutexes[key] = setMutex
  } else {
    setMutex.NumUsers += 1
  }
  cc.mutex.Unlock()

  setMutex.Mutex.Lock()
  return setMutex
}


func (cc *CrowdControl) releaseSetMutex(key string, setMutex *SetMutex) {
  setMutex.Mutex.Unlock()

  cc.mutex.Lock()
  setMutex.NumUsers -= 1
  if setMutex.NumUsers == 0 {
    delete(cc.setMutexes, key)
  }
  cc.mutex.Unlock()
}


func (cc *CrowdControl) Prep(args *PrepArgs, response *PrepResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.view > args.View {
    response.Status = PREP_REFUSED
    return nil
  }

  if cc.view < args.View {
    response.Status = PREP_DELAYED
    return nil
  }

  startIndex := cc.nextOpNum - args.StartOpNum
  if args.Invalid || startIndex < 0 {
    // TODO: must recover
    response.Status = PREP_DELAYED
    return nil
  }

  numOps := len(args.Ops)
  for i := startIndex; i < numOps; i++ {
    args.Ops[i].Perform(&cc.filters)
  }

  // key will take time to commit; don't allow an incoming get request to
  // trigger a RequestKVPair() RPC
  cc.inflightKeys[args.Key] = time.Now()

  if cc.nextOpNum < args.NextOpNum {
    cc.nextOpNum = args.NextOpNum
  }

  response.Status = PREP_SUCCESS
  return nil
}


func (cc *CrowdControl) Commit(args *CommitArgs, response *CommitResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.view != args.View {
    response.Success = false
    return nil
  }

  if cc.nextOpNum < args.NextOpNum {
    // can't commit if we aren't up-to-date
    response.Success = false
    return nil
  }

  // KV pair has been received 
  delete(cc.inflightKeys, args.Key)

  cc.cache.Set(args.Key, args.Value)
  cc.filters[cc.me].Remove([]byte(args.Key))

  response.Success = true
  return nil
}


func (cc *CrowdControl) appendOp(op op_log.Operation) {
  cc.ol.Append(op)

  // primary is always up-to-date
  cc.nextOpNum = cc.ol.GetNextOpNum()
  cc.ol.FastForward(cc.me, cc.nextOpNum)
}


func (cc *CrowdControl) getLeasedNodes(grantedLeasesUntil []time.Time) ([]int, time.Duration) {
  leasedNodes := make([]int, 0, cc.numPeers)
  now := time.Now()

  lastLeaseExpiry := grantedLeasesUntil[0]
  for node, leaseExpiry := range grantedLeasesUntil {
    if leaseExpiry.Sub(lastLeaseExpiry) > 0 {
      lastLeaseExpiry = leaseExpiry
    }

    if leaseExpiry.Sub(now) > 0 {
      leasedNodes = append(leasedNodes, node)
    }
  }

  timeout := lastLeaseExpiry.Sub(now)
  return leasedNodes, timeout
}


func (cc *CrowdControl) revokeLeases_mu(leasedNodes []int, view int) chan []*RPCReply {
  args := &RevokeLeaseArgs{View: view}

  rpcCh := makeParallelRPCs(leasedNodes,
    // sends a RevokeLease RPC to the given peer
    func(node int) chan *RPCReply {
      return cc.rts[node].MakeRPCRetry("CrowdControl.RevokeLease", args,
        &RevokeLeaseResponse{}, int(REVOKE_LEASE_RPCS_TIMEOUT / RPC_TIMEOUT))
    },

    // handles a RevokeLease response
    func(reply *RPCReply) bool {
      cc.mutex.Lock()
      defer cc.mutex.Unlock()

      if reply.Success {
        response := reply.Data.(*RevokeLeaseResponse)
        if response.Success {
          cc.grantedLeasesUntil[reply.Node] = time.Now()
        }
      }

      return false
    }, REVOKE_LEASE_RPCS_TIMEOUT)

  return rpcCh
}


func (cc *CrowdControl) RevokeLease(args *RevokeLeaseArgs,
    response *RevokeLeaseResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if args.View != cc.view {
    response.Success = false
    return nil
  }

  // invalidate lease
  cc.leaseUntil = time.Now()
  response.Success = true
  return nil
}
