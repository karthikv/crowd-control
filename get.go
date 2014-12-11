package cc

import (
  "time"
  "log"
  "crypto/sha256"

  "./op_log"
)


func (cc *CrowdControl) Get(args *GetArgs, response *GetResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.recovering {
    return ErrRecovering
  }

  if cc.leaseUntil.Sub(time.Now()) <= 0 {
    // must get lease from primary
    leaseArgs := &RequestLeaseArgs{
      View: cc.view,
      Node: cc.me,
      FilterHash: cc.filters[cc.me].Hash(),
      Now: time.Now(),
    }

    ch := cc.rts[cc.primary].MakeRPCRetry("CrowdControl.RequestLease", &leaseArgs,
      &RequestLeaseResponse{}, SERVER_RPC_RETRIES)

    originalView := cc.view
    cc.mutex.Unlock()

    reply := <-ch

    cc.mutex.Lock()
    if cc.view > originalView {
      return ErrViewChange
    }

    if reply.Success {
      leaseResponse := reply.Data.(*RequestLeaseResponse)

      if leaseResponse.Status == LEASE_GRANTED {
        cc.leaseUntil = leaseResponse.Until
      } else if leaseResponse.Status == LEASE_REFUSED {
        return ErrViewChange
      } else if leaseResponse.Status == LEASE_UPDATE_FILTER {
        cc.filters[cc.me] = &leaseResponse.Filter
        cc.leaseUntil = leaseResponse.Until
      }
    } else {
      return ErrCouldNotGetLease
    }
  }

  if cc.filters[cc.me].Size > FILTER_CAPACITY {
    // missing a lot of data; must do a hard recovery
    cc.recovering = true
    return ErrRecovering
  }

  if cc.filters[cc.me].Contains([]byte(args.Key)) {
    // not up-to-date for this key
    response.Status = GET_DELAYED

    if cc.me != cc.primary {
      // request the key value pair from the primary
      go cc.getKVPairFromPrimary(args.Key)
    }
  } else {
    response.Status = GET_SUCCESS
    response.Value, response.Exists = cc.cache.Get(args.Key)
  }

  return nil
}


func (cc *CrowdControl) getKVPairFromPrimary(key string) {
  cc.mutex.Lock()
  shouldRequestPair := false

  lastRequest, exists := cc.inflightKeys[key]
  if !exists || time.Now().Sub(lastRequest) >= REQUEST_KV_PAIR_TIMEOUT {
    cc.inflightKeys[key] = time.Now()
    shouldRequestPair = true
  }

  args := &RequestKVPairArgs{
    View: cc.view,
    Node: cc.me,
    Key: key,
  }

  primary := cc.primary
  cc.mutex.Unlock()

  if shouldRequestPair {
    ch := cc.rts[primary].MakeRPCRetry("CrowdControl.RequestKVPair", &args,
      &RequestKVPairResponse{}, SERVER_RPC_RETRIES)
    reply := <-ch

    cc.mutex.Lock()

    if reply.Success {
      response := reply.Data.(*RequestKVPairResponse)

      // primary refused to provide the value; it's in a different view
      if response.Status == REQUEST_KV_PAIR_REFUSED {
        delete(cc.inflightKeys, args.Key)
      }
    } else {
      delete(cc.inflightKeys, args.Key)
    }

    cc.mutex.Unlock()
  }
}

func (cc *CrowdControl) RequestKVPair(args *RequestKVPairArgs,
    response *RequestKVPairResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.view != args.View || cc.me != cc.primary {
    response.Status = REQUEST_KV_PAIR_REFUSED
    return nil
  }

  response.Status = REQUEST_KV_PAIR_SUCCESS
  go func() {
    cc.mutex.Lock()
    var value string
    var exists bool

    if cc.filters[cc.me].Contains([]byte(args.Key)) {
      value, exists = "", false
      cc.cache.Delete(args.Key)

      op := op_log.RemoveOperation{Key: args.Key, Nodes: []int{cc.me}}
      cc.appendOp(op)
      op.Perform(&cc.filters)
    } else {
      value, exists = cc.cache.Get(args.Key)
    }

    sendArgs := &SendKVPairArgs{
      View: cc.view,
      Node: cc.me,
      Key: args.Key,
      Value: value,
      Exists: exists,
    }

    ch := cc.rts[args.Node].MakeRPCRetry("CrowdControl.SendKVPair", &sendArgs,
      &SendKVPairResponse{}, SERVER_RPC_RETRIES)

    originalView := cc.view
    cc.mutex.Unlock()

    reply := <-ch

    cc.mutex.Lock()
    defer cc.mutex.Unlock()

    if cc.view != originalView {
      return
    }

    if reply.Success {
      sendResponse := reply.Data.(*SendKVPairResponse)

      if sendResponse.Status == SEND_KV_PAIR_SUCCESS {
        // append to operation log; delete from node's filter
        op := op_log.RemoveOperation{Key: args.Key, Nodes: []int{args.Node}}
        cc.appendOp(op)
        op.Perform(&cc.filters)
      }
    }
  }()

  return nil
}


func (cc *CrowdControl) SendKVPair(args *SendKVPairArgs,
    response *SendKVPairResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.recovering {
    return ErrRecovering
  }

  if cc.view != args.View || cc.primary != args.Node {
    response.Status = SEND_KV_PAIR_REFUSED
    return nil
  }

  if !args.Exists {
    cc.cache.Delete(args.Key)
  } else {
    cc.cache.Set(args.Key, args.Value)
  }

  cc.filters[cc.me].Remove([]byte(args.Key))
  delete(cc.inflightKeys, args.Key)

  response.Status = SEND_KV_PAIR_SUCCESS
  return nil
}


func (cc *CrowdControl) nodeHasFilterHash(node int, filterHash [sha256.Size]byte) bool {
  hash := cc.filters[node].Hash()

  if len(hash) != len(filterHash) {
    return false
  }

  for i, value := range hash {
    if value != filterHash[i] {
      return false
    }
  }

  return true
}


func (cc *CrowdControl) RequestLease(args *RequestLeaseArgs,
    response *RequestLeaseResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.primary != cc.me || args.View != cc.view {
    response.Status = LEASE_REFUSED
    return nil
  }

  if time.Now().Sub(cc.reachedMajorityAt) > ELECTION_TIMEOUT_MIN {
    // haven't reached a majority of nodes for a while; might be partitioned
    log.Printf("haven't reached majority\n")
    response.Status = LEASE_REFUSED
    return nil
  }

  if cc.nodeHasFilterHash(args.Node, args.FilterHash) {
    response.Status = LEASE_GRANTED
  } else {
    response.Status = LEASE_UPDATE_FILTER
    response.Filter = *cc.filters[args.Node]
  }

  response.Until = args.Now.Add(LEASE_DURATION)
  cc.grantedLeasesUntil[args.Node] = time.Now().Add(LEASE_DURATION)
  return nil
}
