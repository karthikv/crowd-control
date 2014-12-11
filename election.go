package cc

import (
  "time"
  "log"

  "./op_log"
)


func (cc *CrowdControl) scheduleHeartbeat() {
  go func() {
    for {
      select {
      // send a heartbeat after every HEARTBEAT_TIMEOUT
      case <-time.After(HEARTBEAT_TIMEOUT):
        cc.mutex.Lock()
        if cc.dead {
          cc.mutex.Unlock()
          return
        }

        if cc.primary == cc.me {
          cc.sendHeartbeat_ml()
        }
        cc.mutex.Unlock()
      }
    }
  }()
}


func (cc *CrowdControl) sendHeartbeat_ml() {
  // log.Printf("CC[%v] sending heartbeats\n", cc.me)
  args := &HeartbeatArgs{Primary: cc.primary, View: cc.view}
  numReached := 0

  rpcCh := makeParallelRPCs(cc.nodes,
    // sends a Heartbeat RPC to the given peer
    func(node int) chan *RPCReply {
      response := &HeartbeatResponse{}
      return cc.rts[node].MakeRPCRetry("CrowdControl.Heartbeat", args, response,
        int(HEARTBEAT_RPCS_TIMEOUT / RPC_TIMEOUT))
    },

    // handles a Heartbeat response
    func(reply *RPCReply) bool {
      cc.mutex.Lock()
      defer cc.mutex.Unlock()

      if reply.Success {
        heartbeatResponse := reply.Data.(*HeartbeatResponse)

        if heartbeatResponse.Success {
          numReached += 1
        }
      }

      return false
    }, HEARTBEAT_RPCS_TIMEOUT)

  originalView := cc.view
  cc.mutex.Unlock()

  <-rpcCh

  cc.mutex.Lock()
  // note: don't unlock; that's the caller's responsibility

  // view change occurred while waiting; ignore responses
  if cc.view != originalView {
    return
  }

  if numReached > cc.numPeers / 2 {
    cc.reachedMajorityAt = time.Now()
  }

  if time.Now().Sub(cc.reachedMajorityAt) > ELECTION_TIMEOUT_MIN {
    savedLeasesUntil := append([]time.Time(nil), cc.grantedLeasesUntil...)
    view := cc.view

    // haven't reached a majority of nodes in a while; might be partitioned, so
    // revoke leases
    go func() {
      leasedNodes, _ := cc.getLeasedNodes(savedLeasesUntil)
      <-cc.revokeLeases_mu(leasedNodes, view)
    }()
  }
}


func (cc *CrowdControl) Heartbeat(args *HeartbeatArgs, response *HeartbeatResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.view > args.View {
    response.Success = false
    return nil
  }

  if cc.view < args.View {
    cc.setView_ml(args.View, args.Primary)
  }

  if cc.primary != args.Primary {
    log.Fatalf("CC[%v] fatal primaries don't match %v and %v\n", cc.me,
      cc.primary, args.Primary)
  }

  cc.lastHeartbeat = time.Now()

  // ensure electionTimerCh send is non-blocking
  select {
  case cc.electionTimerCh <- true:
  default:
  }

  // handle recovery in the background
  if cc.recovering && time.Now().Sub(cc.lastRecoveryRequest) >
      RECOVERY_REQUEST_TIMEOUT {
    go func() {
      cc.mutex.Lock()

      if cc.recovering && time.Now().Sub(cc.lastRecoveryRequest) >
          RECOVERY_REQUEST_TIMEOUT {
        log.Printf("Making recovery request\n")
        args := &RequestRecoveryArgs{
          View: cc.view,
          Node: cc.me,
        }

        ch := cc.rts[cc.primary].MakeRPCRetry("CrowdControl.RequestRecovery", &args,
          &RequestRecoveryResponse{}, SERVER_RPC_RETRIES)

        originalView := cc.view
        cc.mutex.Unlock()

        reply := <-ch

        cc.mutex.Lock()
        if cc.view != originalView {
          return
        }

        if reply.Success {
          recoveryResponse := reply.Data.(*RequestRecoveryResponse)
          if recoveryResponse.Success {
            cc.lastRecoveryRequest = time.Now()
          }
        }
      }

      cc.mutex.Unlock()
    }()
  }

  response.Success = true
  return nil
}


func (cc *CrowdControl) scheduleElection() {
  go func() {
    for {
      // attempt an election after a random timeout
      timeout := (cc.rand.Intn(ELECTION_TIMEOUT_MAX_INT - ELECTION_TIMEOUT_MIN_INT) +
        ELECTION_TIMEOUT_MIN_INT)

      select {
      case <-time.After(time.Duration(timeout) * time.Millisecond):
        cc.mutex.Lock()
        if cc.dead {
          cc.mutex.Unlock()
          return
        }

        if cc.primary != cc.me && !cc.recovering {
          cc.attemptElection_ml()
        }
        cc.mutex.Unlock()

      // override when a heartbeat arrives
      case <-cc.electionTimerCh:
      }
    }
  }()
}


func (cc *CrowdControl) attemptElection_ml() {
  log.Printf("CC[%v] attempting election for view %v\n", cc.me, cc.nextView)
  // try starting election if no initial view or if no recent heartbeat
  args := &RequestVoteArgs{
    NextView: cc.nextView,
    NextPrimary: cc.me,
    View: cc.view,
    NextOpNum: cc.nextOpNum,
  }

  numGranted := 0
  numAlreadyGranted := 0
  numRefused := 0

  // clear get lease, as we're determining the new primary
  cc.leaseUntil = time.Now()

  rpcCh := makeParallelRPCs(cc.nodes,
    // sends a RequestVote RPC to the given peer
    func(node int) chan *RPCReply {
      response := &RequestVoteResponse{}
      return cc.rts[node].MakeRPCRetry("CrowdControl.RequestVote", args, response,
        int(ELECTION_RPCS_TIMEOUT / RPC_TIMEOUT))
    },

    // aggregates RequestVote replies; determines when to stop collecting them
    func(reply *RPCReply) bool {
      cc.mutex.Lock()
      defer cc.mutex.Unlock()

      if reply.Success {
        // compute number of granted and already granted votes
        voteResponse := reply.Data.(*RequestVoteResponse)

        if voteResponse.Status == VOTE_GRANTED {
          numGranted += 1
          if numGranted > cc.numPeers / 2 {
            return true
          }
        } else if voteResponse.Status == VOTE_ALREADY_GRANTED {
          numAlreadyGranted += 1
        } else if voteResponse.Status == VOTE_REFUSED {
          numRefused += 1
        }

        if numGranted + (cc.numPeers - numAlreadyGranted) <= cc.numPeers / 2 {
          // can't get majority anymore
          return true
        }
      }

      return false
    }, ELECTION_RPCS_TIMEOUT)


  originalView := cc.view
  go func() {
    // wait for replies
    <-rpcCh

    cc.mutex.Lock()
    defer cc.mutex.Unlock()

    if cc.view > originalView {
      // we've already made a transition to a new view; don't proceed
      return
    }

    if numGranted > cc.numPeers / 2 {
      log.Printf("CC[%v] elected by %v/%v peers\n", cc.me, numGranted, cc.numPeers)
      // have a majority of votes
      cc.setView_ml(cc.nextView, cc.me)

      // sync all filters to begin this view
      filters := make([]op_log.Filter, cc.numPeers)
      for i := 0; i < cc.numPeers; i++ {
        filters[i] = *cc.filters[i]
      }

      op := op_log.SetFilterOperation{Nodes: cc.nodes, Filters: filters}
      cc.appendOp(op)
    } else if numRefused == 0 && numGranted + numAlreadyGranted > cc.numPeers / 2 {
      // contention between multiple nodes to become leader; try next view number
      cc.nextView += 1
    }
  }()
}


func (cc *CrowdControl) setView_ml(view int, primary int) {
  cc.view = view
  cc.nextView = view + 1

  cc.primary = primary
  cc.nextOpNum = 0
  cc.lastHeartbeat = time.Now()

  // reset election timer (non-blocking)
  select {
  case cc.electionTimerCh <- true:
  default:
  }

  cc.ol = &op_log.OperationLog{}
  cc.ol.Init(OP_LOG_CAPACITY, cc.numPeers)

  cc.reachedMajorityAt = time.Now()
  cc.leaseUntil = time.Now()
  cc.grantedLeasesUntil = make([]time.Time, cc.numPeers, cc.numPeers)
}


func (cc *CrowdControl) RequestVote(args *RequestVoteArgs, response *RequestVoteResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.recovering {
    return ErrRecovering
  }

  if cc.view > args.View || cc.view >= args.NextView || time.Now().
      Sub(cc.lastHeartbeat) < ELECTION_TIMEOUT_MIN {
    log.Printf("CC[%v] vote refused for %v\n", cc.me, args.NextPrimary)
    response.Status = VOTE_REFUSED
    return nil
  }

  vote, exists := cc.votes[args.NextView]
  if cc.view == args.View && cc.nextOpNum > args.NextOpNum {
    // node not up-to-date
    log.Printf("CC[%v] vote refused for out-of-date node %v\n", cc.me, args.NextPrimary)
    response.Status = VOTE_REFUSED
  } else if exists {
    if vote == args.NextPrimary {
      // vote has been granted to the requesting node
      response.Status = VOTE_GRANTED
    } else {
      // vote granted to another node
      response.Status = VOTE_ALREADY_GRANTED
    }
  } else {
    log.Printf("CC[%v] vote granted for %v\n", cc.me, args.NextPrimary)
    cc.votes[args.NextView] = args.NextPrimary
    response.Status = VOTE_GRANTED
  }

  return nil
}
