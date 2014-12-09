package cc

import (
  "log"
  "sync"
  "net"
  "net/rpc"
  "math/rand"
  "os"
  "time"
  "errors"
  "crypto/sha256"
  "encoding/gob"

  "./op_log"
  "./cache"
)

const (
  // min/max time until election
  ELECTION_TIMEOUT_MIN_INT = 150
  ELECTION_TIMEOUT_MIN = ELECTION_TIMEOUT_MIN_INT * time.Millisecond
  ELECTION_TIMEOUT_MAX_INT = 300
  ELECTION_TIMEOUT_MAX = ELECTION_TIMEOUT_MAX_INT * time.Millisecond

  // time to wait before starting election
  ELECTION_RPCS_TIMEOUT = 100 * time.Millisecond

  // time between heartbeats
  HEARTBEAT_TIMEOUT = 20 * time.Millisecond

  // time to wait for heartbeat responses
  HEARTBEAT_RPCS_TIMEOUT = HEARTBEAT_TIMEOUT

  // time to wait for prep responses
  PREP_RPCS_TIMEOUT = 50 * time.Millisecond

  // time to wait for commit responses
  COMMIT_RPCS_TIMEOUT = PREP_RPCS_TIMEOUT

  // Time to wait for revoke lease responses. If we can't revoke leases, we
  // likely can't heartbeat either. After ELECTION_TIMEOUT_MAX, other nodes
  // will start an election, clearing their leases.
  REVOKE_LEASE_RPCS_TIMEOUT = ELECTION_TIMEOUT_MAX

  // timeout for request operation
  REQUEST_KV_PAIR_TIMEOUT = COMMIT_RPCS_TIMEOUT

  // length of get lease
  LEASE_DURATION = 10 * time.Second

  // number of times to retry RPCs
  SERVER_RPC_RETRIES = 3

  // max length of operation log
  OP_LOG_CAPACITY = 256

  // counting bloom filter configuration
  FILTER_CAPACITY = 256
  FILTER_NUM_HASHES = 6
)


var ErrViewChange = errors.New("aborted due to view change")
var ErrCouldNotGetLease = errors.New("could not get lease")


// TODO: add random seed later
// TODO: handle eviction


/* Represents a mutex along with its number of users. */
type SetMutex struct {
  Mutex sync.Mutex
  NumUsers int
}


/* Represents a single CrowdControl peer that maintains consensus over a set of
 * key -> value pairs. */
type CrowdControl struct {
  mutex sync.Mutex
  listener net.Listener

  dead bool  // used by testing framework to determine if this node is dead

  // set of machines within the cluster
  peers []string  // node -> unix socket string
  numPeers int
  me int  // index of this machine

  nodes []int  // node numbers
  rts []*RPCTarget  // node -> RPCTarget that maintains persistent connection

  // views to disambiguate the primary, as in VR
  view int
  nextView int  // the next view number upon election timeout
  primary int
  nextOpNum int  // used for determining up-to-dateness

  // leader election
  votes map[int]int  // view -> which machine this peer votes for
  lastHeartbeat time.Time
  electionTimerCh chan bool

  // key-value pairs
  cache *cache.Cache

  // whether the data for a given key on a given node is invalid
  filters []*op_log.Filter

  // operation log
  ol *op_log.OperationLog

  // maintains key -> lock mappings; must lock a given key while setting
  setMutexes map[string]*SetMutex

  // last time the primary reached a majority of nodes
  reachedMajorityAt time.Time

  // lease to respond to get requests
  leaseUntil time.Time

  // leases granted by primary to nodes; node -> lease expiry mapping
  grantedLeasesUntil []time.Time

  // mapping from keys to the time when request for fetch for kv pair from primary was made
  inflightKeys map[string]time.Time
}


func (cc *CrowdControl) scheduleHeartbeat() {
  go func() {
    for {
      select {
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

  response.Success = true
  return nil
}


func (cc *CrowdControl) scheduleElection() {
  go func() {
    for {
      // attempt an election after a random timeout
      timeout := (rand.Int63() % (ELECTION_TIMEOUT_MAX_INT - ELECTION_TIMEOUT_MIN_INT) +
        ELECTION_TIMEOUT_MIN_INT)

      select {
      case <-time.After(time.Duration(timeout) * time.Millisecond):
        cc.mutex.Lock()
        if cc.dead {
          cc.mutex.Unlock()
          return
        }

        if cc.primary != cc.me {
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

  if cc.view > args.View || cc.view >= args.NextView || time.Now().
      Sub(cc.lastHeartbeat) < ELECTION_TIMEOUT_MIN {
    // log.Printf("CC[%v] vote refused for %v\n", cc.me, args.NextPrimary)
    // TODO: inform requester of view update
    response.Status = VOTE_REFUSED
    return nil
  }

  vote, exists := cc.votes[args.NextView]
  if cc.view == args.View && cc.nextOpNum > args.NextOpNum {
    log.Printf("CC[%v] vote refused for %v\n", cc.me, args.NextPrimary)
    // node not up-to-date
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

func (cc *CrowdControl) Get(args *GetArgs, response *GetResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.leaseUntil.Sub(time.Now()) <= 0 {
    if cc.primary == cc.me {
      // TODO: handle network partition case here
      // primary can extend its own lease
      cc.leaseUntil = time.Now().Add(LEASE_DURATION)
    } else {
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

        // TODO: case where filter is too full; need to recover?
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
  // TODO: garbage collect inflight keys
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
  // TODO: what are the other cases in which a refusal should happen?

  response.Status = REQUEST_KV_PAIR_SUCCESS
  go func() {
    cc.mutex.Lock()
    var value string
    var exists bool

    if cc.filters[cc.me].Contains([]byte(args.Key)) {
      value, exists = "", false
      cc.cache.Delete(args.Key)

      // TODO: does this cause too many ops to be added? why not just recover?
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

  if cc.view != args.View || cc.primary != args.Node {
    response.Status = SEND_KV_PAIR_REFUSED
    return nil
  }
  // TODO: what are the other failure cases here?

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


func (cc *CrowdControl) RevokeLease(args *RevokeLeaseArgs,
    response *RevokeLeaseResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if args.View != cc.view {
    response.Success = false
    return nil
  }

  // TODO: confirm primary
  // invalidate lease
  cc.leaseUntil = time.Now()
  response.Success = true
  return nil
}


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

  // TODO: add waiting for leases to expire
  majority := false
  waitTime := WAIT_TIME_INITIAL
  var nextOpNum int

  for !majority {
    numPrepped := 0
    refused := false
    nextOpNum = cc.ol.GetNextOpNum()

    // TODO: either throttle RPC retries or just error, expecting client retry
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
      // TODO: handle refused more explicitly?
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

  if cc.view < args.View || args.Invalid {
    // TODO: somehow update my view?
    response.Status = PREP_DELAYED
    return nil
  }

  startIndex := cc.nextOpNum - args.StartOpNum
  if startIndex < 0 {
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

  if cc.view > args.View {
    // TODO: inform of new view?
    response.Success = false
    return nil
  }

  if cc.view < args.View {
    // TODO: somehow update my view?
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


/* Creates a CrowdControl peer. `peers` is an array of peers within the
 * cluster, where each element is a string representing a socket. `me` is the
 * index of this peer. */
func (cc *CrowdControl) Init(peers []string, me int, capacity uint64) {
  cc.peers = peers
  cc.numPeers = len(peers)
  cc.me = me

  cc.nodes = make([]int, cc.numPeers, cc.numPeers)
  for i := 0; i < cc.numPeers; i++ {
    cc.nodes[i] = i
  }

  cc.rts = make([]*RPCTarget, cc.numPeers, cc.numPeers)
  for i := 0; i < cc.numPeers; i++ {
    rt := &RPCTarget{}
    rt.Init(peers[me], peers[i], i)
    cc.rts[i] = rt
  }

  cc.view = -1
  cc.nextView = 0
  cc.primary = -1
  cc.nextOpNum = 0

  cc.votes = make(map[int]int)
  cc.lastHeartbeat = time.Now()
  cc.electionTimerCh = make(chan bool, 1)

  cc.scheduleElection()
  cc.scheduleHeartbeat()

  cc.cache = &cache.Cache{}
  cc.cache.Init(capacity)
  cc.filters = make([]*op_log.Filter, cc.numPeers, cc.numPeers)

  for i := 0; i < cc.numPeers; i++ {
    cc.filters[i] = &op_log.Filter{}
    cc.filters[i].Init(FILTER_CAPACITY, FILTER_NUM_HASHES)
  }

  cc.ol = &op_log.OperationLog{}
  cc.ol.Init(OP_LOG_CAPACITY, cc.numPeers)

  cc.setMutexes = make(map[string]*SetMutex)
  cc.reachedMajorityAt = time.Now()
  cc.leaseUntil = time.Now()

  cc.grantedLeasesUntil = make([]time.Time, cc.numPeers, cc.numPeers)
  for i := 0; i < cc.numPeers; i++ {
    cc.grantedLeasesUntil[i] = time.Now()
  }

  cc.inflightKeys = make(map[string]time.Time)

  gob.Register(op_log.AddOperation{})
  gob.Register(op_log.RemoveOperation{})
  gob.Register(op_log.SetFilterOperation{})

  rpcServer := rpc.NewServer()
  rpcServer.Register(cc)

  var listener net.Listener
  var err error

  if USE_UNIX_SOCKETS {
    // remove any potentially stale socket
    os.Remove(peers[me])
    listener, err = net.Listen("unix", peers[me])
  } else {
    listener, err = net.Listen("tcp", peers[me])
  }

  if err != nil {
    log.Fatalf("CC[%v] Listen() failed: %v\n", me, err)
  }

  cc.listener = listener

  go func() {
    for {
      conn, err := cc.listener.Accept()

      if err != nil {
        log.Printf("CC[%v] Accept() failed: %v\n", me, err)
        continue
      }

      go rpcServer.ServeConn(conn)
    }
  }()
}
