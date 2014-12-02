package cc

import (
  "log"
  "sync"
  "net"
  "net/rpc"
  "math/rand"
  "os"
  "syscall"
  "time"
  "errors"
  "crypto/sha256"
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

  // error messages
  GET_LEASE_ERROR_MESSAGE = "could not get lease"
)


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

  // used by the testing harness to test edge cases
  dead bool
  unreliable bool

  reject map[string]bool
  rejectMutex sync.Mutex

  // set of machines within the cluster
  peers []string  // node -> unix socket string
  numPeers int
  nodes []int  // node numbers
  me int  // index of this machine

  // views to disambiguate the primary, as in VR
  view int
  nextView int  // the next view number upon election timeout
  primary int
  latestOp int  // used for determining up-to-dateness

  // leader election
  votes map[int]int  // view -> which machine this peer votes for
  lastHeartbeat time.Time
  electionTimerCh chan bool

  // key-value pairs
  cache map[string]string

  // whether the data for a given key on a given node is invalid
  filters []map[string]bool

  // operation log
  ol *OperationLog

  // maintains key -> lock mappings; must lock a given key while setting
  setMutexes map[string]*SetMutex

  // if we've prepped for a given nonce; nonce -> status mapping
  prepped map[int]bool

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
      return makeRPCRetry(cc.peers[cc.me], cc.peers[node], node,
        "CrowdControl.Heartbeat", args, response,
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
  log.Printf("CC[%v] attempting election\n", cc.me)
  // try starting election if no initial view or if no recent heartbeat
  args := &RequestVoteArgs{
    NextView: cc.nextView,
    NextPrimary: cc.me,
    View: cc.view,
    LatestOp: cc.latestOp,
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
      return makeRPCRetry(cc.peers[cc.me], cc.peers[node], node,
        "CrowdControl.RequestVote", args, response,
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
  cc.latestOp = 0

  cc.lastHeartbeat = time.Now()

  // reset election timer (non-blocking)
  select {
  case cc.electionTimerCh <- true:
  default:
  }

  cc.ol = &OperationLog{}
  cc.ol.Init(OP_LOG_CAPACITY, cc.numPeers)
  cc.prepped = make(map[int]bool)

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
  if cc.view == args.View && cc.latestOp > args.LatestOp {
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
        FilterHash: cc.hashFilter(cc.filters[cc.me]),
        Now: time.Now(),
      }

      ch := makeRPCRetry(cc.peers[cc.me], cc.peers[cc.primary], cc.primary,
        "CrowdControl.RequestLease", &leaseArgs, &RequestLeaseResponse{},
        SERVER_RPC_RETRIES)
      reply := <-ch

      if reply.Success {
        leaseResponse := reply.Data.(*RequestLeaseResponse)

        // TODO: case where filter is too full; need to recover?
        if leaseResponse.Status == LEASE_GRANTED {
          cc.leaseUntil = leaseResponse.Until
        } else if leaseResponse.Status == LEASE_REFUSED {
          return errors.New("get aborted due to view change")
        } else if leaseResponse.Status == LEASE_UPDATE_FILTER {
          cc.filters[cc.me] = leaseResponse.Filter
          cc.leaseUntil = leaseResponse.Until
        }
      } else {
        return errors.New(GET_LEASE_ERROR_MESSAGE)
      }
    }
  }

  if cc.filters[cc.me][args.Key] {
    // not up-to-date for this key
    response.Status = GET_DELAYED

    if cc.me != cc.primary {
      // request the key value pair from the primary
      go cc.getKVPairFromPrimary(args.Key)
    }
  } else {
    response.Status = GET_SUCCESS
    response.Value, response.Exists = cc.cache[args.Key]
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

  peerMe := cc.peers[cc.me]
  primary := cc.primary
  peerPrimary := cc.peers[primary]

  cc.mutex.Unlock()

  if shouldRequestPair {
    ch := makeRPCRetry(peerMe, peerPrimary, primary, "CrowdControl.RequestKVPair",
      &args, &RequestKVPairResponse{}, SERVER_RPC_RETRIES)
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

    if cc.filters[cc.me][args.Key] {
      value, exists = "", false
      delete(cc.cache, args.Key)
      delete(cc.filters[cc.me], args.Key)

      // TODO: does this cause too many ops to be added? why not just recover?
      op := &Operation{
        Add: false,
        Key: args.Key,
        Nodes: []int{cc.me},
      }
      cc.ol.Append(op)
    } else {
      value, exists = cc.cache[args.Key]
    }

    sendArgs := &SendKVPairArgs{
      View: cc.view,
      Node: cc.me,
      Key: args.Key,
      Value: value,
      Exists: exists,
    }

    ch := makeRPCRetry(cc.peers[cc.me], cc.peers[args.Node], args.Node,
      "CrowdControl.SendKVPair", &sendArgs, &SendKVPairResponse{},
      SERVER_RPC_RETRIES)

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
        // delete from node's filter; append to operation log
        delete(cc.filters[args.Node], args.Key)

        op := &Operation{
          Add: false,
          Key: args.Key,
          Nodes: []int{args.Node},
        }
        cc.ol.Append(op)
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
    delete(cc.cache, args.Key)
  } else {
    cc.cache[args.Key] = args.Value
  }

  delete(cc.filters[cc.me], args.Key)
  delete(cc.inflightKeys, args.Key)

  response.Status = SEND_KV_PAIR_SUCCESS
  return nil
}

func (cc *CrowdControl) hashFilter(filter map[string]bool) [sha256.Size]byte {
  numBytes := 0
  for key, _ := range filter {
    numBytes += len(key)
  }

  // create array of bytes corresponding to keys
  bytes := make([]byte, 0, numBytes)
  for key, _ := range filter {
    bytes = append(bytes, []byte(key)...)
  }

  return sha256.Sum256(bytes)
}


func (cc *CrowdControl) nodeHasFilterHash(node int, filterHash [sha256.Size]byte) bool {
  hash := cc.hashFilter(cc.filters[node])

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
    log.Printf("haven't reached majority\n")
    // haven't reached a majority of nodes for a while; might be partitioned
    response.Status = LEASE_REFUSED
    return nil
  }

  if cc.nodeHasFilterHash(args.Node, args.FilterHash) {
    response.Status = LEASE_GRANTED
  } else {
    response.Status = LEASE_UPDATE_FILTER
    response.Filter = cc.filters[args.Node]
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

  curValue, exists := cc.cache[key]
  if exists && curValue == value {
    // already set this value (set likely in progress from earlier)
    response.Status = SET_SUCCESS
    cc.mutex.Unlock()
    cc.releaseSetMutex(key, setMutex)
    return nil
  }

  cc.cache[key] = value
  for i, _ := range cc.peers {
    cc.filters[i][key] = true
  }

  // don't worry about leases changed during this set() operation; they don't
  // compromise safety, since they will happen after the Prep() RPC
  savedLeasesUntil := append([]time.Time(nil), cc.grantedLeasesUntil...)

  op := &Operation{Add: true, Key: key}
  cc.ol.Append(op)

  // TODO: add waiting for leases to expire
  majority := false
  nonce := cc.ol.GetNextOpNum()
  waitTime := WAIT_TIME_INITIAL

  for !majority {
    numPrepped := 0
    refused := false

    // TODO: either throttle RPC retries or just error, expecting client retry
    rpcCh := makeParallelRPCs(cc.nodes,
      // sends a Prep RPC to the given peer
      func(node int) chan *RPCReply {
        invalid, ops := cc.ol.GetPending(node)
        args := &PrepArgs{
          View: cc.view,
          Invalid: invalid,
          Nonce: nonce,
          Key: key,
          Ops: ops,
        }

        return makeRPCRetry(cc.peers[cc.me], cc.peers[node], node,
          "CrowdControl.Prep", args, &PrepResponse{},
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

    // wait for replies; don't block get requests
    replies := <-rpcCh

    cc.mutex.Lock()

    if cc.view > originalView || refused {
      // TODO: handle refused more explicitly?
      // we've made a transition to a new view; don't proceed
      cc.mutex.Unlock()
      cc.releaseSetMutex(key, setMutex)
      return errors.New("set aborted due to view change")
    }

    majority = (numPrepped > cc.numPeers / 2)
    if majority {
      for _, reply := range replies {
        if reply.Success && reply.Data.(*PrepResponse).Status == PREP_SUCCESS {
          cc.ol.FastForward(reply.Node)
        }
      }

      // successfully prepped
      log.Printf("CC[%v] set by %v/%v peers\n", cc.me, numPrepped, cc.numPeers)
    } else {
      // throttle requests
      time.Sleep(waitTime)
      if waitTime < WAIT_TIME_MAX {
        waitTime *= WAIT_TIME_MULTIPLICATIVE_INCREASE
      }
    }
  }

  commitDoneCh := make(chan bool, 1)
  go func() {
    cc.mutex.Lock()

    originalView := cc.view
    committedNodes := make([]int, 0, cc.numPeers)

    rpcCh := makeParallelRPCs(cc.nodes,
      // sends a Commit RPC to the given peer
      func(node int) chan *RPCReply {
        args := &CommitArgs{
          View: cc.view,
          Key: key,
          Value: value,
          Nonce: nonce,
        }
        response := &CommitResponse{}
        return makeRPCRetry(cc.peers[cc.me], cc.peers[node], node,
          "CrowdControl.Commit", args, response,
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
            delete(cc.filters[reply.Node], key)
            committedNodes = append(committedNodes, reply.Node)
          }
        }
        return false
      }, COMMIT_RPCS_TIMEOUT)

    cc.mutex.Unlock()

    <-rpcCh

    cc.mutex.Lock()

    if cc.view > originalView {
      cc.mutex.Unlock()
      commitDoneCh <- true
      return
    }

    op := &Operation{Add: false, Key: key, Nodes: committedNodes}
    cc.ol.Append(op)

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
    return errors.New("set aborted due to view change")
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

  if cc.prepped[args.Nonce] {
    response.Status = PREP_SUCCESS
    return nil
  }

  numOps := len(args.Ops)
  for i := 0; i < numOps; i++ {
    cc.performOp(&args.Ops[i])
  }

  // key will take time to commit; don't allow an incoming get request to
  // trigger a RequestKVPair() RPC
  cc.inflightKeys[args.Key] = time.Now()

  // nonce is the op number
  cc.latestOp = args.Nonce

  cc.prepped[args.Nonce] = true
  response.Status = PREP_SUCCESS
  return nil
}


func (cc *CrowdControl) performOp(op *Operation) {
  if op.Add {
    // add operations apply to all nodes
    for node, _ := range cc.peers {
      cc.filters[node][op.Key] = true
    }
  } else {
    // remove operations apply to nodes in op.Nodes
    for node, _ := range op.Nodes {
      delete(cc.filters[node], op.Key)
    }
  }
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

  if !cc.prepped[args.Nonce] {
    // can't commit if we didn't prep; need to get up-to-date
    response.Success = false
    return nil
  }

  // KV pair has been received 
  delete(cc.inflightKeys, args.Key)

  cc.cache[args.Key] = args.Value
  delete(cc.filters[cc.me], args.Key)
  // TODO: Need to garbage collect prepped somehow. We can't just delete
  // cc.prepped[args.Nonce] here because the response could get lost.
  // Perhaps if we associate nonces with the log, we can delete a nonce
  // as soon as we see a remove operation for it. More simply, we could
  // just keep hold of the latest N preps, and delete after that
  // (sufficiently low probability that prep will come up again)

  response.Success = true
  return nil
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
      return makeRPCRetry(cc.peers[cc.me], cc.peers[node], node,
        "CrowdControl.RevokeLease", args, &RevokeLeaseResponse{},
        int(REVOKE_LEASE_RPCS_TIMEOUT / RPC_TIMEOUT))
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


func (cc *CrowdControl) rejectConnFrom(node int) {
  cc.rejectMutex.Lock()
  defer cc.rejectMutex.Unlock()

  peer := cc.peers[node]
  cc.reject[peer] = true
}


func (cc *CrowdControl) rejectConnFromAll() {
  cc.rejectMutex.Lock()
  defer cc.rejectMutex.Unlock()

  for i, peer := range cc.peers {
    // don't reject from self
    if i != cc.me {
      cc.reject[peer] = true
    }
  }
}


func (cc *CrowdControl) acceptConnFrom(node int) {
  cc.rejectMutex.Lock()
  defer cc.rejectMutex.Unlock()

  peer := cc.peers[node]
  delete(cc.reject, peer)
}


func (cc *CrowdControl) acceptConnFromAll() {
  cc.rejectMutex.Lock()
  defer cc.rejectMutex.Unlock()

  for _, peer := range cc.peers {
    delete(cc.reject, peer)
  }
}


/* Creates a CrowdControl peer. `peers` is an array of peers within the
 * cluster, where each element is a string representing a socket. `me` is the
 * index of this peer. */
func (cc *CrowdControl) Init(peers []string, me int) {
  cc.dead = false
  cc.unreliable = false

  cc.reject = make(map[string]bool)

  cc.peers = peers
  cc.numPeers = len(peers)

  cc.nodes = make([]int, cc.numPeers, cc.numPeers)
  for i := 0; i < cc.numPeers; i++ {
    cc.nodes[i] = i
  }
  cc.me = me

  cc.view = -1
  cc.nextView = 0
  cc.primary = -1
  cc.latestOp = 0

  cc.votes = make(map[int]int)
  cc.lastHeartbeat = time.Now()
  cc.electionTimerCh = make(chan bool, 1)

  cc.scheduleElection()
  cc.scheduleHeartbeat()

  cc.cache = make(map[string]string)
  cc.filters = make([]map[string]bool, cc.numPeers, cc.numPeers)

  for i := 0; i < cc.numPeers; i++ {
    cc.filters[i] = make(map[string]bool)
  }

  cc.ol = &OperationLog{}
  cc.ol.Init(OP_LOG_CAPACITY, cc.numPeers)

  cc.setMutexes = make(map[string]*SetMutex)
  cc.prepped = make(map[int]bool)

  cc.reachedMajorityAt = time.Now()
  cc.leaseUntil = time.Now()

  cc.grantedLeasesUntil = make([]time.Time, cc.numPeers, cc.numPeers)
  for i := 0; i < cc.numPeers; i++ {
    cc.grantedLeasesUntil[i] = time.Now()
  }

  cc.inflightKeys = make(map[string]time.Time)

  rpcServer := rpc.NewServer()
  rpcServer.Register(cc)

  // remove any potentially stale socket (only when using unix sockets)
  os.Remove(peers[me])
  listener, err := net.Listen("unix", peers[me])

  if err != nil {
    log.Fatalf("CC[%v] Listen() failed: %v\n", me, err)
  }

  cc.listener = listener

  go func() {
    for !cc.dead {
      conn, err := cc.listener.Accept()

      // Accept() could take a long time; check for dead again
      if cc.dead && err == nil {
        conn.Close()
      }

      if !cc.dead && err == nil {
        // remove -sender suffix
        sender := conn.RemoteAddr().String()
        sender = sender[:len(sender) - 7]

        cc.rejectMutex.Lock()
        reject := cc.reject[sender]
        cc.rejectMutex.Unlock()

        if reject {
          conn.Close()
        } else if cc.unreliable && (rand.Int63() % 1000) < 100 {
          // 10% chance to drop request
          conn.Close()
        } else if cc.unreliable && (rand.Int63() % 1000) < 200 {
          // 20% chance to drop response if request wasn't dropped
          unixConn := conn.(*net.UnixConn)

          handle, err := unixConn.File()
          if err != nil {
            log.Printf("CC[%v] File() failed: %v\n", me, err)
          } else {
            // stop transmissions (not receptions) on unix socket
            err := syscall.Shutdown(int(handle.Fd()), syscall.SHUT_WR)
            if err != nil {
              log.Printf("CC[%v] Shutdown() failed: %v\n", me, err)
            }
          }

          go rpcServer.ServeConn(conn)
       } else {
          // successful request/response
          go rpcServer.ServeConn(conn)
        }
      }

      if err != nil {
        log.Printf("CC[%v] Accept() failed: %v\n", me, err)
      }
    }
  }()
}
