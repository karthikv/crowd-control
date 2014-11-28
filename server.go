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
  ELECTION_TIMEOUT_MIN = 150
  ELECTION_TIMEOUT_MAX = 300
  ELECTION_RPCS_TIMEOUT = 100 * time.Millisecond

  HEARTBEAT_TIMEOUT = 25 * time.Millisecond
  LEASE_DURATION = 10 * time.Second

  SERVER_RPC_RETRIES = 3
  OP_LOG_CAPACITY = 256
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

  // set of machines within the cluster
  peers []string
  numPeers int
  me int  // index of this machine

  // views to disambiguate the primary, as in VR
  view int
  nextView int  // the next view number upon election timeout
  primary int

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

  // lease to respond to get requests
  leaseUntil time.Time

  // leases granted by primary to nodes; node -> lease expiry mapping
  leases []time.Time
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
  log.Printf("CC[%v] sending heartbeats\n", cc.me)
  args := &HeartbeatArgs{Primary: cc.primary, View: cc.view}

  makeParallelRPCs(cc.peers,
    // sends a Heartbeat RPC to the given peer
    func(node int) chan *RPCReply {
      response := &HeartbeatResponse{}
      return makeRPCRetry(cc.peers[node], node, "CrowdControl.Heartbeat", args,
        response, SERVER_RPC_RETRIES)
    },

    // handles a Heartbeat response
    func(reply *RPCReply) bool {
      return false
    }, HEARTBEAT_TIMEOUT)
}


func (cc *CrowdControl) Heartbeat(args *HeartbeatArgs, response *HeartbeatResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.view < args.View {
    cc.setView_ml(args.View, args.Primary)
  }

  if cc.view == args.View {
    if cc.primary != args.Primary {
      log.Fatalf("CC[%v] fatal primaries don't match %v and %v\n", cc.me,
        cc.primary, args.Primary)
    }

    log.Printf("CC[%v] primary asserted beat %v\n", cc.me, cc.primary)
    cc.lastHeartbeat = time.Now()
    cc.electionTimerCh <- true
  }

  return nil
}


func (cc *CrowdControl) scheduleElection() {
  go func() {
    for {
      // attempt an election after a random timeout
      timeout := (rand.Int63() % (ELECTION_TIMEOUT_MAX - ELECTION_TIMEOUT_MIN) +
        ELECTION_TIMEOUT_MIN)

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
  args := &RequestVoteArgs{Primary: cc.me, View: cc.nextView}

  numGranted := 0
  numAlreadyGranted := 0
  numRefused := 0

  rpcCh := makeParallelRPCs(cc.peers,
    // sends a RequestVote RPC to the given peer
    func(node int) chan *RPCReply {
      response := &RequestVoteResponse{}
      return makeRPCRetry(cc.peers[node], node, "CrowdControl.RequestVote",
        args, response, SERVER_RPC_RETRIES)
    },

    // aggregates RequestVote replies; determines when to stop collecting them
    func(reply *RPCReply) bool {
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
}


func (cc *CrowdControl) RequestVote(args *RequestVoteArgs, response *RequestVoteResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.view >= args.View || time.Now().Sub(cc.lastHeartbeat) < ELECTION_TIMEOUT_MIN *
      time.Millisecond {
    log.Printf("CC[%v] vote refused for %v\n", cc.me, args.Primary)
    // TODO: inform requester of view update
    response.Status = VOTE_REFUSED
    return nil
  }

  vote, ok := cc.votes[args.View]
  if ok && vote != args.Primary {
    response.Status = VOTE_ALREADY_GRANTED
  } else {
    log.Printf("CC[%v] vote granted for %v\n", cc.me, args.Primary)
    // TODO: extra condition if primary is up to date
    cc.votes[args.View] = args.Primary
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

      ch := makeRPCRetry(cc.peers[cc.primary], cc.primary, "CrowdControl.RequestLease",
        &leaseArgs, &RequestLeaseResponse{}, SERVER_RPC_RETRIES)
      reply := <-ch

      if reply.Success {
        leaseResponse := reply.Data.(*RequestLeaseResponse)

        // TODO: case where filter is too full; need to recover?
        if leaseResponse.Status == LEASE_GRANTED {
          log.Printf("got granted for req lease\n")
          cc.leaseUntil = leaseResponse.Until
        } else if leaseResponse.Status == LEASE_REFUSED {
          log.Printf("got refused for req lease\n")
          return errors.New("get aborted due to view change")
        } else if leaseResponse.Status == LEASE_UPDATE_FILTER {
          log.Printf("got update for req lease\n")
          cc.filters[cc.me] = leaseResponse.Filter
          cc.leaseUntil = leaseResponse.Until
        }
      } else {
        return errors.New("could not get lease")
      }
    }
  }

  if cc.filters[cc.me][args.Key] {
    // not up-to-date for this key
    response.Status = GET_DELAYED
    // TODO: ask other server for k/v pair
  } else {
    response.Status = GET_SUCCESS
    response.Value, response.Exists = cc.cache[args.Key]
  }

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

  if cc.nodeHasFilterHash(args.Node, args.FilterHash) {
    response.Status = LEASE_GRANTED
  } else {
    response.Status = LEASE_UPDATE_FILTER
    response.Filter = cc.filters[args.Node]
  }

  response.Until = args.Now.Add(LEASE_DURATION)
  cc.leases[args.Node] = time.Now().Add(LEASE_DURATION)
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

  op := &Operation{Add: true, Key: key}
  cc.ol.Append(op)

  // TODO: add waiting for leases to expire
  majority := false
  nonce := cc.ol.GetNextOpNum()
  for !majority {
    numPrepped := 0
    refused := false

    // TODO: either throttle RPC retries or just error, expecting client retry
    rpcCh := makeParallelRPCs(cc.peers,
      // sends a Prep RPC to the given peer
      func(node int) chan *RPCReply {
        invalid, ops := cc.ol.GetPending(node)
        args := &PrepArgs{
          View: cc.view,
          Invalid: invalid,
          Ops: ops,
          Nonce: nonce,
        }
        response := &PrepResponse{}
        return makeRPCRetry(cc.peers[node], node, "CrowdControl.Prep", args,
          response, SERVER_RPC_RETRIES)
      },

      // aggregates Prep replies; determines when to stop collecting them
      func(reply *RPCReply) bool {
        if reply.Success {
          // compute number of granted and already granted votes
          prepResponse := reply.Data.(*PrepResponse)

          if prepResponse.Status == PREP_SUCCESS {
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
      }, ELECTION_RPCS_TIMEOUT)


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
    }
  }

  go func() {
    cc.mutex.Lock()

    originalView := cc.view
    committedNodes := make([]int, 0, cc.numPeers)

    rpcCh := makeParallelRPCs(cc.peers,
      // sends a Commit RPC to the given peer
      func(node int) chan *RPCReply {
        args := &CommitArgs{
          View: cc.view,
          Key: key,
          Value: value,
          Nonce: nonce,
        }
        response := &CommitResponse{}
        return makeRPCRetry(cc.peers[node], node, "CrowdControl.Commit", args,
          response, SERVER_RPC_RETRIES)
      },

      // aggregates Commit replies; determines when to stop collecting them
      func(reply *RPCReply) bool {
        if reply.Success {
          // compute number of granted and already granted votes
          commitResponse := reply.Data.(*CommitResponse)
          if commitResponse.Success {
            // possible view change; reply handlers can happen without lock
            cc.mutex.Lock()
            if cc.view == originalView {
              delete(cc.filters[reply.Node], key)
            }
            cc.mutex.Unlock()
            committedNodes = append(committedNodes, reply.Node)
          }
        }
        return false
      }, ELECTION_RPCS_TIMEOUT)

    cc.mutex.Unlock()

    <-rpcCh

    cc.mutex.Lock()

    if cc.view > originalView {
      cc.mutex.Unlock()
      cc.releaseSetMutex(key, setMutex)
      return
    }

    op := &Operation{Add: false, Key: key, Nodes: committedNodes}
    cc.ol.Append(op)

    cc.mutex.Unlock()
    cc.releaseSetMutex(key, setMutex)
  }()

  response.Status = SET_SUCCESS
  cc.mutex.Unlock()
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


/* Creates a CrowdControl peer. `peers` is an array of peers within the
 * cluster, where each element is a string representing a socket. `me` is the
 * index of this peer. */
func (cc *CrowdControl) Init(peers []string, me int) {
  cc.dead = false
  cc.unreliable = false

  cc.peers = peers
  cc.numPeers = len(peers)
  cc.me = me

  cc.view = -1
  cc.nextView = 0
  cc.primary = -1

  cc.votes = make(map[int]int)
  cc.lastHeartbeat = time.Now()
  cc.electionTimerCh = make(chan bool)

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

  cc.leaseUntil = time.Now()
  cc.leases = make([]time.Time, cc.numPeers, cc.numPeers)

  for i := 0; i < cc.numPeers; i++ {
    cc.leases[i] = time.Now()
  }

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
        if cc.unreliable && (rand.Int63() % 1000) < 100 {
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
