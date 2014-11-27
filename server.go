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
)

const (
  ELECTION_TIMEOUT_MIN = 150
  ELECTION_TIMEOUT_MAX = 300

  ELECTION_RPCS_TIMEOUT = 100 * time.Millisecond
  HEARTBEAT_TIMEOUT = 25 * time.Millisecond

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

  // whether the data for a given key on a given peer is invalid
  filter []map[string]bool

  // operation log
  ol *OperationLog

  // maintains key -> lock mappings; must lock a given key while setting
  setMutexes map[string]*SetMutex
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

  response.Value, response.Exists = cc.cache[args.Key]
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

func (cc *CrowdControl) Set(args *SetArgs, response *SetResponse) error {
  setMutex := cc.acquireSetMutex(args.Key)
  cc.mutex.Lock()
  
  if cc.primary != cc.me {
    // only the primary can handle set requests
    // TODO: should probably send back who the real primary is
    cc.mutex.Unlock()
    cc.releaseSetMutex(args.Key, setMutex)
    return nil
  }

  key, value := args.Key, args.Value
  curValue, exists := cc.cache[key]
  if exists && curValue == value {
    // already set this value (set likely in progress from earlier)
    cc.mutex.Unlock()
    cc.releaseSetMutex(args.Key, setMutex)
    return nil
  }

  cc.cache[key] = value
  for i, _ := range cc.peers {
    cc.filter[i][key] = true
  }

  op := &Operation{Add: true, Key: key}
  cc.ol.Append(op)

  // TODO: add waiting for leases to expire
  majority := false
  nonce := cc.ol.GetNextOpNum()
  for !majority {
    numPrepped := 0
    refused := false

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
    defer cc.mutex.Unlock()

    if cc.view > originalView || refused {
      // we've made a transition to a new view; don't proceed
      cc.releaseSetMutex(args.Key, setMutex)
      return errors.New("set aborted due to view change")
    }

    for _, reply := range replies {
      if reply.Success && reply.Data.(*PrepResponse).Status == PREP_SUCCESS {
        cc.ol.FastForward(reply.Node)
      }
    }

    majority = (numPrepped <= cc.numPeers / 2)
    if majority {
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
              cc.filter[reply.Node][key] = false
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
    defer cc.mutex.Unlock()

    if originalView < cc.view {
      cc.releaseSetMutex(args.Key, setMutex)
      return
    }

    op := &Operation{Add: false, Key: key, Nodes: committedNodes}
    cc.ol.Append(op)
    cc.releaseSetMutex(args.Key, setMutex)
  }()

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
  cc.filter = make([]map[string]bool, cc.numPeers)

  for i := 0; i < cc.numPeers; i++ {
    cc.filter[i] = make(map[string]bool)
  }

  cc.ol = &OperationLog{}
  cc.ol.Init(OP_LOG_CAPACITY, cc.numPeers)

  cc.setMutexes = make(map[string]*SetMutex)

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
