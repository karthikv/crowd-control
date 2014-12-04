package cc

import (
  "log"
  "sync"
  "time"
  "net/rpc"
  "crypto/sha256"
)

const (
  // if true, use unix sockets; if false, use tcp
  USE_UNIX_SOCKETS = false

  // time to wait for RPCs
  RPC_TIMEOUT = 10 * time.Millisecond

  // time to wait before retrying parallel RPCs
  WAIT_TIME_INITIAL = 1 * time.Millisecond

  // factor to increase wait time by to not congest network
  WAIT_TIME_MULTIPLICATIVE_INCREASE = 2

  // max wait time
  WAIT_TIME_MAX = 5 * time.Second

  // get operation succeded
  GET_SUCCESS = iota

  // can't get because peer isn't up-to-date
  GET_DELAYED = iota

  // vote given to this node
  VOTE_GRANTED = iota

  // vote already given to another node
  VOTE_ALREADY_GRANTED = iota

  // vote refused, as a primary already exists or there's a newer view
  VOTE_REFUSED = iota

  // set operation succeeded
  SET_SUCCESS = iota

  // set operation failed because this node isn't the primary
  SET_REFUSED = iota

  // successfully prepared for operation
  PREP_SUCCESS = iota

  // can't prep because in a later view
  PREP_REFUSED = iota

  // can't prepare until up-to-date
  PREP_DELAYED = iota

  // lease given to this node
  LEASE_GRANTED = iota

  // lease given, but node first has to update its filter
  LEASE_UPDATE_FILTER = iota

  // lease refused, as there's a later view
  LEASE_REFUSED = iota

  // successfully requested kv-pair from primary
  REQUEST_KV_PAIR_SUCCESS = iota

  // request was refused because primary was in a different view
  REQUEST_KV_PAIR_REFUSED = iota

  // requester successfully updated the kv-pair in its local cache
  SEND_KV_PAIR_SUCCESS = iota

  // requester could not perform the update since it is in a different view
  SEND_KV_PAIR_REFUSED = iota
)


/* An RPC response can return failure or data. */
type RPCReply struct {
  Node int
  Success bool
  Data interface{}
}


/* The Get() RPC takes in a key and returns its associated value. */
type GetArgs struct {
  Key string
}

type GetResponse struct {
  Status byte
  Exists bool
  Value string
}


/* The Set() RPC takes in a key-value pair, recording the association. */
type SetArgs struct {
  Key string
  Value string
}

type SetResponse struct {
  Status byte
}


/* The RequestVote() RPC requests a vote from a peer for a new primary. */
type RequestVoteArgs struct {
  NextView int
  NextPrimary int
  View int
  LatestOp int
}

type RequestVoteResponse struct {
  Status byte
}


/* The Heartbeat() RPC lets the primary assert control over peers. */
type HeartbeatArgs struct {
  View int
  Primary int
}

type HeartbeatResponse struct {
  Success bool
}


/* The Prep() RPC prepares a key value pair for insertion. */
type PrepArgs struct {
  View int
  Invalid bool
  Nonce int
  Key string
  Ops []Operation
}

type PrepResponse struct {
  Status byte
}


/* The Commit() RPC inserts/updates a key value pair. */
type CommitArgs struct {
  View int
  Nonce int
  Key string
  Value string
}

type CommitResponse struct {
  Success bool
  // TODO: add evicted keys
}


/* The RequestLease() RPC requests a lease to handle get requests. */
type RequestLeaseArgs struct {
  View int
  Node int  // sending node
  FilterHash [sha256.Size]byte
  Now time.Time
}

type RequestLeaseResponse struct {
  Status byte
  Filter map[string]bool
  Until time.Time
}

/* The RequestKVPair() RPC requests a key-value pair from the primary */
type RequestKVPairArgs struct {
  View int
  Node int
  Key string
}

type RequestKVPairResponse struct {
  Status byte
}

/* The SendKVPair() RPC sends a key-value pair from the primary to the requesting node */
type SendKVPairArgs struct {
  View int
  Node int
  Exists bool
  Key string
  Value string
}

type SendKVPairResponse struct {
  Status byte
}

/* The RevokeLease() RPC revokes a lease to handle get requests. */
type RevokeLeaseArgs struct {
  View int
}

type RevokeLeaseResponse struct {
  Success bool
}


type RPCTarget struct {
  mutex sync.Mutex
  client *rpc.Client  // saved connection
  dead bool  // used by the testing framework to disable RPCs
  sender string
  receiver string
  receiverNode int
}


func (rt *RPCTarget) Init(sender string, receiver string, receiverNode int) {
  rt.client = nil
  rt.dead = false
  rt.sender = sender
  rt.receiver = receiver
  rt.receiverNode = receiverNode
}


func (rt *RPCTarget) die() {
  rt.mutex.Lock()
  defer rt.mutex.Unlock()
  rt.dead = true
}


func (rt *RPCTarget) live() {
  rt.mutex.Lock()
  defer rt.mutex.Unlock()
  rt.dead = false
}


func (rt *RPCTarget) call(name string, args interface{}, reply *RPCReply) {
  rt.mutex.Lock()

  if rt.dead {
    reply.Success = false
    rt.mutex.Unlock()
    return
  }

  client := rt.client
  if client == nil {
    var err error

    // attempt to connect to get a client
    if USE_UNIX_SOCKETS {
      client, err = rpc.Dial("unix", rt.receiver)
    } else {
      client, err = rpc.Dial("tcp", rt.receiver)
    }

    if err != nil {
      log.Printf("Dial() failed: %v\n", err)
      reply.Success = false
      rt.mutex.Unlock()
      return
    }
  }

  rt.client = client
  rt.mutex.Unlock()

  err := client.Call(name, args, reply.Data)
  if err != nil {
    log.Printf("Call() failed: %v\n", err)
    reply.Success = false

    // if connection closed, reset client to try connecting again next time
    if err == rpc.ErrShutdown {
      rt.mutex.Lock()
      if rt.client == client {
        rt.client = nil
      }
      rt.mutex.Unlock()
    }
    return
  }

  reply.Success = true
}


/* Makes an RPC to the given `peer`, calling the function specified by `name`.
 * Passes in the arguments `args`. Requires an allocated `response` that can
 * hold the reply data. Returns a channel that holds the reply. */
func (rt *RPCTarget) MakeRPC(name string, args interface{},
    response interface{}) chan *RPCReply {
  rpcCh := make(chan *RPCReply, 1)

  go func() {
    // make RPC call, putting result in channel
    reply := &RPCReply{Node: rt.receiverNode}
    reply.Data = response

    rt.call(name, args, reply)
    rpcCh <- reply
  }()

  replyCh := make(chan *RPCReply, 1)
  go func() {
    // wait until RPC result or timeout
    select {
    case reply := <-rpcCh:
      replyCh <- reply
    case <-time.After(RPC_TIMEOUT):
      replyCh <- &RPCReply{Success: false}
    }
  }()

  return replyCh
}


// TODO: should numRetries be a param?
/* Makes an RPC, as per `makeRPC`. Retries `numRetries` times. */
func (rt *RPCTarget) MakeRPCRetry(name string, args interface{},
    response interface{}, numRetries int) chan *RPCReply {
  replyCh := make(chan *RPCReply, 1)

  go func() {
    for i := 0; i < numRetries; i++ {
      ch := rt.MakeRPC(name, args, response)
      reply := <-ch

      // use result of the first successful RPC, or the last RPC if others fail
      if reply.Success || i == numRetries - 1 {
        replyCh <- reply
        break
      }
    }
  }()

  return replyCh
}


/* Takes in a peer, sends an RPC, returns a channel that gives the results. */
type sendRPCFn func(int) chan *RPCReply

/* Takes in a reply, returns whether to stop processing. */
type replyRPCFn func(*RPCReply) bool

/* Makes parallel RPCs to all `peers`. Calls `sendCb` for each peer to send an
 * RPC. Then, calls `replyCb` when replies come in. Returns a channel that
 * gives back an array of replies when:
 *
 * (a) all replies have been received [or]
 * (b) `replyCb` returns true, indicating that we should stop processing [or]
 * (c) `timeout` elapses
 */
 // TODO: make arrays pointers?
func makeParallelRPCs(nodes []int, sendCb sendRPCFn, replyCb replyRPCFn,
    timeout time.Duration) chan []*RPCReply {
  numNodes := len(nodes)
  doneCh := make(chan bool)
  replyCh := make(chan *RPCReply, numNodes)

  // send RPCs
  for _, node := range nodes {
    go func(node int) {
      ch := sendCb(node)
      select {
      case reply := <-ch:
        replyCh <- reply
      case <-doneCh:
        return
      }
    }(node)
  }

  // collect results, appending them to an array
  replies := make([]*RPCReply, 0, numNodes)
  processedCh := make(chan bool)

  go func() {
    for i := 0; i < numNodes; i += 1 {
      select {
      case reply := <-replyCh:
        stop := replyCb(reply)

        replies = append(replies, reply)
        if stop {
          break
        }
      case <-doneCh:
        break
      }
    }

    close(processedCh)
  }()

  repliesCh := make(chan []*RPCReply, 1)

  // add replies to channel once finished
  go func() {
    select {
    case <-processedCh:
    case <-time.After(timeout):
    }

    close(doneCh)
    repliesCh <- replies
  }()

  return repliesCh
}
