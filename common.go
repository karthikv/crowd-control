package cc

import (
  "log"
  "net/rpc"
  "time"
)

const (
  OK = iota

  // vote given to this node
  VOTE_GRANTED = iota

  // vote already given to another node
  VOTE_ALREADY_GRANTED = iota

  // vote refused, as a primary already exists or there's a newer view
  VOTE_REFUSED = iota
)


/* An RPC response can return failure or data. */
type RPCReply struct {
  Success bool
  Data interface{}
}


/* The Get() RPC takes in a key and returns its associated value. */
type GetArgs struct {
  Key string
}

type GetResponse struct {
  Exists bool
  Value string
}


/* The Set() RPC takes in a key-value pair, recording the association. */
type SetArgs struct {
  Key string
  Value string
}

type SetResponse struct {
}


/* The RequestVote() RPC requests a vote from a peer for a new primary. */
type RequestVoteArgs struct {
  View int
  Primary int
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
}


const RPC_TIMEOUT = 10 * time.Millisecond

/* Makes an RPC to the given `peer`, calling the function specified by `name`.
 * Passes in the arguments `args`. Requires an allocated `response` that can
 * hold the reply data. Returns a channel that holds the reply. */
func makeRPC(peer string, name string, args interface{}, response interface{}) chan *RPCReply {
  rpcCh := make(chan *RPCReply, 1)

  go func() {
    // make RPC call, putting result in channel
    reply := &RPCReply{}
    conn, err := rpc.Dial("unix", peer)

    if err != nil {
      log.Printf("Dial() failed: %v\n", err)
      reply.Success = false
      return
    }

    defer conn.Close()
    reply.Data = response
    err = conn.Call(name, args, reply.Data)

    if err != nil {
      log.Printf("Call() failed: %v\n", err)
      reply.Success = false
      return
    }

    reply.Success = true
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
func makeRPCRetry(peer string, name string, args interface{}, replyType interface{},
    numRetries int) chan *RPCReply {
  replyCh := make(chan *RPCReply, 1)

  go func() {
    for i := 0; i < numRetries; i++ {
      ch := makeRPC(peer, name, args, replyType)
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
type sendRPCFn func(string) chan *RPCReply

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
func makeParallelRPCs(peers []string, sendCb sendRPCFn, replyCb replyRPCFn,
    timeout time.Duration) chan []*RPCReply {
  numPeers := len(peers)
  doneCh := make(chan bool)
  replyCh := make(chan *RPCReply, numPeers)

  // send RPCs
  for i, _ := range peers {
    go func(peer string) {
      ch := sendCb(peer)
      select {
      case reply := <-ch:
        replyCh <- reply
      case <-doneCh:
        return
      }
    }(peers[i])
  }

  // collect results, appending them to an array
  replies := make([]*RPCReply, 0, numPeers)
  processedCh := make(chan bool)

  go func() {
    for i := 0; i < numPeers; i += 1 {
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
