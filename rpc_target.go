package cc

import (
  "log"
  "sync"
  "time"
  "net/rpc"
)

/* An RPC response can return failure or data. */
type RPCReply struct {
  Node int
  Success bool
  Data interface{}
}


/* A target machine to send RPCs to. Maintains a persistent connection. */
type RPCTarget struct {
  mutex sync.Mutex
  client *rpc.Client  // saved connection
  dead bool  // used by the testing framework to disable RPCs
  sender string
  receiver string
  receiverNode int
}


/* Creates a target to make RPCs to. */
func (rt *RPCTarget) Init(sender string, receiver string, receiverNode int) {
  rt.client = nil
  rt.dead = false
  rt.sender = sender
  rt.receiver = receiver
  rt.receiverNode = receiverNode
}


/* Marks this target as dead, preventing further RPCs from going through. Used
 * by the testing framework. */
func (rt *RPCTarget) die() {
  rt.mutex.Lock()
  defer rt.mutex.Unlock()
  rt.dead = true
}


/* Marks this target as alive, allowing further RPCs to go through. Used by the
 * testing framework. */
func (rt *RPCTarget) live() {
  rt.mutex.Lock()
  defer rt.mutex.Unlock()
  rt.dead = false
}


/* Calls the RPC named `name` with the provided `args`, putting the response in
 * the given `reply`. */
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
