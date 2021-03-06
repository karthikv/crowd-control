package cc

import (
  "time"
  "sync"
  "math/rand"
)

const (
  CLIENT_RPC_RETRIES = 3
  SET_RPCS_TIMEOUT = 30 * time.Millisecond

  WEIGHT_MAX = 100
  WEIGHT_MIN = 10
  WEIGHT_ADDITIVE_DECREASE = -10
  WEIGHT_MULTIPLICATIVE_FACTOR = 2
)


type Client struct {
  mutex sync.Mutex
  weightMutex sync.Mutex

  servers []string
  numServers int
  nodes []int
  rts []*RPCTarget

  weights []int
  primary int
}


func (client *Client) Get(key string) (string, bool) {
  client.mutex.Lock()
  defer client.mutex.Unlock()

  args := &GetArgs{Key: key}
  waitTime := WAIT_TIME_INITIAL

  for i := 0; i < CLIENT_RPC_RETRIES; i++ {
    servers := client.pickServers(1)
    outCh := make(chan *GetResponse, len(servers))

    // make RPCs to servers in parallel
    for _, node := range servers {
      go func(node int) {
        ch := client.rts[node].MakeRPC("CrowdControl.Get", args, &GetResponse{})
        reply := <-ch

        if reply.Success {
          getResponse := reply.Data.(*GetResponse)
          if getResponse.Status == GET_SUCCESS {
            outCh <- getResponse
          }
        }

        client.weightMutex.Lock()
        defer client.weightMutex.Unlock()

        // update weights based on response
        if reply.Success {
          getResponse := reply.Data.(*GetResponse)
          if getResponse.Status == GET_SUCCESS {
            client.weights[node] *= WEIGHT_MULTIPLICATIVE_FACTOR
          } else {
            client.weights[node] += WEIGHT_ADDITIVE_DECREASE
          }
        } else {
          client.weights[node] /= WEIGHT_MULTIPLICATIVE_FACTOR
        }

        if client.weights[node] < WEIGHT_MIN {
          client.weights[node] = WEIGHT_MIN
        }

        if client.weights[node] > WEIGHT_MAX {
          client.weights[node] = WEIGHT_MAX
        }
      }(node)
    }

    select {
    case response := <-outCh:
      return response.Value, response.Exists
    case <-time.After(RPC_TIMEOUT):
    }

    // throttle requests
    time.Sleep(waitTime)
    if waitTime < WAIT_TIME_MAX {
      waitTime *= WAIT_TIME_MULTIPLICATIVE_INCREASE
    }
  }

  // couldn't get response
  return "", false
}


func (client *Client) pickServers(numServers int) []int {
  // pick a set of servers to contact
  servers := make([]int, numServers, numServers)

  client.weightMutex.Lock()
  defer client.weightMutex.Unlock()

  // duplicate weights so we can modify
  weights := append([]int(nil), client.weights...)

  totalWeight := 0
  for _, weight := range weights {
    totalWeight += weight
  }

  for i := 0; i < numServers; i++ {
    randWeight := rand.Int() % totalWeight
    cumWeight := 0

    for j, weight := range weights {
      cumWeight += weight
      if randWeight < cumWeight {
        servers[i] = j
        break
      }
    }

    removed := servers[i]
    totalWeight -= weights[removed]
    weights = append(weights[:removed], weights[removed + 1:]...)
  }

  return servers
}


func (client *Client) Set(key string, value string) {
  client.mutex.Lock()
  defer client.mutex.Unlock()

  args := &SetArgs{Key: key, Value: value}
  success := false
  waitTime := WAIT_TIME_INITIAL

  for !success {
    if client.primary == -1 {
      ch := makeParallelRPCs(client.nodes,
        // sends a Set RPC to the given peer
        func(node int) chan *RPCReply {
          response := &SetResponse{}
          return client.rts[node].MakeRPCRetry("CrowdControl.Set", args,
            response, CLIENT_RPC_RETRIES)
        },

        // finds a proper Set reply
        func(reply *RPCReply) bool {
          if reply.Success {
            setResponse := reply.Data.(*SetResponse)

            if setResponse.Status == SET_SUCCESS {
              client.primary = reply.Node
              success = true
              return true
            }
          }

          return false
        }, SET_RPCS_TIMEOUT)

      <-ch
    } else {
      // we know the primary; only send the request to it
      response := &SetResponse{}
      ch := client.rts[client.primary].MakeRPCRetry("CrowdControl.Set", args,
        response, CLIENT_RPC_RETRIES)

      reply := <-ch
      if reply.Success {
        setResponse := reply.Data.(*SetResponse)

        if setResponse.Status == SET_SUCCESS {
          success = true
        } else if setResponse.Status == SET_REFUSED {
          client.primary = -1
        }
      } else {
        client.primary = -1
      }
    }

    // throttle requests
    if !success {
      time.Sleep(waitTime)
      if waitTime < WAIT_TIME_MAX {
        waitTime *= WAIT_TIME_MULTIPLICATIVE_INCREASE
      }
    }
  }
}

func (client *Client) Init(sender string, servers []string) {
  client.servers = servers
  client.numServers = len(servers)

  client.nodes = make([]int, client.numServers, client.numServers)
  for i := 0; i < client.numServers; i++ {
    client.nodes[i] = i
  }

  client.rts = make([]*RPCTarget, client.numServers, client.numServers)
  for i := 0; i < client.numServers; i++ {
    rt := &RPCTarget{}
    rt.Init(sender, servers[i], i)
    client.rts[i] = rt
  }

  client.weights = make([]int, client.numServers, client.numServers)
  for i := 0; i < client.numServers; i++ {
    client.weights[i] = WEIGHT_MAX
  }

  client.primary = -1
}
