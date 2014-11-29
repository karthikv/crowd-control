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
  socket string  // string representing socket address

  servers []string
  numServers int
  nodes []int

  weights []int
  primary int
}


func (client *Client) Get(key string) (string, bool) {
  client.mutex.Lock()
  defer client.mutex.Unlock()

  // TODO: try different servers
  args := &GetArgs{Key: key}
  waitTime := WAIT_TIME_INITIAL

  for i := 0; i < CLIENT_RPC_RETRIES; i++ {
    servers := client.pickServers()
    outCh := make(chan *GetResponse, len(servers))

    for i, _ := range servers {
      go func(i int) {
        ch := makeRPC(client.socket, client.servers[i], i, "CrowdControl.Get",
          args, &GetResponse{})
        reply := <-ch

        if reply.Success {
          getResponse := reply.Data.(*GetResponse)
          if getResponse.Status == GET_SUCCESS {
            outCh <- getResponse
          }
        }

        client.weightMutex.Lock()
        defer client.weightMutex.Unlock()

        if reply.Success {
          getResponse := reply.Data.(*GetResponse)
          if getResponse.Status == GET_SUCCESS {
            client.weights[i] *= WEIGHT_MULTIPLICATIVE_FACTOR
          } else {
            client.weights[i] += WEIGHT_ADDITIVE_DECREASE
          }
        } else {
          client.weights[i] /= WEIGHT_MULTIPLICATIVE_FACTOR
        }

        if client.weights[i] < WEIGHT_MIN {
          client.weights[i] = WEIGHT_MIN
        }

        if client.weights[i] > WEIGHT_MAX {
          client.weights[i] = WEIGHT_MAX
        }
      }(i)
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


func (client *Client) pickServers() []string {
  // pick a majority of servers to contact
  numChosen := client.numServers / 2 + 1
  servers := make([]string, numChosen, numChosen)

  client.weightMutex.Lock()
  defer client.weightMutex.Unlock()

  totalWeight := 0
  for _, weight := range client.weights {
    totalWeight += weight
  }

  for i := 0; i < numChosen; i++ {
    randWeight := rand.Int() % totalWeight
    cumWeight := 0

    for j, weight := range client.weights {
      cumWeight += weight
      if randWeight < cumWeight {
        servers[i] = client.servers[j]
        break
      }
    }
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
          return makeRPCRetry(client.socket, client.servers[node], node,
            "CrowdControl.Set", args, response, CLIENT_RPC_RETRIES)
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
      response := &SetResponse{}
      ch := makeRPCRetry(client.socket, client.servers[client.primary], 0,
        "CrowdControl.Set", args, response, CLIENT_RPC_RETRIES)

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

func (client *Client) Init(socket string, servers []string) {
  client.socket = socket
  client.servers = servers
  client.numServers = len(servers)

  client.nodes = make([]int, client.numServers, client.numServers)
  for i := 0; i < client.numServers; i++ {
    client.nodes[i] = i
  }

  client.weights = make([]int, client.numServers, client.numServers)
  for i := 0; i < client.numServers; i++ {
    client.weights[i] = WEIGHT_MAX
  }

  client.primary = -1
}
