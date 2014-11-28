package cc

import (
  "time"
  "sync"
)

type Client struct {
  mutex sync.Mutex
  servers []string
  primary int
}

const (
  CLIENT_RPC_RETRIES = 3
  SET_RPCS_TIMEOUT = 50 * time.Millisecond
)


func (client *Client) Get(key string) (string, bool) {
  client.mutex.Lock()
  defer client.mutex.Unlock()

  // TODO: try different servers
  args := &GetArgs{Key: key}
  response := &GetResponse{}
  waitTime := 10 * time.Millisecond

  lastServer := len(client.servers) - 1

  for {
    ch := makeRPCRetry(client.servers[lastServer], lastServer, "CrowdControl.Get", args, response,
      CLIENT_RPC_RETRIES)
    reply := <-ch

    if reply.Success {
      getResponse := reply.Data.(*GetResponse)

      if getResponse.Status == GET_SUCCESS {
        return getResponse.Value, getResponse.Exists
      }
    }

    // throttle requests
    time.Sleep(waitTime)
    if waitTime < 10 * time.Second {
      waitTime *= 2
    }
  }
}


func (client *Client) Set(key string, value string) {
  client.mutex.Lock()
  defer client.mutex.Unlock()

  args := &SetArgs{Key: key, Value: value}
  success := false
  waitTime := 10 * time.Millisecond

  for !success {
    if client.primary == -1 {
      ch := makeParallelRPCs(client.servers,
        // sends a Set RPC to the given peer
        func(node int) chan *RPCReply {
          response := &SetResponse{}
          return makeRPCRetry(client.servers[node], node, "CrowdControl.Set", args,
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
      response := &SetResponse{}
      ch := makeRPCRetry(client.servers[client.primary], 0, "CrowdControl.Set", args,
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

      if waitTime < 10 * time.Second {
        waitTime *= 2
      }
    }
  }
}

func (client *Client) Init(servers []string) {
  client.servers = servers
  client.primary = -1
}
