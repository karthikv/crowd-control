package cc

// import "log"

type Client struct {
  servers []string
}

const (
  CLIENT_RPC_RETRIES = 3
)


func (client *Client) Get(key string) (string, bool) {
  // TODO: try different servers
  args := &GetArgs{Key: key}
  response := &GetResponse{}
  ch := makeRPCRetry(client.servers[0], 0, "CrowdControl.Get", args, response, CLIENT_RPC_RETRIES)

  reply := <-ch
  if reply.Success {
    getResponse := reply.Data.(*GetResponse)
    return getResponse.Value, getResponse.Exists
  }

  // assume key doesn't exist
  return "", false
}


func (client *Client) Set(key string, value string) bool {
  // TODO: send to primary
  args := &SetArgs{Key: key, Value: value}
  response := &SetResponse{}
  ch := makeRPCRetry(client.servers[0], 0, "CrowdControl.Set", args, response, CLIENT_RPC_RETRIES)

  reply := <-ch
  if reply.Success {
    return true
  }

  return false
}

func (client *Client) Init(servers []string) {
  client.servers = servers
}
