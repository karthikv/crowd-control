package cc

import (
  "fmt"
  "log"
  "testing"
  "time"
  "sync"
  "math/rand"
)

type Cluster []*CrowdControl

func makeCluster(numPeers int, prefix string) ([]string, Cluster) {
  // cluster uses randomness for elections
  rand.Seed(time.Now().UnixNano())
  peers := make([]string, numPeers, numPeers)
  port := 10000 + rand.Intn(10000)

  for i := 0; i < numPeers; i++ {
    var socket string

    if USE_UNIX_SOCKETS {
      socket = fmt.Sprintf("/tmp/%v%v.sock", prefix, i)
    } else {
      socket = fmt.Sprintf(":%v", port)
      port += 1
    }

    peers[i] = socket
  }

  cluster := make(Cluster, numPeers)
  for i := 0; i < numPeers; i++ {
    cc := &CrowdControl{}
    cc.Init(peers, i)
    cluster[i] = cc
  }

  return peers, cluster
}

func (cluster Cluster) enable(from int, to int) {
  cluster[from].rts[to].live()
}

func (cluster Cluster) enableFrom(from int) {
  for _, rt := range cluster[from].rts {
    rt.live()
  }
}

func (cluster Cluster) enableTo(to int) {
  for _, cc := range cluster {
    cc.rts[to].live()
  }
}

func (cluster Cluster) enableAll() {
  for from, _ := range cluster {
    cluster.enableFrom(from)
  }
}

func (cluster Cluster) disable(from int, to int) {
  cluster[from].rts[to].die()
}

func (cluster Cluster) disableFrom(from int) {
  for _, rt := range cluster[from].rts {
    rt.die()
  }
}

func (cluster Cluster) disableTo(to int) {
  for _, cc := range cluster {
    cc.rts[to].die()
  }
}

func (cluster Cluster) disableAll() {
  for from, _ := range cluster {
    cluster.disableFrom(from)
  }
}

func (cluster Cluster) kill(node int) {
  cluster.disableFrom(node)
  cluster.disableTo(node)
  cluster[node].dead = true
}

func (cluster Cluster) killAll() {
  for node, _ := range cluster {
    cluster.kill(node)
  }
}

func checkView(t *testing.T, cc *CrowdControl, view int, primary int) {
  if cc.primary != primary {
    t.Fatalf("Disagreement on primary: %v and %v\n", primary, cc.primary)
  }

  if cc.view != view {
    t.Fatalf("Disagreement on view: %v and %v\n", view, cc.view)
  }
}

func TestLeaderElection(t *testing.T) {
  log.Printf("\n\nTestLeaderElection(): Begin\n\n")

  _, cluster := makeCluster(5, "le")
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)

  primary := cluster[0].primary
  view := cluster[0].view

  if primary == -1 {
    t.Fatalf("No primary elected\n")
  }

  for _, cc := range cluster {
    checkView(t, cc, view, primary)
  }

  log.Printf("Primary %v successfully elected\n", primary)
  time.Sleep(1 * time.Second)

  for i := 0; i < 2; i++ {
    log.Printf("Primary %v going down\n", primary)
    cluster.kill(primary)

    time.Sleep(3 * ELECTION_TIMEOUT_MAX)
    for _, cc := range cluster {
      if !cc.dead {
        primary = cc.primary
        view = cc.view
        break
      }
    }

    if primary == -1 {
      t.Fatalf("No primary elected\n")
    }

    for _, cc := range cluster {
      if cc.dead {
        continue
      }

      checkView(t, cc, view, primary)
    }

    log.Printf("Primary %v successfully elected\n", primary)
    time.Sleep(1 * time.Second)
  }

  log.Printf("Primary %v going down\n", primary)
  cluster.kill(primary)
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)

  for _, cc := range cluster {
    if cc.dead {
      continue
    }

    checkView(t, cc, view, primary)
  }

  cluster.killAll()
  log.Printf("\n\nTestLeaderElection(): End\n\n")
}

func TestPrimarySelection(t *testing.T) {
  log.Printf("\n\nTestPrimarySelection(): Begin\n\n")
  numPeers := 5
  peers, cluster := makeCluster(numPeers, "ps")

  var client Client
  client.Init("/tmp/ps-client.sock", peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)

  primary := cluster[0].primary
  view := cluster[0].view

  if primary == -1 {
    t.Fatalf("No primary elected\n")
  }

  for _, cc := range cluster {
    checkView(t, cc, view, primary)
  }

  client.Set("lorem", "ipsum")

  // partition node
  log.Printf("partitioning node to make it stale\n")
  staleNode := (primary + 1) % numPeers

  cluster.disableTo(staleNode)
  cluster.disableFrom(staleNode)

  // do set operation to make node stale
  client.Set("dolor sit", "amet")

  // kill primary and make nodes only respond to stale node
  log.Printf("trying to make stale node the new primary\n")
  cluster.kill(primary)

  cluster.disableAll()
  cluster.enableFrom(staleNode)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)

  // view/primary should be the same
  for _, cc := range cluster {
    if !cc.dead {
      checkView(t, cc, view, primary)
    }
  }

  // make nodes communicate with non-stale node
  log.Printf("trying to make good node the new primary\n")
  goodNode := (staleNode + 1) % numPeers

  cluster.disableAll()
  cluster.enableFrom(staleNode)
  cluster.enableFrom(goodNode)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)

  // view/primary should update
  primary = goodNode
  if cluster[goodNode].view < view {
    log.Fatalf("view did not update\n")
  }
  view = cluster[goodNode].view

  for _, cc := range cluster {
    if !cc.dead {
      checkView(t, cc, view, primary)
    }
  }

  // reset cluster
  cluster.enableAll()

  // make all nodes up-to-date
  client.Set("consectetur", "adipiscing")

  // ensure previously-stale node can now be elected
  log.Printf("trying to make previously-stale node the new primary\n")
  cluster.kill(primary)

  cluster.disableAll()
  cluster.enableFrom(staleNode)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)

  // view/primary should update
  primary = staleNode
  if cluster[staleNode].view < view {
    log.Fatalf("view did not update\n")
  }
  view = cluster[staleNode].view

  for _, cc := range cluster {
    if !cc.dead {
      checkView(t, cc, view, primary)
    }
  }

  cluster.killAll()
  log.Printf("\n\nTestPrimarySelection(): End\n\n")
}

func checkBasicGetSetOps(t *testing.T, client *Client) {
  client.Set("foo", "bar")
  client.Set("john", "doe")

  // wait for values to propagate
  time.Sleep(COMMIT_RPCS_TIMEOUT)

  value, exists := client.Get("foo")
  if !(exists && value == "bar") {
    t.Fatalf("Incorrect value: foo -> %v, %v\n", value, exists)
  }

  value, exists = client.Get("john")
  if !(exists && value == "doe") {
    t.Fatalf("Incorrect value: john -> %v, %v\n", value, exists)
  }

  value, exists = client.Get("other")
  if exists {
    t.Fatal("Key should not exist: other\n")
  }

  value, exists = client.Get("joh")
  if exists {
    t.Fatal("Key should not exist: joh\n")
  }

  // case sensitive
  value, exists = client.Get("Foo")
  if exists {
    t.Fatal("Key should not exist: Foo\n")
  }
}

// single client, single server
func TestGetSetSingle(t *testing.T) {
  log.Printf("\n\nTestGetSetSingle(): Begin\n\n")
  peers, cluster := makeCluster(1, "gss")

  var client Client
  client.Init("/tmp/gss0-client.sock", peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)
  checkBasicGetSetOps(t, &client)

  cluster.killAll()
  log.Printf("\n\nTestGetSetSingle(): End\n\n")
}

// multiple server, single client
func TestGetSetMultiple(t *testing.T) {
  log.Printf("\n\nTestGetSetMultiple(): Begin\n\n")
  peers, cluster := makeCluster(5, "gsm")

  var client Client
  client.Init("/tmp/gsm0-client.sock", peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)
  checkBasicGetSetOps(t, &client)

  cluster.killAll()
  log.Printf("\n\nTestGetSetMultiple(): End\n\n")
}

type getResponseChecker func(*testing.T, *GetResponse, error) bool

func checkGetOperation(t *testing.T, cluster Cluster, key string,
    checker getResponseChecker) {
  numPeers := len(cluster)
  failCh := make(chan bool, numPeers)

  var wg sync.WaitGroup
  wg.Add(numPeers)

  for i, _ := range cluster {
    go func(i int) {
      defer wg.Done()

      response := &GetResponse{}
      err := cluster[i].Get(&GetArgs{Key: key}, response)

      if !checker(t, response, err) {
        failCh <- true
      }
    }(i)
  }

  wg.Wait()
  select {
  case <-failCh:
    t.FailNow()
  default:
  }
}

func TestGetLeases(t *testing.T) {
  log.Printf("\n\nTestGetLease(): Begin\n\n")
  numPeers := 5
  peers, cluster := makeCluster(numPeers, "gl")

  var client Client
  client.Init("/tmp/gl-client.sock", peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)
  key, value := "lazy", "dog"

  // wait for set to propagate
  client.Set(key, value)
  time.Sleep(COMMIT_RPCS_TIMEOUT)

  // prevent primary from giving leases
  primary := cluster[0].primary
  cluster.disableTo(primary)

  // get slice without primary
  clusterNoPrimary := append(Cluster(nil), cluster...)
  clusterNoPrimary = append(clusterNoPrimary[:primary], clusterNoPrimary[primary + 1:]...)

  checkGetOperation(t, clusterNoPrimary, key,
    func(t *testing.T, response *GetResponse, err error) bool {
      // should not be able to finish get, since primary is down
      if err == nil {
        t.Logf("Get didn't acquire lease from primary\n")
        return false
      }

      if err != ErrCouldNotGetLease {
        t.Logf("Get had incorrect error %v\n", err)
        return false
      }

      return true
    })

  validateGet := func(t *testing.T, response *GetResponse, err error) bool {
    // should be able to acquire lease and finish get
    if err != nil {
      t.Logf("Get error: %v\n", err)
      return false
    }

    if response.Status != GET_SUCCESS {
      t.Logf("Expected SUCCESS status, got %v\n", response.Status)
      return false
    }

    if !(response.Exists && response.Value == value) {
      t.Logf("Incorrect value: %v -> %v, %v\n", key,
        response.Value, response.Exists)
      return false
    }

    return true
  }

  cluster.enableTo(primary)
  checkGetOperation(t, clusterNoPrimary, key, validateGet)

  cluster.disableTo(primary)
  time.Sleep(LEASE_DURATION / 2)

  checkGetOperation(t, clusterNoPrimary, key, validateGet)
  cluster.killAll()
  log.Printf("\n\nTestGetLease(): End\n\n")
}

func TestGetInvalid(t *testing.T) {
  log.Printf("\n\nTestGetInvalid(): Begin\n\n")
  numPeers := 5
  peers, cluster := makeCluster(numPeers, "gl")

  var client Client
  client.Init("/tmp/gl-client.sock", peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX)

  primary := cluster[0].primary
  invalid1 := (primary + 1) % numPeers
  invalid2 := (primary + 2) % numPeers

  // make invalid nodes miss set operation
  cluster.disable(primary, invalid1)
  cluster.disable(primary, invalid2)

  key, value := "quick brown", "fox jumps"
  client.Set(key, value)

  // allow invalid nodes to acquire get lease
  cluster.enable(primary, invalid1)
  cluster.enable(primary, invalid2)

  // invalid nodes should return a delayed response
  invalidCluster := Cluster{cluster[invalid1], cluster[invalid2]}
  checkGetOperation(t, invalidCluster, key,
    func(t *testing.T, response *GetResponse, err error) bool {
      if err != nil {
        t.Logf("Get error: %v\n", err)
        return false
      }

      if response.Status != GET_DELAYED {
        t.Logf("Expected DELAYED status, got %v\n", response.Status)
        return false
      }

      return true
    })

  // give invalid nodes enough time to request key
  time.Sleep(REQUEST_KV_PAIR_TIMEOUT)

  checkGetOperation(t, invalidCluster, key,
    func(t *testing.T, response *GetResponse, err error) bool {
      if err != nil {
        t.Logf("Get error: %v\n", err)
        return false
      }

      if response.Status != GET_SUCCESS {
        t.Logf("Expected SUCCESS status, got %v\n", response.Status)
        return false
      }

      if !(response.Exists && response.Value == value) {
        t.Logf("Incorrect value: %v -> %v, %v\n", key,
          response.Value, response.Exists)
        return false
      }

      return true
    })

  cluster.killAll()
  log.Printf("\n\nTestGetInvalid(): End\n\n")
}

var chars = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
var numChars = len(chars)

func generateString(size int) string {
  str := make([]rune, size)
  for i := 0; i < size; i++ {
    str[i] = chars[rand.Intn(numChars)]
  }

  return string(str)
}

func BenchmarkGet(b *testing.B) {
  numPeers := 5
  peers, cluster := makeCluster(numPeers, "bg")

  var client Client
  client.Init("/tmp/bg-client.sock", peers)

  numPairs := 10000
  keys := make([]string, numPairs)
  values := make([]string, numPairs)

  fmt.Printf("loading kv\n")
  for i := 0; i < numPairs; i++ {
    keys[i] = generateString(16)
    values[i] = generateString(32)

    client.Set(keys[i], values[i])
  }

  fmt.Printf("starting test\n")
  b.ResetTimer()
  for i := 0; i < b.N; i++ {
    index := rand.Intn(numPairs)
    value, exists := client.Get(keys[index])

    if !exists {
      log.Printf("%v doesn't have an associated value\n", keys[index])
    } else if value != values[index] {
      log.Printf("incorrect value %v, expected %v\n", value, values[index])
    }
  }

  cluster.killAll()
}

func TestGetThroughput(t *testing.T) {
  numPeers := 5
  peers, cluster := makeCluster(numPeers, "bg")

  var seedClient Client
  seedClient.Init("/tmp/gth-client.sock", peers)

  numPairs := 10000
  keys := make([]string, numPairs)
  values := make([]string, numPairs)

  fmt.Printf("loading kv\n")
  for i := 0; i < numPairs; i++ {
    keys[i] = generateString(16)
    values[i] = generateString(32)

    seedClient.Set(keys[i], values[i])
  }

  fmt.Printf("starting test\n")
  numClients := 10

  numOps := make([]int, numClients)
  doneCh := make(chan bool)

  var wg sync.WaitGroup
  wg.Add(numClients)

  start := time.Now()

  for i := 0; i < numClients; i++ {
    go func(i int) {
      var client Client
      clientSock := fmt.Sprintf("/tmp/bg-client%v.sock", i)
      client.Init(clientSock, peers)

      randSrc := rand.NewSource(time.Now().UnixNano())
      randGen := rand.New(randSrc)

      MainLoop:
      for {
        index := randGen.Intn(numPairs)
        value, exists := client.Get(keys[index])

        if !exists {
          log.Printf("%v doesn't have an associated value\n", keys[index])
        } else if value != values[index] {
          log.Printf("incorrect value %v, expected %v\n", value, values[index])
        }

        numOps[i] += 1

        select {
        case <-doneCh:
          break MainLoop
        default:
        }
      }

      wg.Done()
    }(i)
  }

  time.Sleep(5 * time.Second)
  close(doneCh)

  wg.Wait()
  end := time.Now()

  totalOps := 0
  for i := 0; i < numClients; i++ {
    totalOps += numOps[i]
  }

  fmt.Printf("total time = %v, ops = %v\n", end.Sub(start), totalOps)
  cluster.killAll()
}
