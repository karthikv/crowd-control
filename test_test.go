package cc

import (
  "fmt"
  "log"
  "testing"
  "time"
)

func makeCluster(numPeers int, prefix string) ([]string, []*CrowdControl) {
  peers := make([]string, numPeers, numPeers)
  for i := 0; i < numPeers; i++ {
    peers[i] = fmt.Sprintf("/tmp/%v%v.sock", prefix, i)
  }
  ccs := make([]*CrowdControl, numPeers)

  for i := 0; i < numPeers; i++ {
    cc := &CrowdControl{}
    cc.Init(peers, i)
    ccs[i] = cc
  }

  return peers, ccs
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

  _, ccs := makeCluster(5, "le")
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  primary := ccs[0].primary
  view := ccs[0].view

  if primary == -1 {
    t.Fatalf("No primary elected\n")
  }

  for _, cc := range ccs {
    checkView(t, cc, view, primary)
  }

  log.Printf("Primary %v successfully elected\n", primary)
  time.Sleep(1 * time.Second)

  for i := 0; i < 2; i++ {
    log.Printf("Primary %v going down\n", primary)
    ccs[primary].dead = true

    time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)
    for _, cc := range ccs {
      if !cc.dead {
        primary = cc.primary
        view = cc.view
        break
      }
    }

    if primary == -1 {
      t.Fatalf("No primary re-elected after %v ms\n", 2 * ELECTION_TIMEOUT_MAX)
    }

    for _, cc := range ccs {
      if cc.dead {
        continue
      }

      checkView(t, cc, view, primary)
    }

    log.Printf("Primary %v successfully elected\n", primary)
    time.Sleep(1 * time.Second)
  }

  log.Printf("Primary %v going down\n", primary)
  ccs[primary].dead = true
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  for _, cc := range ccs {
    if cc.dead {
      continue
    }

    checkView(t, cc, view, primary)
  }

  log.Printf("\n\nTestLeaderElection(): End\n\n")
}

func checkBasicGetSetOps(t *testing.T, client *Client) {
  client.Set("foo", "bar")
  client.Set("john", "doe")

  // wait for values to propagate
  time.Sleep(100 * time.Microsecond)

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
  peers, _ := makeCluster(1, "gss")

  var client Client
  client.Init("/tmp/gss0-client.sock", peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  checkBasicGetSetOps(t, &client)
  log.Printf("\n\nTestGetSetSingle(): End\n\n")
}

// multiple server, single client
func TestGetSetMultiple(t *testing.T) {
  log.Printf("\n\nTestGetSetMultiple(): Begin\n\n")
  peers, _ := makeCluster(5, "gsm")

  var client Client
  client.Init("/tmp/gsm0-client.sock", peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  checkBasicGetSetOps(t, &client)
  log.Printf("\n\nTestGetSetMultiple(): End\n\n")
}

func TestPrimarySelection(t *testing.T) {
  log.Printf("\n\nTestPrimarySelection(): Begin\n\n")
  numPeers := 5
  peers, ccs := makeCluster(numPeers, "ps")

  var client Client
  client.Init("/tmp/ps-client.sock", peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  primary := ccs[0].primary
  view := ccs[0].view

  if primary == -1 {
    t.Fatalf("No primary elected\n")
  }

  for _, cc := range ccs {
    checkView(t, cc, view, primary)
  }

  client.Set("lorem", "ipsum")

  // partition node
  log.Printf("partitioning node to make it stale\n")
  staleNode := (primary + 1) % numPeers
  for i, _ := range peers {
    if i != staleNode {
      ccs[staleNode].rejectConnFrom(i)
      ccs[i].rejectConnFrom(staleNode)
    }
  }

  // do set operation to make node stale
  client.Set("dolor sit", "amet")

  // kill primary and make nodes only respond to stale node
  log.Printf("trying to make stale node the new primary\n")
  ccs[primary].dead = true
  for i, _ := range peers {
    ccs[i].rejectConnFromAll()
  }

  for i, _ := range peers {
    ccs[i].acceptConnFrom(staleNode)
  }

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  // view/primary should be the same
  for _, cc := range ccs {
    if !cc.dead {
      checkView(t, cc, view, primary)
    }
  }

  // make nodes communicate with non-stale node
  log.Printf("trying to make good node the new primary\n")
  goodNode := (staleNode + 1) % numPeers
  for i, _ := range peers {
    ccs[i].rejectConnFromAll()
  }

  for i, _ := range peers {
    ccs[i].acceptConnFrom(staleNode)
    ccs[i].acceptConnFrom(goodNode)
  }

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  // view/primary should update
  primary = goodNode
  if ccs[goodNode].view < view {
    log.Fatalf("view did not update\n")
  }
  view = ccs[goodNode].view

  for _, cc := range ccs {
    if !cc.dead {
      checkView(t, cc, view, primary)
    }
  }

  // reset cluster
  for i, _ := range peers {
    ccs[i].acceptConnFromAll()
  }

  // make all nodes up-to-date
  client.Set("consectetur", "adipiscing")

  // ensure previously-stale node can now be elected
  log.Printf("trying to make previously-stale node the new primary\n")
  ccs[primary].dead = true

  for i, _ := range peers {
    ccs[i].rejectConnFromAll()
  }

  for i, _ := range peers {
    ccs[i].acceptConnFrom(staleNode)
  }

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  // view/primary should update
  primary = staleNode
  if ccs[staleNode].view < view {
    log.Fatalf("view did not update\n")
  }
  view = ccs[staleNode].view

  for _, cc := range ccs {
    if !cc.dead {
      checkView(t, cc, view, primary)
    }
  }

  log.Printf("\n\nTestPrimarySelection(): End\n\n")
}
