package cc

import (
  "log"
  "testing"
  "time"
)

func TestLeaderElection(testing *testing.T) {
  log.Printf("TestLeaderElection(): Begin\n")

  peers := []string{"/tmp/le0.sock", "/tmp/le1.sock", "/tmp/le2.sock",
    "/tmp/le3.sock", "/tmp/le4.sock"}
  ccs := make([]*CrowdControl, len(peers))

  for i, _ := range peers {
    cc := &CrowdControl{}
    cc.Init(peers, i)
    ccs[i] = cc
  }

  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)
  primary := ccs[0].primary
  view := ccs[0].view

  if primary == -1 {
    testing.Fatalf("No primary elected after %v ms\n", 2 * ELECTION_TIMEOUT_MAX)
  }

  for _, cc := range ccs {
    if cc.primary != primary {
      testing.Fatalf("Disagreement on primary: %v and %v\n", primary, cc.primary)
    }

    if cc.view != view {
      testing.Fatalf("Disagreement on view: %v and %v\n", view, cc.view)
    }
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
      testing.Fatalf("No primary re-elected after %v ms\n", 2 * ELECTION_TIMEOUT_MAX)
    }

    for _, cc := range ccs {
      if cc.dead {
        continue
      }

      if cc.primary != primary {
        testing.Fatalf("Disagreement on primary: %v and %v\n", primary, cc.primary)
      }

      if cc.view != view {
        testing.Fatalf("Disagreement on view: %v and %v\n", view, cc.view)
      }
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

    if cc.primary != primary {
      testing.Fatalf("Primary change without majority: %v to %v\n", primary, cc.primary)
    }

    if cc.view != view {
      testing.Fatalf("View change without majority: %v to %v\n", view, cc.view)
    }
  }

  log.Printf("TestLeaderElection(): End\n")
}

func checkBasicGetSetOps(testing *testing.T, client *Client) {
  client.Set("foo", "bar")
  client.Set("john", "doe")

  // wait for values to propagate
  time.Sleep(100 * time.Microsecond)

  value, exists := client.Get("foo")
  if !(exists && value == "bar") {
    testing.Fatalf("Incorrect value: foo -> %v, %v\n", value, exists)
  }

  value, exists = client.Get("john")
  if !(exists && value == "doe") {
    testing.Fatalf("Incorrect value: john -> %v, %v\n", value, exists)
  }

  value, exists = client.Get("other")
  if exists {
    testing.Fatal("Key should not exist: other\n")
  }

  value, exists = client.Get("joh")
  if exists {
    testing.Fatal("Key should not exist: joh\n")
  }

  // case sensitive
  value, exists = client.Get("Foo")
  if exists {
    testing.Fatal("Key should not exist: Foo\n")
  }
}

// single client, single server
func TestGetSetSingle(testing *testing.T) {
  log.Printf("TestGetSetSingle(): Begin\n")

  peers := []string{"/tmp/gss0.sock"}
  ccs := make([]*CrowdControl, len(peers))

  for i, _ := range peers {
    cc := &CrowdControl{}
    cc.Init(peers, i)
    ccs[i] = cc
  }

  var client Client
  client.Init(peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  checkBasicGetSetOps(testing, &client)
  log.Printf("TestGetSetSingle(): End\n")
}

// multiple server, single client
func TestGetSetMultiple(testing *testing.T) {
  log.Printf("TestGetSetMultiple(): Begin\n")

  peers := []string{"/tmp/gsm0.sock", "/tmp/gsm1.sock", "/tmp/gsm2.sock",
    "/tmp/gsm3.sock", "/tmp/gsm4.sock"}
  ccs := make([]*CrowdControl, len(peers))

  for i, _ := range peers {
    cc := &CrowdControl{}
    cc.Init(peers, i)
    ccs[i] = cc
  }

  var client Client
  client.Init(peers)

  // wait for leader election
  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  checkBasicGetSetOps(testing, &client)
  log.Printf("TestGetSetMultiple(): End\n")
}
