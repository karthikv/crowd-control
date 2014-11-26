package cc

import (
  "log"
  "testing"
  "time"
)

func TestLeaderElection(testing *testing.T) {
  log.Printf("TestLeaderElection(): Begin\n")

  peers := []string{"/tmp/0.sock", "/tmp/1.sock", "/tmp/2.sock", "/tmp/3.sock", "/tmp/4.sock"}
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
}

func TestGetSetSingle(testing *testing.T) {
  log.Printf("TestGetSetSingle(): Begin\n")

  peers := []string{"/tmp/0.sock"}
  ccs := make([]*CrowdControl, len(peers))

  for i, _ := range peers {
    cc := &CrowdControl{}
    cc.Init(peers, i)
    ccs[i] = cc
  }

  var client Client
  client.Init(peers)

  time.Sleep(3 * ELECTION_TIMEOUT_MAX * time.Millisecond)

  ok := client.Set("foo", "bar")
  if !ok {
    testing.Fatal("Set foo -> bar failed\n")
  }

  ok = client.Set("john", "doe")
  if !ok {
    testing.Fatal("Set john -> doe failed\n")
  }

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

  log.Printf("TestGetSetSingle(): End\n")
}
