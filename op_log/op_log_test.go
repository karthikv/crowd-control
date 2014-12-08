package op_log

import (
  "fmt"
  "testing"
)


func TestAppend(t *testing.T) {
  ol := &OperationLog{}
  capacity := 256
  ol.Init(capacity, 1)

  ops := make([]Operation, capacity, capacity)

  for i := 0; i < capacity; i++ {
    key := fmt.Sprintf("%v", i)
    ops[i] = AddOperation{Key: key}
    ol.Append(ops[i])
  }

  if ol.length != capacity {
    t.Fatalf("Invalid length\n")
  }

  for i := 0; i < capacity; i++ {
    if ol.ops[i] != ops[i] {
      t.Fatalf("Invalid op %v, expected %v\n", ol.ops[i], ops[i])
    }
  }
}


func TestFastForward(t *testing.T) {
  ol := &OperationLog{}
  capacity := 256
  ol.Init(capacity, 1)

  for i := 0; i < capacity - 100; i++ {
    op := AddOperation{Key: "k"}
    ol.Append(op)
  }

  if ol.length != capacity - 100 {
    t.Fatalf("Invalid length after inserting\n")
  }

  ol.FastForward(0, capacity - 100)

  if ol.length != 0 {
    t.Fatalf("Invalid length after fast forward\n")
  }

  for i := 0; i < 150; i++ {
    op := AddOperation{Key: "k"}
    ol.Append(op)
  }

  if ol.length != 150 {
    t.Fatalf("Invalid length after insert\n")
  }
}


func checkPending(t *testing.T, ol *OperationLog, node int, ops []Operation,
    shouldBeInvalid bool) {
  invalid, pending := ol.GetPending(node)

  if shouldBeInvalid {
    if !invalid {
      t.Fatalf("Should be invalid\n")
    }
    return
  }

  if len(pending) != len(ops) {
    t.Fatalf("Expected %v pending ops\n", len(ops))
  }

  for i, op := range pending {
    if op != ops[i] {
      t.Fatalf("Invalid op, got %v expected %v\n", op, ops[i])
    }
  }
}


func TestGetPending(t *testing.T) {
  ol := &OperationLog{}
  capacity := 256
  ol.Init(capacity, 1)

  numAppend := capacity - 100
  ops := make([]Operation, capacity, capacity)

  for i := 0; i < numAppend; i++ {
    key := fmt.Sprintf("%v", i)
    ops[i] = AddOperation{Key: key}
    ol.Append(ops[i])
  }

  if ol.length != numAppend {
    t.Fatalf("Invalid length after inserting\n")
  }

  checkPending(t, ol, 0, ops[0:numAppend], false)

  for i := numAppend; i < capacity; i++ {
    key := fmt.Sprintf("%v", i)
    ops[i] = AddOperation{Key: key}
    ol.Append(ops[i])
  }

  checkPending(t, ol, 0, ops, false)

  overflowOp := AddOperation{Key: "overflow"}
  ol.Append(overflowOp)

  checkPending(t, ol, 0, ops, true)
}

func TestIntegration(t *testing.T) {
  ol := &OperationLog{}
  capacity := 256
  ol.Init(capacity, 3)

  ops := make([]Operation, 300, 300)

  for i := 0; i < 100; i++ {
    key := fmt.Sprintf("%v", i)
    ops[i] = AddOperation{Key: key}
    ol.Append(ops[i])
  }

  ol.FastForward(0, 100)

  if ol.length != 100 {
    t.Fatalf("Invalid length after single fast forward\n")
  }

  for i := 100; i < 200; i++ {
    key := fmt.Sprintf("%v", i)
    ops[i] = AddOperation{Key: key}
    ol.Append(ops[i])
  }

  ol.FastForward(1, 200)

  if ol.length != 200 {
    t.Fatalf("Invalid length after single fast forward\n")
  }

  // node 0 - 100
  // node 1 - 200
  // node 2 - 0

  checkPending(t, ol, 0, ops[100:200], false)
  checkPending(t, ol, 1, ops[200:200], false)
  checkPending(t, ol, 2, ops[0:200], false)

  for i := 200; i < 300; i++ {
    key := fmt.Sprintf("%v", i)
    ops[i] = AddOperation{Key: key}
    ol.Append(ops[i])
  }

  // node 0 - 100
  // node 1 - 200
  // node 2 - invalid

  checkPending(t, ol, 0, ops[100:300], false)
  checkPending(t, ol, 1, ops[200:300], false)
  checkPending(t, ol, 2, ops, true)

  ol.FastForward(0, 300)

  if ol.length != 100 {
    t.Fatalf("Invalid length after single fast forward\n")
  }

  // node 0 - 300
  // node 1 - 200
  // node 2 - invalid

  checkPending(t, ol, 0, ops[300:300], false)
  checkPending(t, ol, 1, ops[200:300], false)
  checkPending(t, ol, 2, ops, true)

  // should be idempotent
  ol.FastForward(0, 300)

  if ol.length != 100 {
    t.Fatalf("Invalid length after single fast forward\n")
  }

  checkPending(t, ol, 0, ops[300:300], false)
  checkPending(t, ol, 1, ops[200:300], false)
  checkPending(t, ol, 2, ops, true)

  ol.FastForward(2, 300)

  if ol.length != 100 {
    t.Fatalf("Invalid length after single fast forward\n")
  }

  // node 0 - 300
  // node 1 - 200
  // node 2 - 300

  checkPending(t, ol, 0, ops[300:300], false)
  checkPending(t, ol, 1, ops[200:300], false)
  checkPending(t, ol, 2, ops[300:300], false)

  ol.FastForward(1, 300)

  if ol.length != 0 {
    t.Fatalf("Invalid length after single fast forward\n")
  }

  // node 0 - 300
  // node 1 - 300
  // node 2 - 300

  checkPending(t, ol, 0, ops[300:300], false)
  checkPending(t, ol, 1, ops[300:300], false)
  checkPending(t, ol, 2, ops[300:300], false)

  // should be idempotent
  ol.FastForward(1, 300)

  if ol.length != 0 {
    t.Fatalf("Invalid length after single fast forward\n")
  }

  checkPending(t, ol, 0, ops[300:300], false)
  checkPending(t, ol, 1, ops[300:300], false)
  checkPending(t, ol, 2, ops[300:300], false)
}
