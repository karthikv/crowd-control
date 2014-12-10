package op_log

import "log"


type OperationLog struct {
  start int  // beginning of ring buffer
  length int  // number of ops in buffer
  capacity int  // number of possible ops in buffer
  ops []Operation

  // node -> index into op log
  opNums []int
}


func (ol *OperationLog) invalidateOldestNodes() {
  // we know oldest nodes have opNum == ol.start; invalidate them
  for node, opNum := range ol.opNums {
    if opNum == ol.start {
      ol.opNums[node] = -1
    }
  }

  // maintain invariant oldest opNum == ol.start
  ol.updateStart()
}

func (ol *OperationLog) updateStart() {
  oldStart := ol.start
  ol.start = -1

  // pick least non-negative op number
  for _, opNum := range ol.opNums {
    if opNum >= 0 && (opNum < ol.start || ol.start == -1) {
      ol.start = opNum
    }
  }

  ol.length -= ol.start - oldStart
}


func (ol *OperationLog) Append(op Operation) {
  index := (ol.start + ol.length) % ol.capacity

  if ol.length == ol.capacity {
    ol.invalidateOldestNodes()
  }

  ol.ops[index] = op
  ol.length += 1
}


func (ol *OperationLog) FastForward(node int, opNum int) {
  if opNum < ol.opNums[node] {
    // log.Printf("Redundant fast-forward\n")
    return
  }

  if opNum > ol.GetNextOpNum() {
    log.Printf("Invalid fast-forward[%v] to %v, should be <= %v\n",
      node, opNum, ol.GetNextOpNum())
    return
  }

  // if opNums[node] = n, node hasn't executed operation n and all future ops
  ol.opNums[node] = opNum

  // maintain invariant oldest opNum == ol.start
  ol.updateStart()
}


/* Returns (invalid, array of pending ops). invalid is true if this node has to
 * recover (i.e. it's not up to date). */
func (ol *OperationLog) GetPending(node int) (bool, []Operation) {
  startOp := ol.opNums[node]
  if startOp == -1 {
    return true, nil
  }

  numOps := (ol.start + ol.length) - startOp
  pendingOps := make([]Operation, numOps, numOps)

  for i := 0; i < numOps; i++ {
    index := (startOp + i) % ol.capacity
    pendingOps[i] = ol.ops[index]
  }

  return false, pendingOps
}

func (ol *OperationLog) GetNextOpNum() int {
  return ol.start + ol.length
}

func (ol *OperationLog) Init(capacity int, numNodes int) {
  ol.start = 0
  ol.length = 0
  ol.capacity = capacity

  ol.ops = make([]Operation, capacity, capacity)
  ol.opNums = make([]int, numNodes, numNodes)
}

