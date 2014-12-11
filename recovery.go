package cc

import (
  "log"
  "./op_log"
)


func (cc *CrowdControl) RequestRecovery(args *RequestRecoveryArgs,
    response *RequestRecoveryResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.view != args.View || cc.me != cc.primary {
    response.Success = false
    return nil
  }

  response.Success = true

  go func() {
    cc.mutex.Lock()

    filters := make([]op_log.Filter, cc.numPeers)
    for i := 0; i < cc.numPeers; i++ {
      filters[i] = *cc.filters[i]
    }

    recoverArgs := &RecoverArgs{
      View: cc.view,
      Pairs: cc.cache.Export(),
      Filters: filters,
    }

    log.Printf("Got recovery request, making recovery rpc\n")
    ch := cc.rts[args.Node].MakeRPCRetry("CrowdControl.Recover", &recoverArgs,
      &RecoverResponse{}, SERVER_RPC_RETRIES)
    nextOpNum := cc.ol.GetNextOpNum()

    originalView := cc.view
    cc.mutex.Unlock()

    reply := <-ch

    cc.mutex.Lock()
    defer cc.mutex.Unlock()

    if cc.view != originalView {
      return
    }

    log.Printf("Updating op log for recovered node.\n")
    if reply.Success {
      recoverResponse := reply.Data.(*RecoverResponse)

      if recoverResponse.Success {
        // append to operation log; delete from node's filter
        op := op_log.SetFilterOperation{Nodes: []int{args.Node},
          Filters: []op_log.Filter{filters[cc.me]}}
        cc.appendOp(op)
        op.Perform(&cc.filters)

        cc.ol.FastForward(args.Node, nextOpNum)
      }
    }
  }()

  return nil
}


func (cc *CrowdControl) Recover(args *RecoverArgs, response *RecoverResponse) error {
  cc.mutex.Lock()
  defer cc.mutex.Unlock()

  if cc.view != args.View || !cc.recovering {
    response.Success = false
    return nil
  }

  for key, value := range args.Pairs {
    cc.cache.Set(key, value)
  }

  for i := 0; i < cc.numPeers; i++ {
    cc.filters[i] = &args.Filters[i]
  }

  log.Printf("Done recovering!\n")
  cc.recovering = false
  response.Success = true
  return nil
}
