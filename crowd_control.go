package cc

import (
  "log"
  "sync"
  "net"
  "net/rpc"
  "math/rand"
  "os"
  "time"
  "errors"
  "encoding/gob"

  "./op_log"
  "./cache"
)

const (
  // min/max time until election
  ELECTION_TIMEOUT_MIN_INT = 150
  ELECTION_TIMEOUT_MIN = ELECTION_TIMEOUT_MIN_INT * time.Millisecond
  ELECTION_TIMEOUT_MAX_INT = 300
  ELECTION_TIMEOUT_MAX = ELECTION_TIMEOUT_MAX_INT * time.Millisecond

  // time to wait before starting election
  ELECTION_RPCS_TIMEOUT = 100 * time.Millisecond

  // time between heartbeats
  HEARTBEAT_TIMEOUT = 20 * time.Millisecond

  // time to wait for heartbeat responses
  HEARTBEAT_RPCS_TIMEOUT = HEARTBEAT_TIMEOUT

  // time to wait for prep responses
  PREP_RPCS_TIMEOUT = 50 * time.Millisecond

  // time to wait for commit responses
  COMMIT_RPCS_TIMEOUT = PREP_RPCS_TIMEOUT

  // Time to wait for revoke lease responses. If we can't revoke leases, we
  // likely can't heartbeat either. After ELECTION_TIMEOUT_MAX, other nodes
  // will start an election, clearing their leases.
  REVOKE_LEASE_RPCS_TIMEOUT = ELECTION_TIMEOUT_MAX

  // timeout for request operation
  REQUEST_KV_PAIR_TIMEOUT = COMMIT_RPCS_TIMEOUT

  // length of get lease
  LEASE_DURATION = 10 * time.Second

  // number of times to retry RPCs
  SERVER_RPC_RETRIES = 3

  // max length of operation log
  OP_LOG_CAPACITY = 256

  // counting bloom filter configuration
  FILTER_CAPACITY = 256
  FILTER_NUM_HASHES = 6
)


var ErrViewChange = errors.New("aborted due to view change")
var ErrCouldNotGetLease = errors.New("could not get lease")


/* Represents a mutex along with its number of users. */
type SetMutex struct {
  Mutex sync.Mutex
  NumUsers int
}


/* Represents a single CrowdControl peer that maintains consensus over a set of
 * key -> value pairs. */
type CrowdControl struct {
  mutex sync.Mutex
  listener net.Listener

  dead bool  // used by testing framework to determine if this node is dead
  rand *rand.Rand  // to generate random numbers

  // set of machines within the cluster
  peers []string  // node -> unix socket string
  numPeers int
  me int  // index of this machine

  nodes []int  // node numbers
  rts []*RPCTarget  // node -> RPCTarget that maintains persistent connection

  // views to disambiguate the primary, as in VR
  view int
  nextView int  // the next view number upon election timeout
  primary int
  nextOpNum int  // used for determining up-to-dateness

  // leader election
  votes map[int]int  // view -> which machine this peer votes for
  lastHeartbeat time.Time
  electionTimerCh chan bool

  // key-value pairs
  cache *cache.Cache

  // whether the data for a given key on a given node is invalid
  filters []*op_log.Filter

  // operation log
  ol *op_log.OperationLog

  // maintains key -> lock mappings; must lock a given key while setting
  setMutexes map[string]*SetMutex

  // last time the primary reached a majority of nodes
  reachedMajorityAt time.Time

  // lease to respond to get requests
  leaseUntil time.Time

  // leases granted by primary to nodes; node -> lease expiry mapping
  grantedLeasesUntil []time.Time

  // mapping from keys to the time when request for fetch for kv pair from primary was made
  inflightKeys map[string]time.Time
}


/* Creates a CrowdControl peer. `peers` is an array of peers within the
 * cluster, where each element is a string representing a socket. `me` is the
 * index of this peer. */
func (cc *CrowdControl) Init(peers []string, me int, capacity uint64) {
  source := rand.NewSource(time.Now().UnixNano())
  cc.rand = rand.New(source)

  cc.peers = peers
  cc.numPeers = len(peers)
  cc.me = me

  cc.nodes = make([]int, cc.numPeers, cc.numPeers)
  for i := 0; i < cc.numPeers; i++ {
    cc.nodes[i] = i
  }

  cc.rts = make([]*RPCTarget, cc.numPeers, cc.numPeers)
  for i := 0; i < cc.numPeers; i++ {
    rt := &RPCTarget{}
    rt.Init(peers[me], peers[i], i)
    cc.rts[i] = rt
  }

  cc.view = -1
  cc.nextView = 0
  cc.primary = -1
  cc.nextOpNum = 0

  cc.votes = make(map[int]int)
  cc.lastHeartbeat = time.Now()
  cc.electionTimerCh = make(chan bool, 1)

  cc.scheduleElection()
  cc.scheduleHeartbeat()

  cc.cache = &cache.Cache{}
  cc.cache.Init(capacity)
  cc.filters = make([]*op_log.Filter, cc.numPeers, cc.numPeers)

  for i := 0; i < cc.numPeers; i++ {
    cc.filters[i] = &op_log.Filter{}
    cc.filters[i].Init(FILTER_CAPACITY, FILTER_NUM_HASHES)
  }

  cc.ol = &op_log.OperationLog{}
  cc.ol.Init(OP_LOG_CAPACITY, cc.numPeers)

  cc.setMutexes = make(map[string]*SetMutex)
  cc.reachedMajorityAt = time.Now()
  cc.leaseUntil = time.Now()

  cc.grantedLeasesUntil = make([]time.Time, cc.numPeers, cc.numPeers)
  for i := 0; i < cc.numPeers; i++ {
    cc.grantedLeasesUntil[i] = time.Now()
  }

  cc.inflightKeys = make(map[string]time.Time)

  gob.Register(op_log.AddOperation{})
  gob.Register(op_log.RemoveOperation{})
  gob.Register(op_log.SetFilterOperation{})

  rpcServer := rpc.NewServer()
  rpcServer.Register(cc)

  var listener net.Listener
  var err error

  if USE_UNIX_SOCKETS {
    // remove any potentially stale socket
    os.Remove(peers[me])
    listener, err = net.Listen("unix", peers[me])
  } else {
    listener, err = net.Listen("tcp", peers[me])
  }

  if err != nil {
    log.Fatalf("CC[%v] Listen() failed: %v\n", me, err)
  }

  cc.listener = listener

  go func() {
    for {
      conn, err := cc.listener.Accept()

      if err != nil {
        log.Printf("CC[%v] Accept() failed: %v\n", me, err)
        continue
      }

      go rpcServer.ServeConn(conn)
    }
  }()
}
