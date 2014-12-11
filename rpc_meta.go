package cc

import (
  "time"
  "crypto/sha256"
  "./op_log"
)

const (
  // if true, use unix sockets; if false, use tcp
  USE_UNIX_SOCKETS = false

  // time to wait for RPCs
  RPC_TIMEOUT = 30 * time.Millisecond

  // time to wait before retrying parallel RPCs
  WAIT_TIME_INITIAL = 1 * time.Millisecond

  // factor to increase wait time by to not congest network
  WAIT_TIME_MULTIPLICATIVE_INCREASE = 2

  // max wait time
  WAIT_TIME_MAX = 5 * time.Second

  // get operation succeded
  GET_SUCCESS = iota

  // can't get because peer isn't up-to-date
  GET_DELAYED = iota

  // vote given to this node
  VOTE_GRANTED = iota

  // vote already given to another node
  VOTE_ALREADY_GRANTED = iota

  // vote refused, as a primary already exists or there's a newer view
  VOTE_REFUSED = iota

  // set operation succeeded
  SET_SUCCESS = iota

  // set operation failed because this node isn't the primary
  SET_REFUSED = iota

  // successfully prepared for operation
  PREP_SUCCESS = iota

  // can't prep because in a later view
  PREP_REFUSED = iota

  // can't prepare until up-to-date
  PREP_DELAYED = iota

  // lease given to this node
  LEASE_GRANTED = iota

  // lease given, but node first has to update its filter
  LEASE_UPDATE_FILTER = iota

  // lease refused, as there's a later view
  LEASE_REFUSED = iota

  // successfully requested kv-pair from primary
  REQUEST_KV_PAIR_SUCCESS = iota

  // request was refused because primary was in a different view
  REQUEST_KV_PAIR_REFUSED = iota

  // requester successfully updated the kv-pair in its local cache
  SEND_KV_PAIR_SUCCESS = iota

  // requester could not perform the update since it is in a different view
  SEND_KV_PAIR_REFUSED = iota
)


/* The Get() RPC takes in a key and returns its associated value. */
type GetArgs struct {
  Key string
}

type GetResponse struct {
  Status byte
  Exists bool
  Value string
}


/* The Set() RPC takes in a key-value pair, recording the association. */
type SetArgs struct {
  Key string
  Value string
}

type SetResponse struct {
  Status byte
}


/* The RequestVote() RPC requests a vote from a peer for a new primary. */
type RequestVoteArgs struct {
  NextView int
  NextPrimary int
  View int
  NextOpNum int
}

type RequestVoteResponse struct {
  Status byte
}


/* The Heartbeat() RPC lets the primary assert control over peers. */
type HeartbeatArgs struct {
  View int
  Primary int
}

type HeartbeatResponse struct {
  Success bool
}


/* The Prep() RPC prepares a key value pair for insertion. */
type PrepArgs struct {
  View int
  Invalid bool
  StartOpNum int
  NextOpNum int
  Key string
  Ops []op_log.Operation
}

type PrepResponse struct {
  Status byte
}


/* The Commit() RPC inserts/updates a key value pair. */
type CommitArgs struct {
  View int
  NextOpNum int
  Key string
  Value string
}

type CommitResponse struct {
  Success bool
}


/* The RequestLease() RPC requests a lease to handle get requests. */
type RequestLeaseArgs struct {
  View int
  Node int  // sending node
  FilterHash [sha256.Size]byte
  Now time.Time
}

type RequestLeaseResponse struct {
  Status byte
  Filter op_log.Filter
  Until time.Time
}


/* The RequestKVPair() RPC requests a key-value pair from the primary */
type RequestKVPairArgs struct {
  View int
  Node int
  Key string
}

type RequestKVPairResponse struct {
  Status byte
}


/* The SendKVPair() RPC sends a key-value pair from the primary to the requesting node */
type SendKVPairArgs struct {
  View int
  Node int
  Exists bool
  Key string
  Value string
}

type SendKVPairResponse struct {
  Status byte
}


/* The RevokeLease() RPC revokes a lease to handle get requests. */
type RevokeLeaseArgs struct {
  View int
}

type RevokeLeaseResponse struct {
  Success bool
}


/* The RequestRecovery() RPC requests the primary to recover this node. */
type RequestRecoveryArgs struct {
  View int
  Node int
}

type RequestRecoveryResponse struct {
  Success bool
}


/* The Recover() RPC recovers a node. */
type RecoverArgs struct {
  View int
  Pairs map[string]string
  Filters []op_log.Filter
}

type RecoverResponse struct {
  Success bool
}
