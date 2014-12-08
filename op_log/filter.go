package op_log

import (
  "hash/fnv"
  "crypto/sha256"
)


const (
  BITS_PER_COUNTER = 4

  COUNTERS_PER_ELEMENT = 12
  COUNTERS_PER_BYTE = 2
  COUNTER_MASK = byte(0xf)

  HASH_OFFSET = 32
  HASH_MASK = uint64(0xffffffff)
)


/* Represents a counting bloom filter using 4 bits for each counter. */
type Filter struct {
  Data []uint8  // actual bit data

  NumCounters int  // number of counters
  NumHashes int  // number of hash functions to use

  Size int  // how many elements are stored
  Capacity int  // number of elements to hold
}


/* Adds a key to this filter. */
func (filter *Filter) Add(key []byte) {
  counterIndexes := filter.getCounterIndexes(key)
  for _, index := range counterIndexes {
    filter.set(index, filter.get(index) + 1)
  }

  filter.Size += 1
}


/* Removes a key from this filter. */
func (filter *Filter) Remove(key []byte) {
  // remove keeps removing until the key is gone from the filter
  for filter.Contains(key) {
    counterIndexes := filter.getCounterIndexes(key)
    for _, index := range counterIndexes {
      filter.set(index, filter.get(index) - 1)
    }

    filter.Size -= 1
  }
}


/* Checks if the filter contains the given key. */
func (filter *Filter) Contains(key []byte) bool {
  counterIndexes := filter.getCounterIndexes(key)
  for _, index := range counterIndexes {
    if filter.get(index) == 0 {
      return false
    }
  }

  return true
}


/* Returns the counter indexes to increment for the given key. */
func (filter *Filter) getCounterIndexes(key []byte) []int {
  hash := fnv.New64a()
  hash.Reset()
  hash.Write(key)

  // we can generate an arbitrary number of hash functions by taking the left
  // and right halves of an fnv hash and superimposing them
  value := hash.Sum64()
  left := uint64(value & HASH_MASK)
  right := uint64((value >> HASH_OFFSET) & HASH_MASK)

  counterIndexes := make([]int, filter.NumHashes)
  numHashes64 := uint64(filter.NumHashes)
  numCounters64 := uint64(filter.NumCounters)

  for i := uint64(0); i < numHashes64; i++ {
    counterIndexes[i] = int((left + i * right) % numCounters64)
  }
  return counterIndexes
}


/* Gets the data index and offset for a given counter index. */
func (filter *Filter) getDataIndexOffset(counterIndex int) (int, uint8) {
  dataIndex := counterIndex / COUNTERS_PER_BYTE
  offset := uint8((counterIndex % COUNTERS_PER_BYTE) * BITS_PER_COUNTER)
  return dataIndex, offset
}


/* Gets the count at the given counter index. */
func (filter *Filter) get(counterIndex int) uint8 {
  dataIndex, offset := filter.getDataIndexOffset(counterIndex)
  return (filter.Data[dataIndex] >> offset) & COUNTER_MASK
}


/* Sets the count at the given counter index. */
func (filter *Filter) set(counterIndex int, value uint8) {
  if value >= (1 << BITS_PER_COUNTER) {
    return
  }

  dataIndex, offset := filter.getDataIndexOffset(counterIndex)
  clearMask := ^COUNTER_MASK

  filter.Data[dataIndex] &= (clearMask << offset)
  filter.Data[dataIndex] |= (value << offset)
}


/* Returns a hash of this filter for comparison purposes. */
func (filter *Filter) Hash() [sha256.Size]byte {
  return sha256.Sum256(filter.Data)
}


/* Initializes this filter for use. */
func (filter *Filter) Init(capacity int, numHashes int) {
  // TODO: adjust counters per element
  // TODO: adjust remove op functionality
  filter.Data = make([]uint8, capacity * COUNTERS_PER_ELEMENT / COUNTERS_PER_BYTE)
  filter.NumCounters = len(filter.Data) * COUNTERS_PER_BYTE

  filter.Size = 0
  filter.Capacity = capacity

  filter.NumHashes = numHashes
}
