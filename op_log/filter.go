package op_log

import (
  "log"
  "hash"
  "hash/fnv"
)


const (
  BITS_PER_COUNTER = 4

  COUNTERS_PER_ELEMENT = 8
  COUNTERS_PER_BYTE = 2
  COUNTER_MASK = byte(0xf)

  HASH_OFFSET = 32
  HASH_MASK = uint64(0xffffffff)
)


/* Represents a counting bloom filter using 4 bits for each counter. */
type Filter struct {
  data []uint8  // actual bit data
  numCounters int  // number of counters

  size int
  capacity int  // number of elements to hold

  numHashes int  // number of hash functions to use
  hash hash.Hash64
}


/* Adds a key to this filter. */
func (filter *Filter) Add(key []byte) {
  counterIndexes := filter.getCounterIndexes(key)
  for _, index := range counterIndexes {
    filter.set(index, filter.get(index) + 1)
  }

  filter.size += 1
}


/* Removes a key from this filter. */
func (filter *Filter) Remove(key []byte) {
  if !filter.Contains(key) {
    log.Printf("Removing element that doesn't exist in filter.\n")
    return
  }

  counterIndexes := filter.getCounterIndexes(key)
  for _, index := range counterIndexes {
    filter.set(index, filter.get(index) - 1)
  }

  filter.size -= 1
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


/* Returns the number of items in this filter. */
func (filter *Filter) Size() int {
  return filter.size
}


/* Returns the capacity of the filter. */
func (filter *Filter) Capacity() int {
  return filter.capacity
}


/* Returns the counter indexes to increment for the given key. */
func (filter *Filter) getCounterIndexes(key []byte) []int {
  filter.hash.Reset()
  filter.hash.Write(key)

  // we can generate an arbitrary number of hash functions by taking the left
  // and right halves of an fnv hash and superimposing them
  value := filter.hash.Sum64()
  left := uint64(value & HASH_MASK)
  right := uint64((value >> HASH_OFFSET) & HASH_MASK)

  counterIndexes := make([]int, filter.numHashes)
  numHashes64 := uint64(filter.numHashes)
  numCounters64 := uint64(filter.numCounters)

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
  return (filter.data[dataIndex] >> offset) & COUNTER_MASK
}


/* Sets the count at the given counter index. */
func (filter *Filter) set(counterIndex int, value uint8) {
  if value >= (1 << BITS_PER_COUNTER) {
    return
  }

  dataIndex, offset := filter.getDataIndexOffset(counterIndex)
  clearMask := ^COUNTER_MASK

  filter.data[dataIndex] &= (clearMask << offset)
  filter.data[dataIndex] |= (value << offset)
}


/* Initializes this filter for use. */
func (filter *Filter) Init(capacity int, numHashes int) {
  filter.data = make([]uint8, capacity * COUNTERS_PER_ELEMENT / COUNTERS_PER_BYTE)
  filter.numCounters = len(filter.data) * COUNTERS_PER_BYTE

  filter.size = 0
  filter.capacity = capacity

  filter.numHashes = numHashes
  filter.hash = fnv.New64a()
}
