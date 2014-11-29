package cuckoofilter
import (
  "errors"
  "crypto/sha256"
  "math/rand"
  "math/big"
)
// Constants
const (
  BUCKET_SIZE = 4
  MAX_DISPLACEMENTS = 500
)
// Data structures to implement the cuckoo filter
type CuckooFilter struct {
  hashtable []bucketList
  size int // size of array of buckets
}
// Represents a bucket in each position of the hash table
type bucketList struct {
  fingerprints [BUCKET_SIZE]uint16 // 4 fingerprints
  numElements uint8 // number of elements
}

/****** General Utilities *******/

// Takes in a string and returns an 32-byte (64-bit) hash value. Using SHA-256.
func hash(s string) []byte {
  longchecksum := sha256.Sum256([]byte(s))
  return longchecksum[0:]
}
// Choose one of firstIndex and secondIndex randomly.
func choose(firstIndex, secondIndex uint64) uint64 {
  i := rand.Intn(2)
  if i == 0 {
    return firstIndex
  }
  return secondIndex
}

// Given two byte arrays, returns a new XORed byte array 
func xor(a, b []byte) []byte {
  var min int
  if len(a) < len(b) {
    min = len(a)
  } else {
    min = len(b)
  }
  result := make([]byte, min)
  for i := 0; i < min; i++ {
    result[i] = a[i] ^ b[i]
  }
  return result
}

func getFingerprint(a []byte) uint16 {
  xor := new(big.Int)
  xor.SetBytes(a)
  result := uint16(xor.Uint64() >> 48)
  return result
}

/**** To instantiate a new cuckoo filter, call this *****/
func Make(size int) (*CuckooFilter, error) {
  if size <= 0 {
    return nil, errors.New("Specify a positive size.")
  }
  cf := &CuckooFilter { 
    size: size, 
    hashtable: make([]bucketList, size)}
  return cf, nil
}

/**** Private methods *****/

// Takes in a fingerprint and a bucket index, and inserts the fingerprint at the end if possible. Otherwise, returns false. Doesn't displace anything.
func (cf *CuckooFilter) insert(index uint64, fingerprint uint16) bool {
  bucket := &cf.hashtable[index]
  if bucket.numElements < BUCKET_SIZE {
    bucket.fingerprints[bucket.numElements] = fingerprint
    bucket.numElements++
    return true
  }
  return false
}

// This function is responsible for the chained displacements of fingerprints. Forces an insert of fingerprint and the relevant index, and then chains displace for the evicted fingerprint. Returns false ('too full') in case number of displacements is over MAX_DISPLACEMENTS
func (cf *CuckooFilter) displace(fingerprint uint16, index uint64) bool {
  var displacements int = 1
  firstIndex, intermediateXor, newIndex, cuckooSize := big.NewInt(int64(index)), new(big.Int), new(big.Int), big.NewInt(int64(cf.size))
  for displacements < MAX_DISPLACEMENTS {
    if cf.insert(index, fingerprint) {
      return true
    }
    currBucket := &cf.hashtable[index]
    displaceIndex := rand.Intn(BUCKET_SIZE)

    // Swap out the old fingerprint
    oldFingerprint := currBucket.fingerprints[displaceIndex]
    currBucket.fingerprints[displaceIndex] = fingerprint

    // Calculate secondary index of the old fingerprint
    _ = firstIndex.SetInt64(int64(index))
    intermediateXor.SetBytes(xor(firstIndex.Bytes(), hash(string(oldFingerprint))))
    newIndex.Mod(intermediateXor, cuckooSize)
    // Apply displace on the old fingerprint
    index = newIndex.Uint64()
    fingerprint = oldFingerprint
    displacements++
  }
  return false
}

// This function calculates the relevant first and second bucket indices of the fingerprint.
func (cf *CuckooFilter) calculateIndices(checksum []byte, fingerprint uint16) (uint64, uint64) {
  checksumNumber, firstIndex, secondIndex, intermediateXor := new(big.Int), new(big.Int), new(big.Int), new(big.Int)
  cuckooSize := big.NewInt(int64(cf.size))

  _ = checksumNumber.SetBytes(checksum)
  _ = firstIndex.Mod(checksumNumber, cuckooSize)
  intermediateXor.SetBytes(xor(firstIndex.Bytes(), hash(string(fingerprint))))
  secondIndex.Mod(intermediateXor, cuckooSize)

  return firstIndex.Uint64(), secondIndex.Uint64()
}

// This function returns a triplet: true or false if the key/fingerprint is in the filter, the bucket index and the index of the fingerprint within the bucket.
func (cf *CuckooFilter) contains(key string) (bool, uint64, int) {

  checksum := hash(key)
  fingerprint := getFingerprint(checksum)
  firstIndex, secondIndex := cf.calculateIndices(checksum, fingerprint)
 
  fingerprintIndex, present := cf.search(firstIndex, fingerprint)
  if present {
    return true, firstIndex, fingerprintIndex
  } else {
    fingerprintIndex, present = cf.search(secondIndex, fingerprint)
    if present {
      return true, secondIndex, fingerprintIndex
    }
  }
  return false, 0, 0
}

// Search bucket for fingerprint. Returns (index, present). If not found, present is false
func (cf *CuckooFilter) search(index uint64, fingerprint uint16) (int, bool){
  currBucket := &cf.hashtable[index]
 
  i := uint8(0)
  for i = 0; i < currBucket.numElements; i++ {
    if currBucket.fingerprints[i] == fingerprint {
      return int(i), true
    }
  }
  return int(i), false
}
// Deletes a fingerprint entry, given the index of the bucket and index of the entry within the bucket.
func (cf *CuckooFilter) deleteEntry(bucketIndex uint64, entryIndex int) {
  currBucket := &cf.hashtable[bucketIndex]
  fingerprints := &currBucket.fingerprints
 
  fingerprints[entryIndex] = fingerprints[currBucket.numElements - 1]
  currBucket.numElements--
}

/************* Public API methods ************/

func (cf *CuckooFilter) Add(key string) bool {
  // TODO: Initialize random generator
  // 8-byte SHA checksum of the key
  if cf.Contains(key) {
    return true
  }
  checksum := hash(key)
  // Calculate the fingerprint: pick highest byte from the checksum
  fingerprint := getFingerprint(checksum)
  // find indices of the fingerprint in the filter
  firstIndex, secondIndex := cf.calculateIndices(checksum, fingerprint)

  // Try to insert at first index
  tryFirst := cf.insert(firstIndex, fingerprint)
  if !tryFirst {
    // try at second
    trySecond := cf.insert(secondIndex, fingerprint)
    if !trySecond {
      // first and second both failed, choose one of the two and force insert and chain displace
      randomIndex := choose(firstIndex, secondIndex)
      // returns false is filter is "TOO FULL"
      return cf.displace(fingerprint, randomIndex)
    }
    return true
  }
  return true
}

func (cf *CuckooFilter) Delete(key string) {
  present, index, fingerprintIndex := cf.contains(key)

  if present {
    cf.deleteEntry(index, fingerprintIndex)
  }
}

func (cf* CuckooFilter) Contains(key string) bool {
  present, _, _ := cf.contains(key)
  return present
}