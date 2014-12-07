package op_log

import (
  "testing"
  "time"
  "math"
  "math/rand"
  "encoding/binary"
)


func checkSize(t *testing.T, filter *Filter, size int) {
  if filter.Size() != size {
    t.Fatalf("Got size %v, expected %v\n", filter.Size(), size)
  }
}

func checkCount(t *testing.T, filter *Filter, key string, count uint8) {
  counterIndexes := filter.getCounterIndexes([]byte(key))
  for _, index := range counterIndexes {
    gotCount := filter.get(index)
    if gotCount < count {
      t.Fatalf("Got count %v, expected >= %v\n", gotCount, count)
    }
  }
}

func TestAdd(t *testing.T) {
  filter := &Filter{}
  capacity := 256
  filter.Init(capacity, 6)

  if filter.Capacity() != capacity {
    t.Fatalf("Got capacity %v, expected %v\n", filter.Capacity(), capacity)
  }
  checkSize(t, filter, 0)

  filter.Add([]byte("foo"))
  checkSize(t, filter, 1)
  checkCount(t, filter, "foo", 1)

  filter.Add([]byte("bar"))
  checkSize(t, filter, 2)
  checkCount(t, filter, "bar", 1)

  filter.Add([]byte("baz"))
  checkSize(t, filter, 3)
  checkCount(t, filter, "baz", 1)

  filter.Add([]byte("foo"))
  checkSize(t, filter, 4)
  checkCount(t, filter, "foo", 2)
}

func checkContains(t *testing.T, filter *Filter, key string, contains bool) {
  if filter.Contains([]byte(key)) != contains {
    t.Fatalf("For key %v, expected contains %v\n", key, contains)
  }
}

func TestContains(t *testing.T) {
  filter := &Filter{}
  filter.Init(256, 6)

  checkContains(t, filter, "foo", false)
  checkContains(t, filter, "bar", false)
  checkContains(t, filter, "baz", false)

  filter.Add([]byte("foo"))

  checkContains(t, filter, "foo", true)
  checkContains(t, filter, "bar", false)
  checkContains(t, filter, "baz", false)

  filter.Add([]byte("bar"))

  checkContains(t, filter, "foo", true)
  checkContains(t, filter, "bar", true)
  checkContains(t, filter, "baz", false)

  filter.Add([]byte("baz"))

  checkContains(t, filter, "foo", true)
  checkContains(t, filter, "bar", true)
  checkContains(t, filter, "baz", true)

  filter.Add([]byte("foo"))

  checkContains(t, filter, "foo", true)
  checkContains(t, filter, "bar", true)
  checkContains(t, filter, "baz", true)
}

func TestRemove(t *testing.T) {
  filter := &Filter{}
  filter.Init(256, 6)

  checkSize(t, filter, 0)
  filter.Add([]byte("foo"))
  filter.Add([]byte("bar"))
  filter.Add([]byte("baz"))

  filter.Remove([]byte("foo"))
  checkContains(t, filter, "foo", false)
  checkSize(t, filter, 2)
  checkCount(t, filter, "foo", 0)

  // shouldn't remove anything
  filter.Remove([]byte("foo"))
  checkContains(t, filter, "foo", false)
  checkSize(t, filter, 2)
  checkCount(t, filter, "foo", 0)

  // add second bar
  filter.Add([]byte("bar"))
  checkContains(t, filter, "bar", true)
  checkSize(t, filter, 3)
  checkCount(t, filter, "bar", 2)

  filter.Remove([]byte("bar"))
  checkContains(t, filter, "bar", true)
  checkSize(t, filter, 2)
  checkCount(t, filter, "bar", 1)

  filter.Remove([]byte("bar"))
  checkContains(t, filter, "bar", false)
  checkSize(t, filter, 1)
  checkCount(t, filter, "bar", 0)
}

func TestSaturation(t *testing.T) {
  filter := &Filter{}
  filter.Init(256, 6)

  for i := 0; i < 15; i++ {
    filter.Add([]byte("foo"))
  }

  checkContains(t, filter, "foo", true)
  checkCount(t, filter, "foo", 15)
  checkSize(t, filter, 15)

  filter.Add([]byte("foo"))

  checkContains(t, filter, "foo", true)
  checkCount(t, filter, "foo", 15)  // saturated count; shouldn't change
  checkSize(t, filter, 16)  // size should still be modified

  for i := 0; i < 15; i++ {
    filter.Remove([]byte("foo"))
  }

  checkContains(t, filter, "foo", false)
  checkCount(t, filter, "foo", 0)
  checkSize(t, filter, 1)
}

func TestFalsePositiveRate(t *testing.T) {
  countersPerElementFloat := float64(COUNTERS_PER_ELEMENT)
  rand.Seed(time.Now().UnixNano())

  for i := 0; i < 100; i++ {
    capacity := 100 + rand.Intn(2000)
    numHashes := 1 + rand.Intn(20)

    filter := &Filter{}
    filter.Init(capacity, numHashes)

    key := make([]byte, 8)
    for i := 0; i < capacity; i++ {
      binary.BigEndian.PutUint64(key, uint64(rand.Int63()))
      filter.Add(key)
    }

    numHashesFloat := float64(numHashes)
    probFalsePositive := math.Pow(1.0 - math.Exp(-numHashesFloat /
      countersPerElementFloat), numHashesFloat)

    numTrials := 10000
    numFalsePositives := 0

    for i := 0; i < numTrials; i++ {
      binary.BigEndian.PutUint64(key, uint64(rand.Int63()))
      if filter.Contains(key) {
        numFalsePositives += 1
      }
    }

    expectedFalsePositives := int(probFalsePositive * float64(numTrials))
    if float64(numFalsePositives - expectedFalsePositives) /
        float64(expectedFalsePositives) > 0.1 {
      t.Fatalf("Many more false positives than expected: got %v, expected %v\n",
        numFalsePositives, expectedFalsePositives)
    }

    t.Logf("For cap = %4v, k = %2v, got %4v fp, expected %4v\n", capacity,
      numHashes, numFalsePositives, expectedFalsePositives)
  }
}
