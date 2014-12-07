package cc

import (
  "testing"
)

func checkLenSize(t *testing.T, cache *Cache, length int, size uint64) {
  if cache.size != size {
    t.Fatalf("Invalid cache size: got %v, expected %v\n", cache.size, size)
  }

  if len(cache.data) != length {
    t.Fatalf("Invalid cache length: got %v, expected %v\n", len(cache.data), length)
  }
}

func TestSet(t *testing.T) {
  cache := &Cache{}
  cache.Init(256)

  checkLenSize(t, cache, 0, 0)

  cache.Set("foo", "bar")
  checkLenSize(t, cache, 1, 6)

  cache.Set("john", "doe")
  checkLenSize(t, cache, 2, 13)

  cache.Set("foo", "baz")
  checkLenSize(t, cache, 2, 13)

  cache.Set("john", "jacob")
  checkLenSize(t, cache, 2, 15)
}

func checkGet(t *testing.T, cache *Cache, key string, value string, exists bool) {
  gotValue, gotExists := cache.Get(key)

  if gotValue != value {
    t.Fatalf("Get value incorrect: got %v, expected %v\n", gotValue, value)
  }

  if gotExists != exists {
    t.Fatalf("Get exists incorrect: got %v, expected %v\n", gotExists, exists)
  }
}

func TestGet(t *testing.T) {
  cache := &Cache{}
  cache.Init(256)

  cache.Set("foo", "bar")
  checkGet(t, cache, "foo", "bar", true)

  cache.Set("john", "doe")
  checkGet(t, cache, "john", "doe", true)

  checkGet(t, cache, "baz", "", false)
  checkGet(t, cache, "james", "", false)

  cache.Set("foo", "baz")
  checkGet(t, cache, "foo", "baz", true)

  cache.Set("john", "jacob")
  checkGet(t, cache, "john", "jacob", true)
}

func TestDelete(t *testing.T) {
  cache := &Cache{}
  cache.Init(256)

  cache.Set("foo", "bar")
  checkGet(t, cache, "foo", "bar", true)

  cache.Set("john", "doe")
  checkGet(t, cache, "john", "doe", true)

  checkGet(t, cache, "baz", "", false)
  checkGet(t, cache, "james", "", false)

  cache.Delete("foo")
  checkGet(t, cache, "foo", "", false)
  checkLenSize(t, cache, 1, 7)

  cache.Delete("baz")
  checkGet(t, cache, "baz", "", false)
  checkLenSize(t, cache, 1, 7)

  cache.Delete("john")
  checkGet(t, cache, "john", "", false)
  checkLenSize(t, cache, 0, 0)
}

func TestLRU(t *testing.T) {
  cache := &Cache{}
  cache.Init(30)

  cache.Set("foo", "bar")
  cache.Set("baz", "boo")
  cache.Set("john", "doe")
  cache.Set("james", "jacob")

  // total size = 29; everything should be available
  checkLenSize(t, cache, 4, 29)
  checkGet(t, cache, "foo", "bar", true)
  checkGet(t, cache, "baz", "boo", true)
  checkGet(t, cache, "john", "doe", true)
  checkGet(t, cache, "james", "jacob", true)

  cache.Set("baz", "boom")

  // total size = 30; everything should be available
  checkLenSize(t, cache, 4, 30)
  checkGet(t, cache, "foo", "bar", true)
  checkGet(t, cache, "baz", "boom", true)
  checkGet(t, cache, "john", "doe", true)
  checkGet(t, cache, "james", "jacob", true)

  cache.Set("too", "boo")

  // foo should have been evicted
  checkLenSize(t, cache, 4, 30)
  checkGet(t, cache, "foo", "", false)
  checkGet(t, cache, "too", "boo", true)

  checkGet(t, cache, "baz", "boom", true)
  cache.Set("bob", "lin")

  // john should have been evicted (baz recently accessed)
  checkLenSize(t, cache, 4, 29)
  checkGet(t, cache, "john", "", false)
  checkGet(t, cache, "bob", "lin", true)

  cache.Set("large key", "large value")

  // james, too, and baz evicted to make room
  checkLenSize(t, cache, 2, 26)
  checkGet(t, cache, "james", "", false)
  checkGet(t, cache, "too", "", false)
  checkGet(t, cache, "baz", "", false)
  checkGet(t, cache, "bob", "lin", true)
  checkGet(t, cache, "large key", "large value", true)

  // set key/value pair that takes up entire cache space
  cache.Set("something very large that can't fit", "extremeley long key")

  // nothing should be in the cache
  checkLenSize(t, cache, 0, 0)
  checkGet(t, cache, "foo", "", false)
  checkGet(t, cache, "baz", "", false)
  checkGet(t, cache, "john", "", false)
  checkGet(t, cache, "james", "", false)
  checkGet(t, cache, "too", "", false)
  checkGet(t, cache, "bob", "", false)
  checkGet(t, cache, "large key", "", false)
  checkGet(t, cache, "large key", "", false)
  checkGet(t, cache, "something very large that can't fit", "", false)
}
