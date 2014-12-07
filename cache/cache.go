package cc

import (
  "container/list"
)


/* Represents a key-value pair stored in the cache. */
type KV struct {
  key string
  value string
}

/* Represents an LRU cache. */
type Cache struct {
  accessList *list.List
  data map[string]*list.Element

  // in bytes
  size uint64
  capacity uint64
}


/* Associates key with value. Evicts items from the cache if necessary. */
func (cache *Cache) Set(key string, value string) {
  element, exists := cache.data[key]

  if exists {
    // change value
    kv := element.Value.(*KV)
    cache.size = cache.size - uint64(len(kv.value)) + uint64(len(value))

    kv.value = value
    cache.accessList.MoveToFront(element)
  } else {
    // insert new item
    cache.size = cache.size + uint64(len(key)) + uint64(len(value))
    kv := &KV{key: key, value: value}

    element = cache.accessList.PushFront(kv)
    cache.data[key] = element
  }

  cache.makeSpace()
}


/* Makes space in the cache by evicting items. */
func (cache *Cache) makeSpace() {
  for cache.size > cache.capacity {
    // evict items that haven't been accessed recently
    element := cache.accessList.Back()
    cache.Delete(element.Value.(*KV).key)
  }
}


/* Retrieves the value associated with key. Returns (value, exists). */
func (cache *Cache) Get(key string) (string, bool) {
  element, exists := cache.data[key]

  if exists {
    // update LRU list
    cache.accessList.MoveToFront(element)
    return element.Value.(*KV).value, true
  } else {
    return "", false
  }
}


/* Deletes the value associated with key. */
func (cache *Cache) Delete(key string) {
  element, exists := cache.data[key]

  if exists {
    cache.accessList.Remove(element)
    kv := element.Value.(*KV)

    cache.size = cache.size - uint64(len(kv.key)) - uint64(len(kv.value))
    delete(cache.data, key)
  }
}


/* Initializes a cache for use. capacity should be in bytes */
func (cache *Cache) Init(capacity uint64) {
  cache.accessList = list.New()
  cache.data = make(map[string]*list.Element)
  cache.size = 0
  cache.capacity = capacity
}
