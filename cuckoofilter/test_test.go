package cuckoofilter
import (
  "testing"
  "strconv"
  "log"
  "math/rand"
  "time"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randSeq(n int) string {
    b := make([]rune, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

func TestBasic(*testing.T) {
  cf, _ := Make(8)
  _ = cf.Add("test")
  result := cf.Contains("test")
  log.Printf("%v",result)
  cf.Delete("test")
  result = cf.Contains("test")
  log.Printf("%v",result)
}

func TestAddAndContainsSerialStrings(*testing.T) {
  cf, _ := Make(64) // 64 buckets
  var result bool
  var lastAdded int
  evictedKey := ""
  for i := 0; i < 250; i++ {
    s := "test" + strconv.Itoa(i)
    result = cf.Add(s)
    if !result {
      log.Printf("Last added value was %v", i - 1)
      lastAdded = i - 1
      break
    }
  }
  for j := 0; j <= lastAdded; j++ {
    s := "test" + strconv.Itoa(j)
    result = cf.Contains(s)
    if !result {
      if evictedKey != "" {
        log.Fatalf("Filter contains element; does not recognize it\n")
      }
      evictedKey = s
    }
  }
  checkFalseContains(cf)
}

func checkFalseContains(cf *CuckooFilter) {
  for j := 0; j < 1000; j++ {
    s := strconv.Itoa(j)
    if cf.Contains(s) {
      log.Printf("false positives! filter returning contains true when it does not contain element")
    }
  }
}

func TestAddAndContainsRandomStrings(*testing.T) {
  log.Printf("TestAddAndContainsRandomStrings: begin()")
  cf, _ := Make(64) // 64 buckets
  var result bool
  rand.Seed(time.Now().Unix())
  addedKeys := make(map[string]bool)
  evictedKey := ""
  for i := 0; i < 250; i++ {
    s := randSeq(rand.Intn(10))
    result = cf.Add(s)
    if !result {
      log.Printf("Last added value was %v", i - 1)
      break
    }
    addedKeys[s] = true
  }
  for key, _ := range addedKeys {
    result = cf.Contains(key)
    if !result {
      if evictedKey != "" {
        log.Fatalf("Filter contains element; does not recognize it\n")
      }
      evictedKey = key
    }
  }
  checkFalseContains(cf)
}
func TestDelete(*testing.T) {
  log.Printf("TestDelete: begin()")
  cf, _ := Make(64)
  var lastAdded int
  evictedKey := ""
  for i := 0; i < 250; i++ {
    s := "test" + strconv.Itoa(i)
    if !cf.Add(s) {
      log.Printf("Last added value was %v", i - 1)
      lastAdded = i - 1
      break
    }
  }
  for j := 0; j <= lastAdded; j++ {
    s := "test" + strconv.Itoa(j)
    if !cf.Contains(s) {
        if (evictedKey != "") {
          log.Fatalf("Filter contains element; does not recognize it\n")
        }
      evictedKey = s
    }
  }

  for j := 0; j <= lastAdded; j += 2 {
    s := "test" + strconv.Itoa(j)
    cf.Delete(s)
  }

  for j := 0; j <= lastAdded; j++ {
    s := "test" + strconv.Itoa(j)
    if j % 2 == 0 {
      if cf.Contains(s) {
        log.Fatalf("key deleted but contains still returns true")
      }
    } else {
      if !cf.Contains(s) && s != evictedKey {
        log.Fatalf("key still there but contains returns false. Key: %v", s)
      }
    }
  }
}