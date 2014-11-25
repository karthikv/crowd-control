package cuckoofilter
import (
  "testing"
  "fmt"
  "strconv"
)
func TestCuckooFilter(*testing.T) {
  cf, _ := Make(8)
  for i := 0; i < 1000; i++ {
    s := "test" + strconv.Itoa(i)
    _ = cf.Add(s)
  }
  result := cf.Contains("test")
  fmt.Println(result)
  cf.Delete("test")
  result = cf.Contains("test")
  fmt.Println(result)
}