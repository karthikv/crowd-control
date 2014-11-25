package cuckoofilter
import (
  "testing"
  "fmt"
)
func TestCuckooFilter(*testing.T) {
  cf, _ := Make(8)
  cf.Add("shubham")
  result := cf.Contains("shubham")
  fmt.Println(result)
}