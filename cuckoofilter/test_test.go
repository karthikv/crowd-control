package cuckoofilter
import (
  "testing"
  "fmt"
  "strconv"
)
func TestBasic(*testing.T) {
  cf, _ := Make(8)
  _ = cf.Add("test")
  result := cf.Contains("test")
  fmt.Println(result)
  cf.Delete("test")
  result = cf.Contains("test")
  fmt.Println(result)
}

func TestIntermediate(*testing.T) {
  cf, _ := Make(64)
  var result bool
  for i := 0; i < 200; i++ {
    s := "test" + strconv.Itoa(i)
    result = cf.Add(s)
    if !result {
      fmt.Println("Add failed. This isn't good.", s)
      break
    }
  }
  for j := 0; j < 200; j++ {
    s := "test" + strconv.Itoa(j)
    result = cf.Contains(s)
    if !result {
      fmt.Println("Contains failed. This is bad. ", s)
    }
  }
}

func TestAdvanced(*testing.T) {

}