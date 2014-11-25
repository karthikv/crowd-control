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
  cf, _ := Make(1000)
  var result bool
  for i := 0; i < 700; i++ {
    s := "test" + strconv.Itoa(i)
    result = cf.Add(s)
    if !result {
      fmt.Println("Add failed. This isn't good.")
    }
  }
  for j := 0; j < 700; i++ {
    s := "test" + strconv.Itoa(i)
    result = cf.Contains("test" + strconv.Itoa(i))
    if !result {
      fmt.Println("Contains failed. This is bad.")
    }
  }
}

func TestAdvanced(*testing.T) {

}