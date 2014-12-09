package op_log


type Operation interface {
  // perform operation on the given bloom filters
  Perform(filters *[]*Filter)
}


type AddOperation struct {
  Key string
}


/* Adds a key to the filter of each node. */
func (add AddOperation) Perform(filters *[]*Filter) {
  for _, filter := range *filters {
    filter.Add([]byte(add.Key))
  }
}


type RemoveOperation struct {
  Key string
  Nodes []int
}


/* Removes a key from the filter of the given nodes. */
func (remove RemoveOperation) Perform(filters *[]*Filter) {
  for _, node := range remove.Nodes {
    (*filters)[node].Remove([]byte(remove.Key))
  }
}


type SetFilterOperation struct {
  Filters []Filter
  Nodes []int
}


/* Sets the filters of the given nodes. */
func (setFilter SetFilterOperation) Perform(filters *[]*Filter) {
  for i, node := range setFilter.Nodes {
    (*filters)[node] = &setFilter.Filters[i]
  }
}
