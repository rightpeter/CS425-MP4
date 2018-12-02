package bolt

import (
	"CS425/CS425-MP4/model"
)

// Bolt bolt
type Bolt struct {
	ID      string
	Execute model.CMD
	Finish  model.CMD
}

// Builder bolt struct
type Builder struct {
	ID       string
	Bolt     Bolt
	Parallel int
	Grouping map[string]model.GroupingType
}

// ShuffleGrouping shuffle grouping
func (b Builder) ShuffleGrouping(id string) {
	b.Grouping[id] = model.ShuffleGroupingType
}
