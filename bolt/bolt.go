package bolt

import (
	"CS425/CS425-MP4/collector"
	"CS425/CS425-MP4/model"
)

// Bolt bolt
type Bolt interface {
	Execute(tuple model.BoltTuple, collector collector.OutputCollector)
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

// RPCBolt rpc bolt
type RPCBolt struct {
	ID   string
	Bolt Bolt
}
