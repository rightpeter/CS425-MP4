package bolt

import (
	"CS425/CS425-MP4/collector"
	"CS425/CS425-MP4/model"
)

// GroupingType grouping type
type GroupingType uint8

const (
	// ShuffleGroupingType shuffle grouping type
	ShuffleGroupingType GroupingType = 0
	// AllGroupingType all grouping type
	AllGroupingType GroupingType = 1
	// FieldsGroupingType fields grouping type
	FieldsGroupingType GroupingType = 2
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
	Grouping map[string]GroupingType
}

// ShuffleGrouping shuffle grouping
func (b Builder) ShuffleGrouping(id string) {
	b.Grouping[id] = ShuffleGroupingType
}

// RPCBolt rpc bolt
type RPCBolt struct {
	ID   string
	Bolt Bolt
}
