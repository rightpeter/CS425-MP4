package spout

import (
	"CS425/CS425-MP4/collector"
)

// Spout spout
type Spout interface {
	Activate(collector collector.OutputCollector)
	Deactive()
}

// Builder spout builder
type Builder struct {
	Spout    Spout
	Parallel int
}
