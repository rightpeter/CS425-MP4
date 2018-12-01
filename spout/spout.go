package spout

import (
	"CS425/CS425-MP4/model"
)

// Spout spout
type Spout struct {
	ID       string
	Activate model.CMD
}

// Builder spout builder
type Builder struct {
	Spout    Spout
	Parallel int
}
