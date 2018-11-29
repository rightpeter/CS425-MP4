package model

import (
	"CS425/CS425-MP4/bolt"
	"CS425/CS425-MP4/model"
)

// EmitType output type
type EmitType uint8

const (
	// SpoutEmitType spout output type
	SpoutEmitType EmitType = 0
	// BoltEmitType bolt output type
	BoltEmitType EmitType = 1
)

// Msg msg
type Msg struct {
	TupleID int
	Res     string
}

// CraneConfig crane config
type CraneConfig struct {
	IP          string   `json:"ip"`
	Port        int      `json:"port"`
	MasterIP    string   `json:"master_ip"`
	MasterPort  int      `json:"master_port"`
	MemList     []string `json:"member_list"`
	SleepTime   int      `json:"sleep_time"`   // Millisecond
	TaskTimeout int      `json:"task_timeout"` // Millisecond
}

// Tuple tuple
type Tuple struct {
	ID       string
	EmitType EmitType
	Content  string
}

// TaskEmit bolt emit
type TaskEmit struct {
	UUID     string
	ID       string
	EmitType EmitType
	Tuples   []string
}

// CraneTask crane task
type CraneTask struct {
	Tuple    model.Tuple
	Finished bool
	Succeed  bool
}

// BoltTuple bolt tuple
type BoltTuple struct {
	UUID  string
	Tuple string
}

type EmitRules struct {
	// key: id of bolt or sput
	// value is listor map of bolt/spouts that it has subscribed to and goruping type.
	// map from bolt/spout ID to bolt.Grouping
	Subscribed map[string]map[string]bolt.GroupingType
}
