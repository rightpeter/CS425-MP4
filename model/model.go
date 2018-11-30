package model

// SIZE md5 size
const SIZE = 16

// EmitType output type
type EmitType uint8

const (
	// SpoutEmitType spout output type
	SpoutEmitType EmitType = 0
	// BoltEmitType bolt output type
	BoltEmitType EmitType = 1
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
	Tuple    Tuple
	Finished bool
	Succeed  bool
}

// BoltTuple bolt tuple
type BoltTuple struct {
	UUID  string
	Tuple string
}

// FileVersion file version
type FileVersion struct {
	// nodes with that version
	Version int
	Nodes   []string
	Hash    [SIZE]byte
}

// FileStructure file structure
type FileStructure struct {
	Version  int
	Filename string
	Hash     [SIZE]byte
}

// GlobalIndexFile global index file
type GlobalIndexFile struct {
	// map from filename->latest md5 hash
	Filename map[string]FileStructure
	// map from Filename to different file versions
	Fileversions map[string][]FileVersion
	// map from node ID to list of files on the node
	NodesToFile map[string][]FileStructure
	// map from filename to list of nodes with the file
	FileToNodes map[string][]string
}

// PullInstruction pull instruction
type PullInstruction struct {
	Filename string
	Node     string
	PullFrom []string // IDs with file
}

type RemoveNodeReturn struct {
	IP         string
	DataOnNode []FileStructure
}
