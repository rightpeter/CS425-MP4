package collector

import (
	"CS425/CS425-MP4/model"
	"net/rpc"
)

// NewOutputCollector new spout output collector
func NewOutputCollector(id string, uuid string, emitType model.EmitType, client *rpc.Client) OutputCollector {
	collector := OutputCollector{}
	collector.id = id
	collector.uuid = uuid
	collector.emitType = emitType
	collector.client = client

	return collector
}

// OutputCollector output collector
type OutputCollector struct {
	id       string
	uuid     string
	emitType model.EmitType
	client   *rpc.Client
}

func (o OutputCollector) emit(tuples []string) error {
	emitTuples := model.TaskEmit{}
	emitTuples.ID = o.id
	emitTuples.UUID = o.uuid
	emitTuples.EmitType = o.emitType
	emitTuples.Tuples = tuples

	err := o.client.Call("Master.RPCEmit", emitTuples, nil)
	if err != nil {
		return err
	}

	return nil
}

// Emit bolt emit
func (o OutputCollector) Emit(tuples []string) error {
	err := o.emit(tuples)

	if err != nil {
		return err
	}
	return nil
}
