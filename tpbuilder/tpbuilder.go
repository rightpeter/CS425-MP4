package tpbuilder

import (
	"CS425/CS425-MP4/bolt"
	"CS425/CS425-MP4/model"
	"CS425/CS425-MP4/spout"
	"fmt"
	"log"
	"net/rpc"
)

// Builder builder struct
type Builder struct {
	ID    string
	Spout map[string]spout.Builder
	Bolt  map[string]bolt.Builder
}

// TpBuilder topology builder
type TpBuilder struct {
	ip      string
	port    int
	builder Builder
}

// NewTpBuilder new tpBuilder
func NewTpBuilder(ip string, port int) *TpBuilder {
	tpBuilder := &TpBuilder{}
	tpBuilder.ip = ip
	tpBuilder.port = port
	builder := Builder{}
	builder.Spout = map[string]spout.Builder{}
	builder.Bolt = map[string]bolt.Builder{}

	tpBuilder.builder = builder

	return tpBuilder
}

// SetSpout set spout
func (t *TpBuilder) SetSpout(id string, sp spout.Spout, parallel int) spout.Builder {
	spoutBuilder := spout.Builder{Spout: sp, Parallel: parallel}
	t.builder.Spout[id] = spoutBuilder
	return spoutBuilder
}

// SetBolt set bolt
func (t *TpBuilder) SetBolt(id string, bt bolt.Bolt, parallel int) bolt.Builder {
	boltBuilder := bolt.Builder{ID: id, Bolt: bt, Parallel: parallel, Grouping: map[string]model.GroupingType{}}
	t.builder.Bolt[id] = boltBuilder
	return boltBuilder
}

func (t *TpBuilder) callSubmitRPC(client *rpc.Client, streamID string) error {
	t.builder.ID = streamID
	err := client.Call("Master.RPCSubmitStream", &t.builder, nil)
	if err != nil {
		return err
	}
	return nil
}

// Submit submit
func (t *TpBuilder) Submit(streamID string) error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", t.ip, t.port))
	if err != nil {
		log.Fatal("dialing:", err)
	}

	err = t.callSubmitRPC(client, streamID)
	if err != nil {
		return err
	}

	return nil
}
