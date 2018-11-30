package main

import (
	"CS425/CS425-MP4/bolt"
	outputCollector "CS425/CS425-MP4/collector"
	"CS425/CS425-MP4/model"
	"CS425/CS425-MP4/spout"
	"encoding/json"
	"errors"
	"fmt"
	"net/rpc"
)

// Worker worker
type Worker struct {
	boltChannels     map[string]chan model.CraneTask
	boltStopChannels map[string]chan bool
	config           model.CraneConfig
	client           *rpc.Client
}

// NewWorker init a worker
func NewWorker(workerConfig []byte) Worker {
	worker := Worker{}
	worker.init(workerConfig)
	return worker
}

func (w Worker) init(workerConfig []byte) {
	json.Unmarshal(workerConfig, &w.config)
	w.boltChannels = map[string]chan string{}
}

// RPCMasterPing rpc master ping
func (w *Worker) RPCMasterPing(ip string, reply *bool) error {
	// TODO
	// ip is the ip of the master
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", ip, w.config.Port))
	if err != nil {
		return err
	}
	w.client = client
	return nil
}

// RPCPrepareSpout rpc prepare spout
func (w *Worker) RPCPrepareSpout(spout spout.Spout, reply *string) error {
	collector := outputCollector.NewOutputCollector(outputCollector.SpoutOutputType, w.client)
	go spout.Activate(collector)
	return nil
}

// RPCPrepareBolt rpc prepare bolt
func (w *Worker) RPCPrepareBolt(bolt bolt.Builder, reply *string) error {

	if _, ok := w.boltChannels[bolt.ID]; !ok {
		return errors.New("bolt id conflicts")
	}

	w.boltChannels[bolt.ID] = make(chan model.CraneTask)
	w.boltStopChannels[bolt.ID] = make(chan bool)

	go func() {
		for {
			select {
			case task := <-w.boltChannels[bolt.ID]:
				collector := outputCollector.NewOutputCollector(bolt.ID, task.UUID, outputCollector.BoltOutputType, w.client)
				bolt.Bolt.Execute(model.BoltTuple, collector)
			case <-w.boltStopChannels[bolt.ID]:
				break
			}
		}
	}()

	return nil
}
