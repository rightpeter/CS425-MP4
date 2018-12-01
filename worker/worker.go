package main

import (
	"CS425/CS425-MP4/bolt"
	outputCollector "CS425/CS425-MP4/collector"
	"CS425/CS425-MP4/model"
	"CS425/CS425-MP4/spout"

	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

// Worker worker
type Worker struct {
	boltChannels     map[string]chan model.BoltTuple
	boltStopChannels map[string]chan bool
	config           model.CraneConfig
	client           *rpc.Client
	masterIP         string
}

// NewWorker init a worker
func NewWorker(workerConfig []byte) *Worker {
	worker := &Worker{}
	worker.init(workerConfig)
	return worker
}

func (w *Worker) init(workerConfig []byte) {
	w.masterIP = "127.0.0.1"
	json.Unmarshal(workerConfig, &w.config)
	w.boltChannels = map[string]chan model.BoltTuple{}
}

func (w *Worker) getLogPath() string {
	return w.config.LogPath
}

func (w *Worker) getIP() string {
	return w.config.IP
}

func (w *Worker) getPort() int {
	return w.config.Port
}

func (w *Worker) getMasterIP() string {
	return w.config.MasterIP
}

func (w *Worker) getMasterPort() int {
	return w.config.MasterPort
}

func (w *Worker) joinGroup() error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", w.getMasterIP(), w.config.Port))
	if err != nil {
		return err
	}

	w.client = client

	var reply bool

	err = w.client.Call("Master.RPCJoinGroup", w.getIP(), reply)
	if err != nil {
		return err
	}
	return nil
}

// RPCMasterPing rpc master ping
func (w *Worker) RPCMasterPing(ip string, reply *bool) error {
	if ip != w.masterIP {
		log.Printf("new master %v\n", ip)
		w.masterIP = ip
		client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", ip, w.config.Port))
		if err != nil {
			return err
		}
		w.client = client
	}
	return nil
}

// RPCPrepareSpout rpc prepare spout
func (w *Worker) RPCPrepareSpout(spout spout.RPCSpout, reply *string) error {
	collector := outputCollector.NewOutputCollector(spout.ID, "", model.SpoutEmitType, w.client)
	go spout.Spout.Activate(collector)
	return nil
}

// RPCPrepareBolt rpc prepare bolt
func (w *Worker) RPCPrepareBolt(bolt bolt.RPCBolt, reply *string) error {

	if _, ok := w.boltChannels[bolt.ID]; !ok {
		return errors.New("bolt id conflicts")
	}

	w.boltChannels[bolt.ID] = make(chan model.BoltTuple)
	w.boltStopChannels[bolt.ID] = make(chan bool)

	go func() {
		for {
			select {
			case task := <-w.boltChannels[bolt.ID]:
				collector := outputCollector.NewOutputCollector(bolt.ID, task.UUID, model.BoltEmitType, w.client)
				bolt.Bolt.Execute(task, collector)
			case <-w.boltStopChannels[bolt.ID]:
				break
			}
		}
	}()

	return nil
}

func main() {
	// parse argument
	configFilePath := flag.String("c", "./config.json", "Config file path")

	// load config file
	configFile, err := ioutil.ReadFile(*configFilePath)
	if err != nil {
		log.Fatalf("File error: %v\n", err)
	}

	// Class for server
	w := NewWorker(configFile)

	f, err := os.OpenFile(w.getLogPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	for {
		err = w.joinGroup()
		if err != nil {
			log.Printf("join group failed, retry 5 sec later: %s", err)
			time.Sleep(5 * time.Second)
		} else {
			break
		}
	}

	// init the rpc server
	rpc.Register(w)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", w.getPort()))
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	log.Printf("Start listen rpc on port: %d", w.getPort())
	http.Serve(l, nil)
}
