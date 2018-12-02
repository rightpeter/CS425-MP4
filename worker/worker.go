package main

import (
	"CS425/CS425-MP4/bolt"
	outputCollector "CS425/CS425-MP4/collector"
	"CS425/CS425-MP4/model"
	"CS425/CS425-MP4/spout"

	"bufio"
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"os/exec"
	"strings"
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
	w.boltStopChannels = map[string]chan bool{}
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

func (w *Worker) executeSpout(name string, args []string, collector outputCollector.OutputCollector) error {
	log.Printf("executeCMD: name: %v, args: %v", name, args)
	cmd := exec.Command("./bin/"+name, args...)
	cmdReader, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(cmdReader)
	go func() {
		for scanner.Scan() {
			log.Printf("executeCMD: emit tuple %v", scanner.Text())
			if scanner.Text() == "#" {
				continue
			}

			err = collector.Emit([]string{scanner.Text()})
			if err != nil {
				log.Printf("executeCMD: collector.Emit fail: %v", err)
			}
		}
	}()
	err = cmd.Start()
	if err != nil {
		return err
	}

	for {
		time.Sleep(500 * time.Millisecond)
	}
	//err = cmd.Wait()
	//if err != nil {
	//return err
	//}

	//return nil
}

func (w *Worker) executeCMD(name string, args []string) ([]string, error) {
	log.Printf("executeCMD: name: %v, args: %v", name, args)
	cmd := exec.Command("./bin/"+name, args...)

	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		log.Fatal(err)
	}

	emitList := []string{}
	for {
		line, err := out.ReadBytes('\n')
		if err != nil {
			break
		}
		text := strings.TrimSuffix(string(line), "\n")
		log.Printf("executeCMD: emit tuple %v", text)
		emitList = append(emitList, text)
	}

	return emitList, nil
}

func (w *Worker) joinGroup() error {
	log.Printf("master_id: %s", w.getMasterIP())
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", w.getMasterIP(), w.getMasterPort()))
	if err != nil {
		return err
	}

	w.client = client

	var reply bool

	err = w.client.Call("Master.RPCJoinGroup", w.getIP(), &reply)
	if err != nil {
		return err
	}
	return nil
}

// RPCMasterPing rpc master ping
func (w *Worker) RPCMasterPing(ip string, reply *bool) error {
	// log.Printf("Pinged by master %v", ip)
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
func (w *Worker) RPCPrepareSpout(theSpout spout.Spout, reply *string) error {
	var err error
	log.Printf("RPCPrepareSpout: %v", theSpout.ID)
	collector := outputCollector.NewOutputCollector(theSpout.ID, "", model.SpoutEmitType, w.client)
	//go spout.Spout.Activate(collector)
	go func() {
		err := w.executeSpout(theSpout.Activate.Name, theSpout.Activate.Args, collector)
		if err != nil {
			log.Printf("RPCPrepareSpout: executeCMD error: %v", err)
		}
	}()

	if err != nil {
		return err
	}
	return nil
}

// RPCPrepareBolt rpc prepare bolt
func (w *Worker) RPCPrepareBolt(theBolt bolt.Bolt, reply *string) error {
	var err error
	log.Printf("RPCPrepareBolt: %v", theBolt.ID)

	if _, ok := w.boltChannels[theBolt.ID]; ok {
		return fmt.Errorf("bolt ID %s has been registered", theBolt.ID)
	}

	w.boltChannels[theBolt.ID] = make(chan model.BoltTuple)
	log.Printf("RPCPrepareBolt: prepare channel for bolt %v", theBolt.ID)
	w.boltStopChannels[theBolt.ID] = make(chan bool)

	go func() {
		for {
			select {
			case task := <-w.boltChannels[theBolt.ID]:
				log.Printf("bolt %v received task %v", theBolt.ID, task.UUID)
				collector := outputCollector.NewOutputCollector(theBolt.ID, task.UUID, model.BoltEmitType, w.client)
				// bolt.Bolt.Execute(task, collector)
				go func() {
					var args []string
					if len(theBolt.Execute.Args) == 0 {
						args = []string{task.Tuple.Content}
					} else {
						args = append(theBolt.Execute.Args, task.Tuple.Content)
					}
					emitList, err := w.executeCMD(theBolt.Execute.Name, args)
					if err != nil {
						log.Printf("RPCPrepareBolt: executeCMD failed: %v", err)
					}
					log.Printf("RPCPrepareBolt: emitList for bolt %v: %v", theBolt.ID, emitList)

					err = collector.Emit(emitList)
					if err != nil {
						log.Printf("executeCMD: collector.Emit fail: %v", err)
					}

					if len(theBolt.Finish.Args) == 0 {
						args = emitList
					} else {
						args = append(theBolt.Finish.Args, emitList...)
					}
					if theBolt.Finish.Name != "" {
						w.executeCMD(theBolt.Finish.Name, args)
					}
				}()
			case <-w.boltStopChannels[theBolt.ID]:
				break
			}
		}
	}()
	if err != nil {
		return err
	}
	return nil
}

// RPCExecuteTask rpc execute task
func (w *Worker) RPCExecuteTask(task model.BoltTuple, reply *bool) error {
	// log.Printf("RPCExecuteTask: task: %v", task.UUID)
	if _, ok := w.boltChannels[task.ID]; !ok {
		return fmt.Errorf("no channel for bolt: %v", task.ID)
	}
	log.Printf("add task %v to bolt %s's channel", task.UUID, task.ID)
	w.boltChannels[task.ID] <- task
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

	go func() {
		for {
			time.Sleep(1 * time.Second)
			err = w.joinGroup()
			if err != nil {
				log.Printf("join group failed, retry 5 sec later: %s", err)
				time.Sleep(5 * time.Second)
			} else {
				break
			}
		}
	}()

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
