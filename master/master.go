package main

import (
	"CS425/CS425-MP4/bolt"
	"CS425/CS425-MP4/index"
	"CS425/CS425-MP4/model"
	"CS425/CS425-MP4/spout"
	"CS425/CS425-MP4/tpbuilder"

	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

// Master master
type Master struct {
	config               model.CraneConfig
	nodesRPCClientsMutex *sync.Mutex
	nodesRPCClients      map[string]*rpc.Client
	memListMutex         *sync.Mutex
	memList              map[string]bool
	taskMapMutex         *sync.Mutex
	taskMap              map[string]*model.CraneTask
	streamBuilders       map[string]*tpbuilder.Builder
	spoutBuilders        map[string]spout.Builder
	boltBuilders         map[string]bolt.Builder
	emitRules            map[string]map[string]model.GroupingType
	spoutIndex           index.Index
	boltIndex            index.Index
}

// NewMaster init a master
func NewMaster(masterConfig []byte) *Master {
	master := &Master{}
	master.init(masterConfig)

	return master
}

func (m *Master) loadConfigFromJSON(jsonFile []byte) error {
	return json.Unmarshal(jsonFile, &m.config)
}

func (m *Master) init(masterConfig []byte) {
	json.Unmarshal(masterConfig, &m.config)
	m.taskMapMutex = &sync.Mutex{}
	m.taskMap = map[string]*model.CraneTask{}
	m.streamBuilders = map[string]*tpbuilder.Builder{}
	m.nodesRPCClientsMutex = &sync.Mutex{}
	m.nodesRPCClients = map[string]*rpc.Client{}
	m.memListMutex = &sync.Mutex{}
	m.memList = map[string]bool{}
	for _, mem := range m.config.MemList {
		m.memList[mem] = false
	}
	m.emitRules = map[string]map[string]model.GroupingType{}
	m.spoutIndex = index.NewIndex()
	m.boltIndex = index.NewIndex()
}

func (m *Master) getLogPath() string {
	return m.config.LogPath
}

func (m *Master) getIP() string {
	return m.config.IP
}

func (m *Master) getPort() int {
	return m.config.Port
}

func (m *Master) getContentFromUUID(uuid string) string {
	if _, ok := m.taskMap[uuid]; !ok {
		return ""
	}
	return m.taskMap[uuid].Tuple.Content
}

func (m *Master) addRPCClient(ip string, client *rpc.Client) {
	if _, ok := m.nodesRPCClients[ip]; !ok {
		m.nodesRPCClientsMutex.Lock()
		m.nodesRPCClients[ip] = client
		m.nodesRPCClientsMutex.Unlock()
	}
}

func (m *Master) deleteRPCClient(ip string) {
	m.nodesRPCClientsMutex.Lock()
	delete(m.nodesRPCClients, ip)
	m.nodesRPCClientsMutex.Unlock()
}

func (m *Master) getRPCClient(ip string) (*rpc.Client, error) {
	client := &rpc.Client{}
	ok := false
	if client, ok = m.nodesRPCClients[ip]; !ok {
		return nil, fmt.Errorf("no rpc client for node: %v", ip)
	}
	return client, nil
}

func (m *Master) removeNode(ip string) error {
	spoutRePrepareList := m.spoutIndex.RemoveNode(ip)

	for _, prepare := range spoutRePrepareList {
		m.askWorkerPrepareSpout(prepare.IP, m.spoutBuilders[prepare.ID].Spout)
	}

	boltRePrepareList := m.boltIndex.RemoveNode(ip)

	for _, prepare := range boltRePrepareList {
		err := m.askWorkerPrepareBolt(prepare.IP, m.boltBuilders[prepare.ID].Bolt)
		if err != nil {
			log.Printf("removeNode: askWorkerPrepareBolt failed: %v", err)
		}
	}
	return nil
}

func (m *Master) pingMember(ip string) error {
	client, err := m.getRPCClient(ip)
	if err != nil {
		return err
	}

	var reply bool
	err = client.Call("Worker.RPCMasterPing", m.config.IP, &reply)
	if err != nil {
		return err
	}
	return nil
}

// RPCJoinGroup rpc join group
func (m *Master) RPCJoinGroup(ip string, reply *bool) error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", ip, m.config.Port))
	if err != nil {
		return err
	}
	log.Printf("Joining. IP %v ", ip)
	m.memListMutex.Lock()
	m.memList[ip] = true
	m.memListMutex.Unlock()
	m.addRPCClient(ip, client)
	m.spoutIndex.AddNewNode(ip)
	m.boltIndex.AddNewNode(ip)
	return nil
}

func (m *Master) rpcPingMember(ip string) error {
	client, err := m.getRPCClient(ip)
	if err != nil {
		return err
	}

	var reply bool
	err = client.Call("Worker.RPCMasterPing", m.config.IP, &reply)
	if err != nil {
		return err
	}

	return nil
}

// KeepPingMemberList keep ping member list
func (m *Master) keepPingMemberList() {
	for {
		time.Sleep(time.Duration(m.config.SleepTime) * time.Millisecond)
		for mem, alive := range m.memList {
			if !alive {
				continue
			}

			go func(ip string) {
				err := m.pingMember(ip)
				if err != nil {
					//log.Printf("pingMember %v: rpc.DialHTTP failed\n", mem)
					m.memListMutex.Lock()
					m.memList[ip] = false
					m.memListMutex.Unlock()
					m.deleteRPCClient(ip)
					m.removeNode(ip)
				}
			}(mem)
		}
	}
}

func (m *Master) addRPCClientForNode(ip string) []string {
	failNodes := []string{}
	fmt.Printf("addRPCClientForNode: try to add rpc client to %s\n", ip)
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", ip, m.config.Port))
	if err != nil {
		fmt.Printf("updateMemberList: rpc.DialHTTP failed")
		failNodes = append(failNodes, ip)
	}
	m.nodesRPCClients[ip] = client
	return failNodes
}

// RPCSubmitStream RPC submit stream
func (m *Master) RPCSubmitStream(builder *tpbuilder.Builder, reply *bool) error {
	log.Printf("RPCSubmitStream, submitting stream: %v", builder.ID)

	for name, builder := range builder.Spout {
		log.Printf("RPCSubmitStream, spout: %v, parallel: %v", name, builder.Parallel)
	}

	for name, builder := range builder.Bolt {
		log.Printf("RPCSubmitStream, bolt: %v, parallel: %v", name, builder.Parallel)
	}

	if _, ok := m.streamBuilders[builder.ID]; ok {
		return errors.New("streamID conflicts")
	}

	// TODO Get emit rule info froim builder and add to emit ruiles
	for bolt, boltBuilder := range builder.Bolt {
		for target, groupingType := range boltBuilder.Grouping {
			if _, ok := m.emitRules[target]; !ok {
				m.emitRules[target] = map[string]model.GroupingType{}
			}
			m.emitRules[target][bolt] = groupingType
		}
	}

	m.streamBuilders[builder.ID] = builder

	for spoutID, spoutBuilder := range m.streamBuilders[builder.ID].Spout {
		log.Printf("RPCSubmitStream: Try to deploy spout: %v, parallel: %v", spoutID, spoutBuilder.Parallel)
		parallelList := m.spoutIndex.AddToIndex(spoutID, spoutBuilder.Parallel)
		log.Printf("RPCSubmitStream: parallelList: %v", parallelList)
		for _, worker := range parallelList {
			m.askWorkerPrepareSpout(worker, spoutBuilder.Spout)
		}
	}

	for boltID, boltBuilder := range builder.Bolt {
		log.Printf("Try to deploy Bolt: %v", boltID)
		parallelList := m.boltIndex.AddToIndex(boltID, boltBuilder.Parallel)
		log.Printf("RPCSubmitStream: parallelList: %v, parallel: %v", parallelList, boltBuilder.Parallel)
		for _, worker := range parallelList {
			err := m.askWorkerPrepareBolt(worker, boltBuilder.Bolt)
			if err != nil {
				log.Printf("RPCSubmitStream: askWorkerPrepareBolt fail: %v", err)
			}
		}
	}

	return nil
}

func (m *Master) askWorkerPrepareSpout(ip string, spout spout.Spout) error {
	// TODO call RPC in workewr
	client, err := m.getRPCClient(ip)
	if err != nil {
		return err
	}

	err = client.Call("Worker.RPCPrepareSpout", spout, nil)
	if err != nil {
		return err
	}

	return nil
}

func (m *Master) askWorkerPrepareBolt(ip string, bolt bolt.Bolt) error {
	// TODO
	client, err := m.getRPCClient(ip)
	if err != nil {
		return err
	}

	err = client.Call("Worker.RPCPrepareBolt", bolt, nil)
	if err != nil {
		return err
	}

	return nil
}

func (m *Master) askToExecuteTask(ip string, uuid string, boltID string) error {
	log.Printf("askToExecuteTask: ip: %v, uuid: %v, boltID: %v, content: %v", ip, uuid, boltID, m.getContentFromUUID(uuid))
	client, err := m.getRPCClient(ip)
	if err != nil {
		return err
	}
	var reply bool
	err = client.Call("Worker.RPCExecuteTask", model.BoltTuple{UUID: uuid, ID: boltID, Tuple: m.taskMap[uuid].Tuple}, &reply)
	if err != nil {
		return err
	}

	return nil
}

func (m *Master) getWorkerForTask(boltID string, rule model.GroupingType) ([]string, error) {
	var potentialWorkers []string
	potentialWorkers = m.boltIndex.GetNodesWithFile(boltID)

	if len(potentialWorkers) == 0 {
		return nil, fmt.Errorf("no worker for bolt %v", boltID)
	}

	r := rand.Intn(len(potentialWorkers))
	return []string{potentialWorkers[r]}, nil
}

func (m *Master) executeTask(uuid string) error {

	rules := m.emitRules[m.taskMap[uuid].Tuple.ID]
	for boltID, rule := range rules {
		// TODO: grouping type
		workers, err := m.getWorkerForTask(boltID, rule)
		if err != nil {
			return err
		}

		for _, ip := range workers {
			err = m.askToExecuteTask(ip, uuid, boltID)
			if err != nil {
				log.Printf("executeTask: askToExecuteTask %v fail: %v", m.getContentFromUUID(uuid), err)
			}
		}

	}

	return nil
}

func (m *Master) dealWithEmit(emit model.TaskEmit) error {
	for _, tuple := range emit.Tuples {
		uuidObject, err := uuid.NewV4()
		if err != nil {
			return err
		}

		uuid := uuidObject.String()

		m.taskMapMutex.Lock()
		m.taskMap[uuid] = &model.CraneTask{
			Tuple: model.Tuple{
				ID:       emit.ID,
				EmitType: emit.EmitType,
				Content:  tuple,
			},
			Finished: false,
			Succeed:  false,
		}
		m.taskMapMutex.Unlock()

		go func(uuid string) {
			for {
				if !m.taskMap[uuid].Finished {
					log.Printf("dealWithEmit: Try to executeTask: %v", m.getContentFromUUID(uuid))
					err := m.executeTask(uuid)
					if err != nil {
						log.Printf("dealWithEmit: execute task: %v, failed: %v", m.getContentFromUUID(uuid), err)
					}
				} else {
					return
				}
				time.Sleep(time.Duration(m.config.TaskTimeout) * time.Millisecond)
			}
		}(uuid)
	}

	return nil
}

// RPCEmit rpc emit
func (m *Master) RPCEmit(emit model.TaskEmit, reply *bool) error {
	log.Printf("RPCEmit: ID: %v, EmitType: %v, Tuples: %v", emit.ID, emit.EmitType, emit.Tuples)
	if emit.EmitType == model.BoltEmitType {
		if m.taskMap[emit.UUID].Finished {
			return fmt.Errorf("task %v with %v has been finished", emit.UUID, emit.Tuples)
		}

		m.taskMapMutex.Lock()
		m.taskMap[emit.UUID].Finished = true
		m.taskMap[emit.UUID].Succeed = true
		m.taskMapMutex.Unlock()
	}

	if len(m.emitRules[emit.ID]) == 0 {
		log.Printf("RPCEmit: emit of %v not been subscribed by anyone", emit.ID)
		return nil
	}

	err := m.dealWithEmit(emit)
	if err != nil {
		return err
	}

	return nil
}

func main() {
	masterConfigFilePath := flag.String("c", "./config.json", "Config file path")

	masterConfigFile, err := ioutil.ReadFile(*masterConfigFilePath)
	if err != nil {
		log.Fatalf("File error: %v\n", err)
	}

	m := NewMaster(masterConfigFile)

	f, err := os.OpenFile(m.getLogPath(), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
	}
	defer f.Close()

	log.SetOutput(f)

	go m.keepPingMemberList()

	// init the rpc server
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", fmt.Sprintf(":%d", m.getPort()))
	if e != nil {
		log.Fatal("listen error: ", e)
	}

	log.Printf("Start listen rpc on port: %d", m.getPort())
	http.Serve(l, nil)

}
