package main

import (
	"CS425/CS425-MP4/bolt"
	"CS425/CS425-MP4/index"
	"CS425/CS425-MP4/model"
	"CS425/CS425-MP4/spout"
	"CS425/CS425-MP4/tpbuilder"
	"encoding/json"
	"errors"
	"fmt"
	"net/rpc"
	"sync"
	"time"
)

// Master master
type Master struct {
	config               model.CraneConfig
	nodesRPCClientsMutex *sync.Mutex
	nodesRPCClients      map[string]*rpc.Client
	memListMutex         *sync.Mutex
	memList              map[string]bool
	taskMapMutex         *sync.Mutex
	taskMap              map[string]model.CraneTask
	streamBuilders       map[string]tpbuilder.Builder
	spoutBuilders        map[string]spout.Builder
	boltBuilders         map[string]bolt.Builder
	emitRules            map[string]map[string]model.GroupingType
	spoutIndex           index.Index
	boltIndex            index.Index
	// workerIndex
	workers index.Index
}

// NewMaster init a master
func NewMaster(masterConfig []byte) Master {
	master := Master{}
	master.init(masterConfig)

	return master
}

func (m Master) loadConfigFromJSON(jsonFile []byte) error {
	return json.Unmarshal(jsonFile, &m.config)
}

func (m Master) init(masterConfig []byte) {
	json.Unmarshal(masterConfig, &m.config)
	m.taskMap = map[string]model.CraneTask{}
	m.nodesRPCClients = map[string]*rpc.Client{}
	for _, mem := range m.config.MemList {
		m.memList[mem] = false
	}
	m.emitRules = map[string]map[string]model.GroupingType{}
}

func (m Master) addRPCClient(ip string, client *rpc.Client) {
	if _, ok := m.nodesRPCClients[ip]; !ok {
		m.nodesRPCClientsMutex.Lock()
		m.nodesRPCClients[ip] = client
		m.nodesRPCClientsMutex.Unlock()
	}
}

func (m Master) deleteRPCClient(ip string) {
	m.nodesRPCClientsMutex.Lock()
	delete(m.nodesRPCClients, ip)
	m.nodesRPCClientsMutex.Unlock()
}

func (m Master) getRPCClient(ip string) (*rpc.Client, error) {
	client := &rpc.Client{}
	ok := false
	if client, ok = m.nodesRPCClients[ip]; !ok {
		return nil, fmt.Errorf("no rpc client for node: %v", ip)
	}
	return client, nil
}

func (m Master) removeNode(ip string) error {
	rePrepareList := m.spoutIndex.RemoveNode(ip)

	for _, prepare := range rePrepareList {
		m.askWorkerPrepareSpout(prepare.IP, m.spoutBuilders[prepare.ID].Spout)
	}

	rePrepareList := m.boltIndex.RemoveNode(ip)

	for _, prepare := range rePrepareList {
		m.askWorkerPrepareBolt(prepare.IP, m.boltBuilders[prepare.ID].Bolt)
	}
}

func (m Master) pingMember(ip string) error {
	client, err := rpc.DialHTTP("tcp", fmt.Sprintf("%s:%d", ip, m.config.Port))
	if err != nil {
		fmt.Printf("pingMember: rpc.DialHTTP failed")
		m.memList[ip] = false
		m.deleteRPCClient(ip)
		m.removeNode(ip)
		return err
	}

	m.addRPCClient(ip, client)
	m.rpcPingMember(ip)
	return nil
}

func (m Master) rpcPingMember(ip string) error {
	client, err := m.getRPCClient(ip)
	if err != nil {
		return err
	}

	err = client.Call("Worker.RPCMasterPing", m.config.IP, nil)
	if err != nil {
		return err
	}

	return nil
}

// KeepPingMemberList keep ping member list
func (m Master) KeepPingMemberList() {
	for {
		time.Sleep(time.Duration(m.config.SleepTime) * time.Millisecond)
		for mem := range m.memList {
			go m.pingMember(mem)
		}
	}
}

func (m Master) addRPCClientForNode(ip string) []string {
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
func (m *Master) RPCSubmitStream(builder tpbuilder.Builder, reply *bool) error {
	if _, ok := m.streamBuilders[builder.ID]; ok {
		return errors.New("streamID conflicts")
	}

	// TODO Get emit rule info froim builder and add to emit ruiles
	for bolt, boltBuilder := range builder.Bolt {

		for target, groupingType := range boltBuilder.Grouping {
			m.emitRules[target][bolt] = groupingType
		}
		m.emitRules[builder.Bolt[bolt].ID] = builder.Bolt[bolt].Grouping
	}
	m.emitRules[builder.ID] = subscribed

	m.streamBuilders[builder.ID] = builder

	for spout := range m.streamBuilders[builder.ID].Spout {
		parallelList := m.spoutIndex.AddToIndex(spout, builder.Spout[spout].Parallel)
		// workers[spout] = parallelList
		for worker := range parallelList {
			m.askWorkerPrepareSpout(worker, spout.Spout)
		}
	}

	for bolt := range builder.Bolt {
		parallelList := m.boltIndex.AddToIndex(bolt, builder.Bolt[bolt].Parallel)
		// workers[builder.ID] = parallelList
		for worker := range parallelList {
			m.askPrepareBolt()
		}
	}

	return nil
}

func (m Master) askWorkerPrepareSpout(ip string, spout spout.Spout) {
	// TODO call RPC in workewr
	client, err := m.getRPCClient(ip)
	if err != nil {
		return err
	}

	err = client.Call("Worker.RPCPrepareSpout", spout, nil)
	if err != nil {
		return err
	}
}

func (m Master) askWorkerPrepareBolt(ip string, bolt bolt.Bolt) {
	// TODO
	client, err := m.getRPCClient(ip)
	if err != nil {
		return err
	}

	err = client.Call("Worker.RPCPrepareBolt", bolt, nil)
	if err != nil {
		return err
	}
}

func (m Master) askToExecuteTask(ip string, uuid string) {
	client, err := m.getRPCClient(ip)
	if err != nil {
		return err
	}

	err = client.Call("Worker.RPCExecuteTask", m.taskMap[uuid], nil)
	if err != nil {
		return err
	}
}

func (m Master) getWorkerForTask(uuid string) ([]string, error) {
	// TODO
	tupleType:=m.taskMap[uuid]
	var potentialWorkers []string
	if tupleType.Tuple.EmitType==model.SpoutEmitType{
		potentialWorkers = m.spoutIndex.GetNodesWithFile(tupleType.Tuple.ID)
	}else{
		potentialWorkers = m.boltIndex.GetNodesWithFile(tupleType.Tuple.ID)
	}
	r := rand.Intn(len(potentialWorkers))
	return {potentialWorkers[r]}, nil
}

func (m Master) executeTask(uuid string) error {
	workers, err := m.getWorkerForTask(uuid)
	if err != nil {
		return err
	}

	for _, worker := range workers {
		m.askToExecuteTask(worker, uuid)
	}
}

func (m Master) dealWithEmit(emit model.TaskEmit) error {
	for _, tuple := range emit.Tuples {
		uuid, err := uuid.NewV4().String()
		if err != nil {
			return err
		}

		m.taskMapMutex.Lock()
		m.taskMap[uuid] = model.CraneTask{
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
					m.executeTask(uuid)
				} else {
					return
				}
				time.Sleep(time.Duration(m.config.TaskTimeout) * time.Millisecond)
			}
		}(uuid)
	}

}

// RPCEmit rpc emit
func (m *Master) RPCEmit(emit model.TaskEmit, reply *bool) error {
	if emit.EmitType == model.BoltEmitType {
		if m.taskMap[emit.UUID].Finished {
			return errors.New("task has been finished")
		}
		m.taskMapMutex.Lock()
		m.taskMap[emit.UUID].Finished = true
		m.taskMap[emit.UUID].Succeed = true
		m.taskMapMutex.Unlock()
	}

	err = m.dealWithEmit(emit)
	if err != nil {
		return err
	}

	return nil
}
