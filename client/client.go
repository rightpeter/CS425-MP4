package main

import (
	"CS425/CS425-MP4/bolt"
	"CS425/CS425-MP4/model"
	"CS425/CS425-MP4/spout"
	"CS425/CS425-MP4/tpbuilder"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"time"
)

func main() {
	jsonFile, e := ioutil.ReadFile("./config.json")
	if e != nil {
		log.Fatalf("File error: %v\n", e)
	}

	craneConfig := model.CraneConfig{}
	e = json.Unmarshal(jsonFile, &craneConfig)
	if e != nil {
		log.Printf("Unmarshal jsonFile fail: %v\n", e)
	}

	mySpout := spout.Spout{ID: "spout", Activate: model.CMD{Name: "mySpout", Args: []string{"/tmp/large.txt"}}}
	builder := tpbuilder.NewTpBuilder(craneConfig.MasterIP, craneConfig.MasterPort)
	builder.SetSpout("spout", mySpout, 1)

	filterBolt := bolt.Bolt{ID: "filter", Execute: model.CMD{Name: "filter", Args: []string{}}}
	filterBt := builder.SetBolt("filter", filterBolt, 8)
	filterBt.ShuffleGrouping("spout")

	//uppercaseBolt := bolt.Bolt{ID: "uppercase", Execute: model.CMD{Name: "uppercase", Args: []string{}}, Finish: model.CMD{Name: "finish", Args: []string{}}}
	//uppercaseBt := builder.SetBolt("uppercase", uppercaseBolt, 8)
	//uppercaseBt.ShuffleGrouping("filter")

	wordCountBolt := bolt.Bolt{ID: "uppercase", Execute: model.CMD{Name: "uppercase", Args: []string{}}}
	wordCountBt := builder.SetBolt("uppercase", wordCountBolt, 8)
	wordCountBt.ShuffleGrouping("filter")

	e = builder.Submit(fmt.Sprintf("uppercase-stream-%d", time.Now().Unix()))
	if e != nil {
		log.Printf("Submit Stream Failed: %v", e)
	}
}
