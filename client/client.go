package main

import (
	"CS425/CS425-MP4/bolt"
	"CS425/CS425-MP4/model"
	"CS425/CS425-MP4/spout"
	"CS425/CS425-MP4/tpbuilder"
	"encoding/json"
	"io/ioutil"
	"log"
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

	//spout := RandomeSentenceSpout{}
	//splitBolt := SplitSentenceBolt{}

	mySpout := spout.Spout{ID: "spout", Activate: model.CMD{Name: "mySpout"}}
	builder := tpbuilder.NewTpBuilder(craneConfig.MasterIP, craneConfig.MasterPort)
	builder.SetSpout("spout", mySpout, 5)

	filterBolt := bolt.Bolt{ID: "filter", Execute: model.CMD{Name: "filter", Args: []string{}}}
	filterBt := builder.SetBolt("filter", filterBolt, 8)
	filterBt.ShuffleGrouping("spout")

	uppercaseBolt := bolt.Bolt{ID: "uppercase", Execute: model.CMD{Name: "uppercase", Args: []string{}}, Finish: model.CMD{Name: "finish", Args: []string{}}}
	uppercaseBt := builder.SetBolt("uppercase", uppercaseBolt, 8)
	uppercaseBt.ShuffleGrouping("filter")

	e = builder.Submit("uppercase-stream")
	if e != nil {
		log.Printf("Submit Stream Failed: %v", e)
	}
}
