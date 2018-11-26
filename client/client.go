package main

import (
	"CS425/CS425-MP4/collector"
	"CS425/CS425-MP4/model"
	"CS425/CS425-MP4/tpbuilder"
	"encoding/json"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
)

// RandomeSentenceSpout random sentence
type RandomeSentenceSpout struct {
	stopChan chan bool
}

// Deactive deactive
func (r RandomeSentenceSpout) Deactive() {
	r.stopChan <- true
}

// Activate activate
func (r RandomeSentenceSpout) Activate(collector collector.OutputCollector) {
	sentences := []string{"the cow jumped over the moon", "an apple a day keeps the doctor away", "four score and seven years ago", "snow white and the seven dwarfs", "i am at two with nature"}

	for {
		select {
		default:
			collector.Emit(sentences[rand.Intn(len(sentences))])
		case <-r.stopChan:
			break
		}
	}
}

// SplitSentenceBolt split sentence bolt
type SplitSentenceBolt struct {
}

// Execute execute
func (s SplitSentenceBolt) Execute(task model.BoltTuple, collector collector.OutputCollector) {
	words := strings.Fields(task.Tuple)

	for _, word := range words {
		collector.Emit(word)
	}
}

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

	spout := RandomeSentenceSpout{}
	splitBolt := SplitSentenceBolt{}

	builder := tpbuilder.NewTpBuilder(craneConfig.MasterIP, craneConfig.MasterPort)
	builder.SetSpout("spout", spout, 5)

	splitBt := builder.SetBolt("split", splitBolt, 8)
	splitBt.ShuffleGrouping("spout")

	e = builder.Submit("word-count")
	if e != nil {
		log.Printf("Submit Stream Failed: %v", e)
	}
}
