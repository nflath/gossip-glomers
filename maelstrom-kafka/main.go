// Single-node version of Kafka, we can just use local storage.

// This took me a while to understand, but it's just exactly the description of
// the problem.  I thought there would be some relationship between the
// commmited_offset and return values of the others, but no.  Simple once you understand the problem.
package main

import (
	"log"
	"sync"
	"encoding/json"
	"os"
	
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()

	var mu sync.Mutex

	var next_offset = make(map[string]float64)
	var committed_offset = make(map[string]float64)
	node_log := make(map[string]map[float64]float64);

	n.Handle("send", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		msg_ := body["msg"].(float64)

		_, ok := next_offset[key]
		if (ok == false) {
			next_offset[key] = 0
			node_log[key] = make(map[float64]float64)
		}
		offset := next_offset[key]

		next_offset[key] = next_offset[key]+1
		node_log[key][offset] = msg_

		reply_body := make(map[string]any)
		reply_body["type"] = "send_ok"
		reply_body["offset"] = offset
		return n.Reply(msg, reply_body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()
		
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
		 	return err
		}
		
		var offsets map[string]interface{} = body["offsets"].(map[string]interface{})
		
		reply_body := make(map[string]any)
		reply_body["type"] = "poll_ok"
		reply_body["msgs"] = make(map[string][][]float64)
		for key, val := range offsets {
			
			_, ok := node_log[key]
			if (ok == false) {
				continue
			}
			_, ok = node_log[key][val.(float64)]
			if (ok == false) {
				continue
			}
			reply_body["msgs"].(map[string][][]float64)[key] = [][]float64{[]float64{val.(float64), node_log[key][val.(float64)]}}
		}
		
		return n.Reply(msg, reply_body)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
		 	return err
		}
		
		var offsets map[string]interface{} = body["offsets"].(map[string]interface{})
		
		for key, val := range offsets {
			committed_offset[key] = val.(float64)
		}
		
		reply_body := make(map[string]any)
		reply_body["type"] = "commit_offsets_ok"

		return n.Reply(msg, reply_body)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
		 	return err
		}
		keys := body["keys"].([]interface{})

		var reply_body = make(map[string]any)
		reply_body["type"] = "list_committed_offsets_ok"
		
		reply_body["offsets"] = make(map[string]float64)
		for _, key := range keys {
			_, ok := committed_offset[key.(string)]
			if ok {
				reply_body["offsets"].(map[string]float64)[key.(string)] = committed_offset[key.(string)]
			}
		}
		
		return n.Reply(msg, reply_body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
