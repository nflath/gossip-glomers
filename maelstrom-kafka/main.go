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

	node_log := make(map[string]map[float64]float64);

	var mu sync.Mutex

	var offset float64 = 0
	//committed_offsets := make(map[string][]float64)

	n.Handle("send", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		msg_ := body["msg"].(float64)

		node_log[key][offset] = msg_
		offset += 1

		reply_body := make(map[string]any)
		reply_body["type"] = "send_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, reply_body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		// var body map[string]any
		// if err := json.Unmarshal(msg.Body, &body); err != nil {
		// 	return err
		// }

		//		offsets := body["offsets"]

		reply_body := make(map[string]any)
		reply_body["type"] = "poll_ok"

		return n.Reply(msg, reply_body)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		// var body map[string]any
		// if err := json.Unmarshal(msg.Body, &body); err != nil {
		// 	return err
		// }
		// offsets := body["offsets"]
		
		 reply_body := make(map[string]any)
		 reply_body["type"] = "commit_offsets_ok"

		return n.Reply(msg, reply_body)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		// var body map[string]any
		// if err := json.Unmarshal(msg.Body, &body); err != nil {
		// 	return err
		// }
		// keys := body["keys"] 
		// for key : range keys 

		var reply_body = make(map[string]any)
		reply_body["type"] = "list_commited_offsets_ok"
		

		return n.Reply(msg, reply_body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
