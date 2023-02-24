package main

import (
	"encoding/json"
	"log"
	"os"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type BroadcastState struct {
	msg_id float64
	node string
}

func main() {
	n := maelstrom.NewNode()

	messages := make(map[float64]bool)
	var messages_arr []float64
	var mu sync.Mutex

	neighbors := make([]interface{},0)

	outstanding_messages := make(map[BroadcastState]float64)

	// Register a handler for the "echo" message that responds with an "echo_ok".
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"]
		_, ok := messages[message.(float64)]
		messages[message.(float64)] = true

		if(ok) {
			body["type"] = "broadcast_ok"
			delete(body, "message")
			return n.Reply(msg, body)
		}

		messages_arr = append(messages_arr, message.(float64))
		
		for _, neighbor := range neighbors {
			bs := BroadcastState{msg_id: body["msg_id"].(float64), node: neighbor.(string)}
			outstanding_messages[bs] = message.(float64)
			n.RPC(neighbor.(string), body, nil)
		}

		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)
	})

	// n.Handle("broadcast_ok", func(msg maelstrom.Message) error {
	// 	mu.Lock()
	// 	defer mu.Unlock()
	// 	var body map[string]any
	// 	if err := json.Unmarshal(msg.Body, &body); err != nil {
	// 		return err
	// 	}
	// 	bs := BroadcastState{msg_id: body["msg_id"].(float64), node: msg.Src}
	// 	delete(outstanding_messages,bs)
	// 	return nil
	// });

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		mu.Lock()
		defer mu.Unlock()
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// Update the message type.
		body["type"] = "read_ok"
		body["messages"] = messages_arr

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// Unmarshal the message body as an loosely-typed map.
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		topo := body["topology"]
		var topology map[string]interface{} = topo.(map[string]interface{})
		neighbors = topology[n.ID()].([]interface{})
		delete(body, "topology")

		// Update the message type.
		body["type"] = "topology_ok"
		
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
