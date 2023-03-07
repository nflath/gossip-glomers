package main

import (
	"encoding/json"
	"log"
	"strconv"
	"os"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func generate_topo(num_nodes int, num_children int) map[string][]interface{} {
	topology := make(map[string][]interface{},0)

	remaining := make([]string,0)

	for i := 1; i < 25; i++ {
		remaining = append(remaining, "n" + strconv.Itoa(i))
		topology["n" + strconv.Itoa(i)] = make([]interface{},0)
	}

	i := 0
	cur := "n" + strconv.Itoa(i)
	i += 1

	for ; len(remaining) > 0;  {

		if(len(topology[cur]) == num_children) {
			cur = "n" + strconv.Itoa(i)
			i += 1
		}

		next := remaining[0];
		remaining = remaining[1:]

		topology[cur] = append(topology[cur], next)
		topology[next] = append(topology[next], cur)
	}

	log.Printf("Print %s", topology)

	return topology
}

type BatchKey struct {
	node string
	msg_id float64
}

type BatchData struct{
	timeout time.Time
	messages []float64
}

func main() {
	n := maelstrom.NewNode()

	var next_id float64 = 1

	messages := make(map[float64]bool)
	var messages_arr []float64
	var mu sync.Mutex

	neighbors := make([]interface{},0)

	neighbors_to_batch := make(map[string][]float64)


	outstanding_messages := make(map[BatchKey]BatchData)

	broadcast_ok := func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		bs := BatchKey{msg_id: body["in_reply_to"].(float64), node: msg.Src}
		log.Printf("Deleting %s %s", bs, len(outstanding_messages))
		delete(outstanding_messages,bs)
		log.Printf("Deleting %s %s", bs, len(outstanding_messages))

		return nil
	}

	sendLoop := func() {
		for(true) {

			mu.Lock()
			for neighbor, batch := range neighbors_to_batch  {
				if(len(batch) > 0) {

					body :=make(map[string]any)
					body["type"]="broadcast"
					body["message"] = batch

					bs := BatchKey{node: neighbor, msg_id: next_id}
					next_id = next_id + 1
					bd := BatchData{messages: batch, timeout: time.Now().Add(230 * time.Millisecond)}
					outstanding_messages[bs] = bd

					n.RPC(neighbor, body, broadcast_ok)

					neighbors_to_batch[neighbor] = make([]float64, 0)
				}
			}
			mu.Unlock()
			time.Sleep(200 * time.Millisecond)

		}
	}

	go sendLoop()

	handle_message := func (message float64, src string) {
		_, ok := messages[message]
		messages[message] = true

		if(ok) {
			return
		}

		messages_arr = append(messages_arr, message)

		for _, neighbor := range neighbors {
			if(neighbor == src) {
				continue;
			}
			neighbors_to_batch[neighbor.(string)] =
				append(neighbors_to_batch[neighbor.(string)], message)
		}
	}

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
		switch  message.(type) {
		case float64:
			handle_message(message.(float64),msg.Src)
		default:
			for _,v := range message.([]interface{}) {
				handle_message(v.(float64),msg.Src)
			}
		}


		var reply_body = make(map[string]any)
		reply_body["type"] = "broadcast_ok"
		return n.Reply(msg, reply_body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		mu.Lock()
		defer mu.Unlock()

		var reply_body = make(map[string]any)

		reply_body["type"] = "read_ok"
		reply_body["messages"] = messages_arr


		return n.Reply(msg, reply_body)
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
		var topology map[string][]interface{} = generate_topo(25,3)
		neighbors = topology[n.ID()]

		log.Printf("neighbors:: %s", neighbors)
		delete(body, "topology")

		// Update the message type.
		body["type"] = "topology_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	//go timeoutLoop()

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
