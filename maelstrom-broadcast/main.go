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

type BroadcastState struct {
	node string
	msg_id float64
}

type BroadcastData struct{
	time time.Time
	val float64
}


func generate_topo(num_nodes int, num_children int) map[string][]interface{} {
	topology := make(map[string][]interface{},0)

	remaining := make([]string,0)

		for i := 0; i < 25; i++ {
		remaining = append(remaining, "n" + strconv.Itoa(i))
		topology["n" + strconv.Itoa(i)] = make([]interface{},0)
	}
	
	for ; len(remaining) > 0;  {
		cur := remaining[0]
		remaining = remaining[1:]
		if(len(remaining) == 0) {
			topology[cur] = append(topology[cur], "n0")
			topology["n0"] = append(topology["n0"], cur)
		}
		for  j := 0; j < num_children && len(remaining) > 0; j++ {
			next := remaining[0];
			remaining = remaining[1:]
			topology[cur] = append(topology[cur], next)
			topology[next] = append(topology[next], cur)
		}
	}

	log.Printf("Print %s", topology)	

	return topology
}

func main() {
	n := maelstrom.NewNode()

	var nextId float64 = 1

	messages := make(map[float64]bool)
	var messages_arr []float64
	var mu sync.Mutex

	neighbors := make([]interface{},0)
	outstanding_messages := make(map[BroadcastState]BroadcastData)

	broadcast_ok := func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		bs := BroadcastState{msg_id: body["msg_id"].(float64), node: msg.Src}
		log.Printf("Deleting %s %s", bs, len(outstanding_messages))
		delete(outstanding_messages,bs)
		log.Printf("Deleting %s %s", bs, len(outstanding_messages))
		
		return nil
	}

	timeoutLoop := func() {
		for(true) {
			{
				mu.Lock()
				for bs, bd := range outstanding_messages {
					if (bd.time.Before(time.Now())) {
						log.Printf("Retrying message: %s %s %s", bs.msg_id, bs.node, bd.val)
						body :=make(map[string]any)
						
						body["type"]="broadcast"
						body["message"] = bd.val

						bs_ := BroadcastState{msg_id: nextId, node: bs.node}
						nextId = nextId + 1
						bd_ := BroadcastData{val: bd.val, time: time.Now().Add(230 * time.Millisecond)}

						delete(outstanding_messages, bs)
						outstanding_messages[bs_] = bd_
						
						n.RPC(bs.node, body, broadcast_ok)
					}
					
				}
				mu.Unlock()
				
				time.Sleep(10 * time.Millisecond)
			}
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
		_, ok := messages[message.(float64)]
		messages[message.(float64)] = true

		if(ok) {
			body["type"] = "broadcast_ok"
			delete(body, "message")
			return n.Reply(msg, body)
		}

		messages_arr = append(messages_arr, message.(float64))
		
		for _, neighbor := range neighbors {
			if(neighbor == msg.Src) {
				continue;
			}
			log.Printf("%s %s", neighbor, msg.Src)
			

			bs := BroadcastState{msg_id: nextId, node: neighbor.(string)}
			nextId = nextId + 1
			bd := BroadcastData{val: message.(float64), time: time.Now().Add(220 * time.Millisecond)}
			
			n.RPC(neighbor.(string), body, broadcast_ok)

			outstanding_messages[bs] = bd

		}

		body["type"] = "broadcast_ok"
		delete(body, "message")
		return n.Reply(msg, body)
	})


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
		//		var topology map[string][]interface{} = generate_topo(25,3)
		neighbors = topology[n.ID()].([]interface{})
		log.Printf("neighbors:: %s", neighbors)
		delete(body, "topology")

		// Update the message type.
		body["type"] = "topology_ok"
		
		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	go timeoutLoop()

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
