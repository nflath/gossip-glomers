// Whenever we get a message, just send it on to all neighbors (aside the one
// that sent it to us.  To deal with partitions, have a retry mechanism and
// retry any message that is unacked for 230 seconds; to get the efficiency
// constraints, generate a spanning tree, so we only have to send one meesage to
// each node per client request, and it's constructed with # children to limit
// the number of hops any message will need to make.

// The main thing that was a pain was realizing that the msg_id I was setting
// was getting overwritten by the framework.
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

// Generate a spanning tree.  TODO(nflath): Pass in the number of nodes, and
// figure out the optimal number of children.
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

func main() {
	n := maelstrom.NewNode()

	// Next message ID to send.  Trying to set a value for it yourself causes
	// the framework to overwrite it, so keep track of what msg_id will be set manually.
	var nextId float64 = 1

	// Have both a slice containing all values, and a map (for testing prescence to stop re-sends)
	messages := make(map[float64]bool)
	var messages_arr []float64

	var mu sync.Mutex

	var neighbors []interface{}
	outstanding_messages := make(map[BroadcastState]BroadcastData)

	broadcast_ok := func(msg maelstrom.Message) error {
		// If we receive an ack to a message we send, stop retrying the message
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		bs := BroadcastState{msg_id: body["msg_id"].(float64), node: msg.Src}
		delete(outstanding_messages,bs)
		
		return nil
	}

	timeoutLoop := func() {
		// Every 10ms, see if there are any messages that have 'timed out'(no
		// response within 230 m) and if so retry them.
		// TODO(nflath): make the timeouts configurable
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
	go timeoutLoop()
	
	n.Handle("broadcast", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()
		
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"]
		_, ok := messages[message.(float64)]
		messages[message.(float64)] = true

		if(ok) {
			// If we already have this message, we don't need to send it on to our neighbors.
			body["type"] = "broadcast_ok"
			delete(body, "message")
			return n.Reply(msg, body)
		}


		messages_arr = append(messages_arr, message.(float64))

		// Send this to each neighbor other than where it came from.  Register
		// it so that if we don't hear a response, we retry.
		for _, neighbor := range neighbors {
			if(neighbor == msg.Src) {
				continue;
			}
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
		mu.Lock()
		defer mu.Unlock()
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "read_ok"
		body["messages"] = messages_arr
		return n.Reply(msg, body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		// TODO(nflath): Use this to get the list of node names, but otherwise ignore
		// it.
		mu.Lock()
		defer mu.Unlock()

		var topology map[string][]interface{} = generate_topo(25,3)
		neighbors = topology[n.ID()]
		
		delete(body, "topology")
		body["type"] = "topology_ok"
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
