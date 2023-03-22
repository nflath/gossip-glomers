// This is a broadcast node that meets the requirements for part e). The key
// thing to note is that, with 25 nodes, if you broadcast for each messages at a
// minimum you'll need to send 24 * 2 per messages.  This is already over our
// limit, so the only way is by batching the messages - the fact that the
// latency requirements were much less loose is another hint.

// We still generate a spanning tree.  When we receive a message, we add it to a
// to-send list to the appropriate nodes.  Every 200ms, we send each neighbor a
// message with all of the pending updates for it.  

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

// Generate a spanning tree for the number of nodes and number of children per
// node.  There are always 25 nodes in this one, but why not parameterize it.
// We should really just pass in the node list, that would also prevent the generation of node names.
// In fact:
// TODO(nflath): Pass in list of nodes instead of # of nodes and constructing names
// But that cleanup isn't interesting to me at this point.
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

// Key indexing used to track pending messages.  Used to track whether we need to retry.
type BatchKey struct {
	node string
	msg_id float64
}

// Data per message - timeout is the time we consider this send a failure and
// retry, messages is the list of indexes.
type BatchData struct{
	timeout time.Time
	messages []float64
}

func main() {
	n := maelstrom.NewNode()

	// We need to keep track of msg_ids ourselves - these are transparently
	// added by the go framework, annoyingly.
	var next_id float64 = 1

	// What messages we have seen.  Keep track so we don't have to retransmiut.
	messages := make(map[float64]bool)

	var messages_arr []float64
	var mu sync.Mutex
	
	// List of our neighbors
	neighbors := make([]interface{},0)

	// For each neighbor, messages we need to send them.  We never reset this.
	neighbors_to_batch := make(map[string][]float64)

	// Messages we haven't seen acked, and information needed to potentially retry
	outstanding_messages := make(map[BatchKey]BatchData)

	broadcast_ok := func(msg maelstrom.Message) error {
		// Neighbor has acked our send.  They have received it, and we do not
		// need to retry.
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		bs := BatchKey{msg_id: body["in_reply_to"].(float64), node: msg.Src}
		delete(outstanding_messages,bs)
		
		return nil
	}

	sendLoop := func() {
		// Every 200ms, send a message to each neighbor with the new values we need to send them.
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
			//TODO(nflath): parameterize this
			time.Sleep(200 * time.Millisecond)

		}
	}
	go sendLoop()

	handle_message := func (message float64, src string) {
		_, ok := messages[message]
		messages[message] = true

		if(ok) {
			// If we've already seen this value, ignore it.
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
