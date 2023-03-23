// Very inefficient, but this was pretty simple.  When we get a txn message, we
// get a unique transaction ID by CompareAndSwap incrementing on a SeqKv until
// it suceeds.  This gets us a ordered, unique transaction ID.  Each node
// maintains a map of all ID->txn, where txn is just the map sent in the txn
// message for that ID.  We broadcast this map to every neighbor ever 200ms (or
// when a txn is updated locally).  Each neighbor maintains a list not only of
// all the txns it's aware of, but all txns it's neighbors are aware of.  We
// evaluate all txn updates that node n0 has broadcast, then apply the
// transaction we just were sent in order to return the value.  It should really
// be all the txns every node is aware of, to maintain the ordering properties,
// but a very strict one would be to wait until every node is aware of every txn
// up to this one's ID.
//
// Neighbors should be every node, in this case there's only one.

package main

import (
	"log"
	"sync"
	"time"
	"context"
	"encoding/json"
	"os"
	
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	var mu sync.Mutex


	var next_txn_id float64 = 1
	
	ctx, _ := context.WithTimeout(context.Background(), 100 * time.Millisecond)

	var node_to_txns = make(map[string]map[float64][]interface{})
	neighbors := make([]interface{}, 0)

	broadcast := func() {
		// Send all the txns we know about to all our neighbors.
		for _, neighbor := range neighbors {
			body :=make(map[string]any)
			body["type"]="broadcast"
			body["message"] = node_to_txns[n.ID()]
			n.RPC(neighbor.(string), body, nil)
		}
	}

	// Every 200 ms, broadcast this node's txns.
	sendLoop := func() {
		for(true) {
			mu.Lock()
			broadcast()
			mu.Unlock()
			time.Sleep(200 * time.Millisecond)
		}
	}
	go sendLoop()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		// When we receive a broadcast from a node, update the txns we know that
		// node knows.
		src := msg.Src

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		message := body["message"].(map[float64][]interface{})
		node_to_txns[src] = message

		for id, txn := range message {
			node_to_txns[n.ID()][id] = txn
		}
		
		return nil
	})

	n.Handle("txn", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		// new txn
		txn := body["txn"].( []interface{} )
		
		// Get the next unique TXN ID from the SeqQV.  
		for {
			err := kv.CompareAndSwap(ctx,
				"next-txn-id",
				next_txn_id, 
				next_txn_id+1, 
				true)
		
			if(err != nil) {
				next_txn_id++;
				continue
			}
			if(err == nil) {
				break;
			}
		}
		txn_id := next_txn_id
		next_txn_id++

		// Update our map and send to other nodes - but we don't have to wait.
		node_to_txns[n.ID()][txn_id] = txn
		broadcast()
				
		// Generate a keystore by applying every txn node n0 knows about
		// TODO(nflath): Generalize and improve by not having to apply every txn
		// on every new txn.
		var local_kv = make(map[float64]float64)
		for  i := 0; i < int(next_txn_id); i++ {
			txn_n0, ok_n1 := node_to_txns["n0"][float64(i)]
			_, ok_n2 := node_to_txns["n0"][float64(i)]
			
			if(ok_n1  && ok_n2) {
				break;
			}
			
			for _, op := range txn_n0 {
				op_ := op.([]interface{})
				typ := op_[0].(string)
				key := op_[1].(float64)

				if(typ == "w") {
					val := op_[2].(float64)
					local_kv[key] = val
				}
			}
		}

		// Apply current TXN
		// TODO(nflath): merge with above.
		for _, op := range txn {
			op_ := op.([]interface{})
			typ := op_[0].(string)
			key := op_[1].(float64)

			if(typ == "r") {
				op_[2] = local_kv[key]
			}
			if(typ == "w") {
				val := op_[2].(float64)
				local_kv[key] = val
			}
				  
		}
		
		// Return values from txn
		var reply_body = make(map[string]any)
		reply_body["type"] = "txn_ok"
		reply_body["txn"] = txn
		return n.Reply(msg, reply_body)
	})

	// Assign list of neighbors.  
	n.Handle("topology", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()
		// TODO(nflath): Ignore the actual topology and use all the other nodes
		// to generalize than more than 2 nodes.
		
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
		return n.Reply(msg, body)
	})

		// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
