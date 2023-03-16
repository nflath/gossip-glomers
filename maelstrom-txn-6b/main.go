package main

import (
	"log"
	"sync"
	"time"
	"context"
	//"strconv"
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

	//var txns = make(map[float64][]interface{})
	var node_to_txns = make(map[string]map[float64][]interface{})
	neighbors := make([]interface{}, 0)

	broadcast := func() {
		for _, neighbor := range neighbors {
			body :=make(map[string]any)
			body["type"]="broadcast"
			body["message"] = node_to_txns[n.ID()]
			n.RPC(neighbor.(string), body, nil)
			}
	}
	
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
		txn := body["txn"].( []interface{} )
		
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

		node_to_txns[n.ID()][txn_id] = txn

		broadcast()
		
		
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
		
		var reply_body = make(map[string]any)

		reply_body["type"] = "txn_ok"
		reply_body["txn"] = txn

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
