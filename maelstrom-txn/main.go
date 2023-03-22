// With a single node, very easy.  Just maintain a local key-value store.

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

	var kv = make(map[float64]float64)
	n.Handle("txn", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		txn := body["txn"].( []interface{} )

		for _, op := range txn {
			op_ := op.([]interface{})
			typ := op_[0].(string)
			key := op_[1].(float64)

			if(typ == "r") {
				op_[2] = kv[key]
			}
			if(typ == "w") {
				val := op_[2].(float64)
				kv[key] = val
			}
				  
		}

		var reply_body = make(map[string]any)


		reply_body["type"] = "txn_ok"
		reply_body["txn"] = txn

		return n.Reply(msg, reply_body)
	})

		// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
