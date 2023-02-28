package main

import (
	"log"
	"sync"
	"encoding/json"
	"os"
	"context"
	
	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)
	ctx := context.Background()

	var count float64 = 0
	var mu sync.Mutex

	// Register a handler for the "echo" message that responds with an "echo_ok".
	n.Handle("add", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := body["delta"].(float64)

		count = count + delta
		
		for(true) {
			err := kv.Write(ctx, n.ID(), count)
			if(err != nil) { log.Printf("Err on write: %s",err) }
			v, err := kv.Read(ctx, n.ID())
			if(err != nil) { v = -1; }

			log.Printf("%s %s", count, v)
			if(v == int(count)) { break; }
			
			log.Printf("Retrying write")
			
		}

		reply_body := make(map[string]any)
		reply_body["type"] = "add_ok"
		// Echo the original message back with the updated message type.
		return n.Reply(msg, reply_body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		body := make(map[string]any)
		body["type"] = "read_ok"

		i, err := kv.ReadInt(ctx, "n0")
		if(err != nil) { i = 0 }
		j, err := kv.ReadInt(ctx, "n1")
		if(err != nil) { j = 0 }
		k, err := kv.ReadInt(ctx, "n2")
		if(err != nil) { k = 0 }

		log.Printf("%s %s %s", i, j, k)

		body["value"] = i+j+k

		// Echo the original message back with the updated message type.
		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
