package main

import (
	"log"
"time"
	"sync"
	"encoding/json"
	"os"
	"context"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	var count float64 = 0
	var mu sync.Mutex

	var last_j float64 = 0
	var last_k float64 = 0

	// Register a handler for the "echo" message that responds with an "echo_ok".
	n.Handle("add", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		ctx, _ := context.WithTimeout(context.Background(),230 * time.Millisecond)

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

	n.Handle("local", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		body := make(map[string]any)
		body["type"] = "local_ok"
		ctx, _ := context.WithTimeout(context.Background(),230 * time.Millisecond)
		v, _ := kv.Read(ctx, n.ID())
		if(v != nil) {
			body["val"] =  v
		} else {
			body["val"] =  0
		}


		return n.Reply(msg, body)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		reply_body := make(map[string]any)
		reply_body["type"] = "read_ok"

		local_body := make(map[string]any)
		local_body["type"] = "local"

		ctx, _ := context.WithTimeout(context.Background(),230 * time.Millisecond)
		i, err := kv.ReadInt(ctx, n.ID())
		var j_r maelstrom.Message;
		var k_r maelstrom.Message;

		if(n.ID() == "n0") {
			ctx, _ := context.WithTimeout(context.Background(),230 * time.Millisecond)
			j_r, err = n.SyncRPC(ctx, "n1", local_body)
			ctx, _ = context.WithTimeout(context.Background(),230 * time.Millisecond)
			k_r, err = n.SyncRPC(ctx, "n2", local_body)
		} else if(n.ID() == "n1") {
			ctx, _ := context.WithTimeout(context.Background(),230 * time.Millisecond)
			j_r, err = n.SyncRPC(ctx, "n0", local_body)
			ctx, _ = context.WithTimeout(context.Background(),230 * time.Millisecond)
			k_r, err = n.SyncRPC(ctx, "n2", local_body)
		} 	else {
			ctx, _ := context.WithTimeout(context.Background(),230 * time.Millisecond)
			j_r, err = n.SyncRPC(ctx, "n0", local_body)
			ctx, _ = context.WithTimeout(context.Background(),230 * time.Millisecond)
			k_r, err = n.SyncRPC(ctx, "n1", local_body)
		}


		var body map[string]any
		var j float64
		var k float64

		if err := json.Unmarshal(j_r.Body, &body); err != nil {
			j  = last_j
		} else {
			j = body["val"].(float64)
		}

		if err := json.Unmarshal(k_r.Body, &body); err != nil {
			k = last_k
		} else {
			k = body["val"].(float64)
		}


		last_j = j
		last_k = k

		if(err != nil) { k = 0 }

		log.Printf("%s %s %s", i, j, k)



		reply_body = make(map[string]any)

		reply_body["value"] = float64(i)+j+k
		reply_body["type"] = "read_ok"

		// Echo the original message back with the updated message type.
		return n.Reply(msg, reply_body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
