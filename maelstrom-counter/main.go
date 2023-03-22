// This was a bit of a pain, mainly due to the key-value store being malicious.
// Not only may it return past values, it always will - it will never converge,
// no matter how long it waits.

// Other than that, it's pretty chill.  Each node has a particular key it
// encrements on send, and it keeps re-trying to set the value for a new one
// until it suceeds.  May have problems if nodes crash, but they don't here, so
// hey.  Also, even if they did, you can't really prevent mutliple increments,
// so probably still fine.

// To prevent the malicious KV behaviour, read() will contact other nodes in
// order to see what their local values are for their counters.  So that will
// converge, as long as you can eventually contact all nodes.  We cache the last
// value from each node, just in case.

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

	n.Handle("add", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		ctx, _ := context.WithTimeout(context.Background(),230 * time.Millisecond)

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		delta := body["delta"].(float64)

		// Update our local count
		count = count + delta

		for(true) {
			// Try writing to the KV store untli we succeed.
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
		return n.Reply(msg, reply_body)
	})

	n.Handle("local", func(msg maelstrom.Message) error {
		// Local message, outside of specification.  Read from our key and return the count from it.
		// Could probably just not do that, and return the local value.
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

		return n.Reply(msg, reply_body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
