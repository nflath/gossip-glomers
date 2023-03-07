package main

import (
	"encoding/json"
	"log"
"strconv"
	"os"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	pid := os.Getpid()

	n := maelstrom.NewNode()
	i := 0

	// Register a handler for the "echo" message that responds with an "echo_ok".
	n.Handle("generate", func(msg maelstrom.Message) error {
		// Unmarshal the message body as an loosely-typed map.
		var reply_body = make(map[string]any)


		reply_body["type"] = "generate_ok"

		nano := now.Now().UnixNano()
		body["id"] = n.ID() + "-" + strconv.FormatInt(nano,10) + "-" + strconv.Itoa(i);

		i += 1

		return n.Reply(msg, body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
