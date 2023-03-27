// This is pretty simnple.  For a globally unique ID, we'll just use the
// hostname, time, and an incrementing ID, just in case.  As long as two nodes
// don't have the same name, and the nanoseconds aren't equal, we're good.  If
// we really needed it, we could add a mutex to make sure that the offset is the
// same.  This technically still isn't safe - the node could crash and be
// restarted multiple times - still probably good with the timestamp.  We're not
// dealing with node crashes in these problems, anyway.

package main

import (
	"log"
	"os"
	"strconv"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
	n := maelstrom.NewNode()
	id := 0

	n.Handle("generate", func(msg maelstrom.Message) error {
		var reply_body = make(map[string]any)
		reply_body["type"] = "generate_ok"

		nano := time.Now().UnixNano()
		reply_body["id"] = n.ID() + "-" + strconv.FormatInt(nano,10) + "-" + strconv.Itoa(id);

		id += 1

		return n.Reply(msg, reply_body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
