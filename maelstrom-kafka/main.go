// :error "java.util.concurrent.ExecutionException: clojure.lang.ExceptionInfo: throw+: {:type :no-writer-of-value, :key \"9\", :value 0} {:type :no-writer-of-value, :key \"9\", :value 0}\n at java.util.concurrent.FutureTask.report (FutureTask.java:122)\n    java.util.concurrent.FutureTask.get (FutureTask.java:191)\n    clojure.core$deref_future.invokeStatic (core.clj:2317)\n    clojure.core$future_call$reify__8544.deref (core.clj:7041)\n    clojure.core$deref.invokeStatic (core.clj:2337)\n    clojure.core$deref.invoke (core.clj:2323)\n    jepsen.tests.kafka$analysis.invokeStatic (kafka.clj:1977)\n    jepsen.tests.kafka$analysis.invoke (kafka.clj:1879)\n    jepsen.tests.kafka$checker$reify__19270.check (kafka.clj:2055)\n    jepsen.checker$check_safe.invokeStatic (checker.clj:86)\n    jepsen.checker$check_safe.invoke (checker.clj:79)\n    jepsen.checker$compose$reify__11881$fn__11883.invoke (checker.clj:102)\n    clojure.core$pmap$fn__8552$fn__8553.invoke (core.clj:7089)\n    clojure.core$binding_conveyor_fn$fn__5823.invoke (core.clj:2047)\n    clojure.lang.AFn.call (AFn.java:18)\n    java.util.concurrent.FutureTask.run (FutureTask.java:317)\n    java.util.concurrent.ThreadPoolExecutor.runWorker (ThreadPoolExecutor.java:1144)\n    java.util.concurrent.ThreadPoolExecutor$Worker.run (ThreadPoolExecutor.java:642)\n    java.lang.Thread.run (Thread.java:1589)\nCaused by: clojure.lang.ExceptionInfo: throw+: {:type :no-writer-of-value, :key \"9\", :value 0}\n{:type :no-writer-of-value, :key \"9\", :value 0}\n at slingshot.support$stack_trace.invoke (support.clj:201)\n    jepsen.tests.kafka$wr_graph$reduce_iter_0__19204$reduce_iter_1__19209.invoke (kafka.clj:1842)\n    clojure.core.protocols$iter_reduce.invokeStatic (protocols.clj:49)\n    clojure.core.protocols$fn__8230.invokeStatic (protocols.clj:75)\n    clojure.core.protocols/fn (protocols.clj:75)\n    clojure.core.protocols$fn__8178$G__8173__8191.invoke (protocols.clj:13)\n    clojure.core$reduce.invokeStatic (core.clj:6886)\n    clojure.core$reduce.invoke (core.clj:6868)\n    jepsen.tests.kafka$wr_graph$reduce_iter_0__19204.invoke (kafka.clj:1842)\n    clojure.core.protocols$iter_reduce.invokeStatic (protocols.clj:49)\n    clojure.core.protocols$fn__8230.invokeStatic (protocols.clj:75)\n    clojure.core.protocols/fn (protocols.clj:75)\n    clojure.core.protocols$fn__8178$G__8173__8191.invoke (protocols.clj:13)\n    clojure.core$reduce.invokeStatic (core.clj:6886)\n    clojure.core$reduce.invoke (core.clj:6868)\n    jepsen.tests.kafka$wr_graph.invokeStatic (kafka.clj:1842)\n    jepsen.tests.kafka$wr_graph.invoke (kafka.clj:1838)\n    clojure.core$partial$fn__5908.invoke (core.clj:2641)\n    elle.core$combine$analyze__17049$launch_analysis__17050$task__17051.invoke (core.clj:189)\n    jepsen.history.task.Task.run (task.clj:282)\n    java.util.concurrent.ThreadPoolExecutor.runWorker (ThreadPoolExecutor.java:1144)\n    java.util.concurrent.ThreadPoolExecutor$Worker.run (ThreadPoolExecutor.java:642)\n    java.lang.Thread.run (Thread.java:1589)\n"},
// WTF does this mean

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

	var next_offset = make(map[string]float64)
	var committed_offset = make(map[string]float64)
	node_log := make(map[string]map[float64]float64);

	n.Handle("send", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		key := body["key"].(string)
		msg_ := body["msg"].(float64)

		_, ok := next_offset[key]
		if (ok == false) {
			next_offset[key] = 0
			//committed_offset[key] = 0
			node_log[key] = make(map[float64]float64)
		}
		offset := next_offset[key]

		next_offset[key] = next_offset[key]+1
		node_log[key][offset] = msg_


		reply_body := make(map[string]any)
		reply_body["type"] = "send_ok"
		reply_body["offset"] = offset
		return n.Reply(msg, reply_body)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()
		
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
		 	return err
		}
		//keys := body["keys"].([]interface{})
		
		var offsets map[string]interface{} = body["offsets"].(map[string]interface{})
		
		reply_body := make(map[string]any)
		reply_body["type"] = "poll_ok"
		reply_body["msgs"] = make(map[string][][]float64)
		for key, val := range offsets {
			
			_, ok := node_log[key]
			if (ok == false) {
				continue
			}
			_, ok = node_log[key][val.(float64)]
			if (ok == false) {
				continue
			}
			reply_body["msgs"].(map[string][][]float64)[key] = [][]float64{[]float64{val.(float64), node_log[key][val.(float64)]}}
		}
		
		return n.Reply(msg, reply_body)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
		 	return err
		}
		
		var offsets map[string]interface{} = body["offsets"].(map[string]interface{})
		
		for key, val := range offsets {
			committed_offset[key] = val.(float64)
		}
		
		reply_body := make(map[string]any)
		 reply_body["type"] = "commit_offsets_ok"

		return n.Reply(msg, reply_body)
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		mu.Lock()
		defer mu.Unlock()

		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
		 	return err
		}
		keys := body["keys"].([]interface{})

		var reply_body = make(map[string]any)
		reply_body["type"] = "list_committed_offsets_ok"
		
		reply_body["offsets"] = make(map[string]float64)
		for _, key := range keys {
			_, ok := committed_offset[key.(string)]
			if ok {
				reply_body["offsets"].(map[string]float64)[key.(string)] = committed_offset[key.(string)]
			}
		}
		
		return n.Reply(msg, reply_body)
	})

	// Execute the node's message loop. This will run until STDIN is closed.
	if err := n.Run(); err != nil {
		log.Printf("ERROR: %s", err)
		os.Exit(1)
	}
}
