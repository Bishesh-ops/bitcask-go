package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/bisheshops/bitcask-go/internal/raft"
	"github.com/bisheshops/bitcask-go/internal/wire"
)

func main() {
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Failed to establish TCP stream to server: %v", err)
	}
	defer conn.Close()

	fmt.Println("==================================================")
	fmt.Println(" PHASE 1: Executing Direct Key-Value Storage I/O  ")
	fmt.Println("==================================================")

	fmt.Println("[DB Client] Issuing PUT command: 'consensus' -> 'stabilized'")
	_ = wire.WriteFrame(conn, wire.CmdPut, []byte("consensus"), []byte("stabilized"))
	resp, _ := wire.ReadFrame(conn)
	fmt.Printf("[Server DB Reply] Status Engine: %s\n\n", string(resp.Key))

	fmt.Println("[DB Client] Issuing GET command: 'consensus'")
	_ = wire.WriteFrame(conn, wire.CmdGet, []byte("consensus"), nil)
	resp2, _ := wire.ReadFrame(conn)
	fmt.Printf("[Server DB Reply] Payload Value: %s\n\n", string(resp2.Value))

	time.Sleep(300 * time.Millisecond)
	fmt.Println("==================================================")
	fmt.Println(" PHASE 2: Injecting Dominant Heartbeat Frame      ")
	fmt.Println("==================================================")

	targetTerm := 100
	heartbeatArgs := raft.AppendEntriesArgs{
		Term:         targetTerm,
		LeaderID:     2,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: 0,
	}

	fmt.Printf("[Simulated Leader] Broadcasting initial AppendEntries: Term=%d\n", targetTerm)
	_ = wire.WriteFrame(conn, wire.CmdRaftAppendEntries, nil, heartbeatArgs.Encode())
	resp3, _ := wire.ReadFrame(conn)

	var activeServerTerm int

	if resp3.Cmd == wire.CmdRaftAppendEntriesReply {
		reply := raft.DecodeAppendEntriesReply(resp3.Value)
		activeServerTerm = reply.Term
		fmt.Printf("[Server Reply] Current Node Term: %d | Acknowledged: %t\n", reply.Term, reply.Success)

		// If the standalone server outpaced our initial term guess, dynamically seize dominance!
		if !reply.Success && reply.Term > targetTerm {
			fmt.Printf("Server term is higher (%d). Dynamically adapting to assert dominance...\n", reply.Term)

			targetTerm = reply.Term + 1
			heartbeatArgs.Term = targetTerm
			activeServerTerm = targetTerm // Update tracking term for Phase 3 checks

			fmt.Printf("[Simulated Leader] Re-broadcasting AppendEntries with overriding Term=%d\n", targetTerm)
			_ = wire.WriteFrame(conn, wire.CmdRaftAppendEntries, nil, heartbeatArgs.Encode())

			retryResp, _ := wire.ReadFrame(conn)
			retryReply := raft.DecodeAppendEntriesReply(retryResp.Value)

			fmt.Printf("[Server Overridden Reply] Node Term: %d | Acknowledged: %t\n", retryReply.Term, retryReply.Success)
			if retryReply.Success {
				fmt.Println("Dominance Successfully Asserted! Server accepted our dynamic term override and demoted to Follower.")
			}
		} else if reply.Success {
			fmt.Println("Dominance Asserted on first try!")
		}
	}

	time.Sleep(300 * time.Millisecond)
	fmt.Println()

	fmt.Println("==================================================")
	fmt.Println(" PHASE 3: Enforcing Consensus Safety Boundaries   ")
	fmt.Println("==================================================")

	staleTerm := activeServerTerm - 10
	if staleTerm < 0 {
		staleTerm = 0
	}

	staleVoteArgs := raft.RequestVoteArgs{
		Term:         staleTerm,
		CandidateID:  3,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}

	fmt.Printf("[Delayed Candidate] Requesting Vote with stale Term=%d\n", staleVoteArgs.Term)
	_ = wire.WriteFrame(conn, wire.CmdRaftRequestVote, nil, staleVoteArgs.Encode())
	resp4, _ := wire.ReadFrame(conn)

	if resp4.Cmd == wire.CmdRaftRequestVoteReply {
		reply := raft.DecodeRequestVoteReply(resp4.Value)
		fmt.Printf("[Server Consensus Reply] Node Current Term: %d | Vote Granted: %t\n", reply.Term, reply.VoteGranted)

		if !reply.VoteGranted && reply.Term >= activeServerTerm {
			fmt.Println("Consensus Secure! Server correctly rejected the stale candidate term to protect cluster state.")
		} else {
			fmt.Println("Boundary Validation failure.")
		}
	}
}
