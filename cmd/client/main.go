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
	// Connect to our running hybrid server/node on port 8080
	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		log.Fatalf("Failed to connect to cluster node: %v", err)
	}
	defer conn.Close()

	fmt.Println("==================================================")
	fmt.Println(" PHASE 1: Testing Direct Bitcask Storage Engine   ")
	fmt.Println("==================================================")

	// Send a standard database PUT command
	fmt.Println("[Client] Sending PUT: 'educational' -> 'journey'")
	err = wire.WriteFrame(conn, wire.CmdPut, []byte("educational"), []byte("journey"))
	if err != nil {
		log.Fatalf("Write PUT failed: %v", err)
	}

	resp, err := wire.ReadFrame(conn)
	if err != nil {
		log.Fatalf("Read PUT response failed: %v", err)
	}
	fmt.Printf("[Server DB Reply] Status: %s\n\n", string(resp.Key))

	// Send a standard database GET command
	fmt.Println("[Client] Sending GET: 'educational'")
	err = wire.WriteFrame(conn, wire.CmdGet, []byte("educational"), nil)
	if err != nil {
		log.Fatalf("Write GET failed: %v", err)
	}

	resp2, err := wire.ReadFrame(conn)
	if err != nil {
		log.Fatalf("Read GET response failed: %v", err)
	}
	fmt.Printf("[Server DB Reply] Value returned: %s\n\n", string(resp2.Value))

	// Give the terminal output a tiny breathing buffer
	time.Sleep(500 * time.Millisecond)

	fmt.Println("==================================================")
	fmt.Println(" PHASE 2: Simulating Internal Raft Peer Traffic   ")
	fmt.Println("==================================================")

	// Let's pretend this client is Candidate Node ID 2 operating in Term 5.
	// We are going to ask the server (Node ID 1) to grant us its vote.
	voteReq := raft.RequestVoteArgs{
		Term:         5,
		CandidateID:  2,
		LastLogIndex: 10,
		LastLogTerm:  4,
	}

	fmt.Printf("[Simulated Peer] Broadcasting RequestVote: CandidateID=%d, Term=%d\n", voteReq.CandidateID, voteReq.Term)

	// Explicitly pack our fields into raw Little Endian bytes using our domain logic
	payload := voteReq.Encode()

	// Push the consensus packet over the wire using our dedicated Raft opcode
	err = wire.WriteFrame(conn, wire.CmdRaftRequestVote, nil, payload)
	if err != nil {
		log.Fatalf("Write RequestVote frame failed: %v", err)
	}

	// Read back the node's synchronized consensus state machine reply
	resp3, err := wire.ReadFrame(conn)
	if err != nil {
		log.Fatalf("Read RequestVote reply failed: %v", err)
	}

	// Verify the routing layer multiplexed the correct response opcode
	if resp3.Cmd == wire.CmdRaftRequestVoteReply {
		// Unpack the contiguous raw byte array back into our Go struct
		reply := raft.DecodeRequestVoteReply(resp3.Value)
		fmt.Printf("[Server Consensus Reply] Node Term: %d | Vote Granted: %t\n", reply.Term, reply.VoteGranted)

		if reply.VoteGranted {
			fmt.Println("The server node evaluated our term logic and granted us its vote.")
		} else {
			fmt.Println("Status: Vote denied (Server term might be higher, or it already voted).")
		}
	} else {
		fmt.Printf("[Error] Received unexpected opcode frame: %d\n", resp3.Cmd)
	}
}
