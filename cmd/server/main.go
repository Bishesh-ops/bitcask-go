package main

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/bisheshops/bitcask-go/internal/engine"
	"github.com/bisheshops/bitcask-go/internal/raft"
	"github.com/bisheshops/bitcask-go/internal/wire"
)

func main() {
	db, err := engine.Open("./test_bitcask.db")
	if err != nil {
		log.Fatalf("Failed to open engine: %v", err)
	}
	defer db.Close()

	node := raft.NewNode(1, []string{"localhost:8081", "localhost:8082"}, db)

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	fmt.Println("Bitcask Consensus Cluster Node booted on :8080")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go handleConnection(conn, db, node)
	}
}

func handleConnection(conn net.Conn, db *engine.DB, node *raft.Node) {
	defer conn.Close()
	fmt.Printf("Connection interface established: %s\n", conn.RemoteAddr())

	for {
		frame, err := wire.ReadFrame(conn)
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Printf("Socket closed or corrupted frame read: %v", err)
			return
		}

		switch frame.Cmd {
		// --- Standard Bitcask Direct Engine Operations ---
		case wire.CmdPut:
			if err := db.Put(frame.Key, frame.Value); err != nil {
				log.Printf("Engine level direct append error: %v", err)
			}
			wire.WriteFrame(conn, wire.CmdPut, []byte("OK"), nil)

		case wire.CmdGet:
			val, err := db.Get(frame.Key)
			if err != nil {
				wire.WriteFrame(conn, wire.CmdGet, []byte("ERR"), []byte("Not Found"))
			} else {
				wire.WriteFrame(conn, wire.CmdGet, []byte("OK"), val)
			}

		// --- Raft Cluster Consensus Communication Multiplexer ---
		case wire.CmdRaftRequestVote:
			args := raft.DecodeRequestVoteArgs(frame.Value)
			reply := node.HandleRequestVote(args)
			respPayload := reply.Encode()
			wire.WriteFrame(conn, wire.CmdRaftRequestVoteReply, nil, respPayload)
		}
	}
}
