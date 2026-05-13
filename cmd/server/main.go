package main

import (
	"fmt"
	"github.com/bisheshops/bitcask-go/internal/engine"
	"github.com/bisheshops/bitcask-go/internal/wire"
	"io"
	"log"
	"net"
)

func main() {
	db, err := engine.Open("./test_bitcask.db")
	if err != nil {
		log.Fatalf("Failed to open engine: %v", err)
	}
	defer db.Close()

	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	fmt.Println("Bitcask Server listening on :8080")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Accept error: %v", err)
			continue
		}
		go handleConnection(conn, db)
	}
}

func handleConnection(conn net.Conn, db *engine.DB) {
	defer conn.Close()
	fmt.Printf("New connection from %s\n", conn.RemoteAddr())

	for {
		frame, err := wire.ReadFrame(conn)
		if err == io.EOF {
			return
		}
		if err != nil {
			log.Printf("Read error: %v", err)
			return
		}

		switch frame.Cmd {
		case wire.CmdPut:
			if err := db.Put(frame.Key, frame.Value); err != nil {
				log.Printf("Put error: %v", err)
			}
			wire.WriteFrame(conn, wire.CmdPut, []byte("OK"), nil)
		case wire.CmdGet:
			val, err := db.Get(frame.Key)
			if err != nil {
				wire.WriteFrame(conn, wire.CmdGet, []byte("ERR"), []byte("Not Found"))
			} else {
				wire.WriteFrame(conn, wire.CmdGet, []byte("OK"), val)
			}
		}
	}
}
