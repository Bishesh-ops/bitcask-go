package main

import (
	"fmt"
	"github.com/bisheshops/bitcask-go/internal/wire"
	"net"
)

func main() {
	conn, _ := net.Dial("tcp", "localhost:8080")
	defer conn.Close()

	// Send a PUT command
	fmt.Println("Sending PUT: 'hello' -> 'world'")
	wire.WriteFrame(conn, wire.CmdPut, []byte("hello"), []byte("world"))

	// Read response
	resp, _ := wire.ReadFrame(conn)
	fmt.Printf("Server responded: %s\n", string(resp.Key))

	// Send a GET command
	fmt.Println("Sending GET: 'hello'")
	wire.WriteFrame(conn, wire.CmdGet, []byte("hello"), nil)

	// Read response
	resp2, _ := wire.ReadFrame(conn)
	fmt.Printf("Server returned value: %s\n", string(resp2.Value))
}
