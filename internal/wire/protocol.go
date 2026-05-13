package wire

import (
	"encoding/binary"
	"io"
)

const (
	// Client Storage Engine Commands
	CmdPut uint8 = 1
	CmdGet uint8 = 2

	// Raft Consensus Peer Multiplexer Commands
	CmdRaftRequestVote        uint8 = 10
	CmdRaftRequestVoteReply   uint8 = 11
	CmdRaftAppendEntries      uint8 = 12
	CmdRaftAppendEntriesReply uint8 = 13
)

type Frame struct {
	Cmd   uint8
	Key   []byte
	Value []byte
}

func ReadFrame(r io.Reader) (*Frame, error) {
	// Read the fixed 7-byte header first to determine dynamic payload boundaries
	header := make([]byte, 7)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	cmd := header[0]
	keyLen := binary.BigEndian.Uint16(header[1:3])
	valLen := binary.BigEndian.Uint32(header[3:7])

	payload := make([]byte, uint32(keyLen)+valLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}
	return &Frame{
		Cmd:   cmd,
		Key:   payload[:keyLen],
		Value: payload[keyLen:],
	}, nil
}

// WriteFrame packs a command opcode, key, and value payload safely over the socket stream
func WriteFrame(w io.Writer, cmd uint8, key, value []byte) error {
	keyLen := len(key)
	valLen := len(value)

	buf := make([]byte, 7+keyLen+valLen)

	buf[0] = cmd
	binary.BigEndian.PutUint16(buf[1:3], uint16(keyLen))
	binary.BigEndian.PutUint32(buf[3:7], uint32(valLen))

	copy(buf[7:7+keyLen], key)
	copy(buf[7+keyLen:], value)

	_, err := w.Write(buf)
	return err
}
