package raft

import (
	"encoding/binary"
)

// RequestVoteArgs is sent by candidates to request votes.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// Encode packs integer fields into a contiguous 32-byte Little Endian buffer.
func (args *RequestVoteArgs) Encode() []byte {
	buf := make([]byte, 32)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(args.Term))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(args.CandidateID))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(args.LastLogIndex))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(args.LastLogTerm))
	return buf
}

func DecodeRequestVoteArgs(buf []byte) RequestVoteArgs {
	return RequestVoteArgs{
		Term:         int(binary.LittleEndian.Uint64(buf[0:8])),
		CandidateID:  int(binary.LittleEndian.Uint64(buf[8:16])),
		LastLogIndex: int(binary.LittleEndian.Uint64(buf[16:24])),
		LastLogTerm:  int(binary.LittleEndian.Uint64(buf[24:32])),
	}
}

// RequestVoteReply is sent by peers in response to a vote request.
type RequestVoteReply struct {
	Term        int  // Current Term of candidate so it can update itself
	VoteGranted bool // True means the candidate received the vote
}

func (reply *RequestVoteReply) Encode() []byte {
	buf := make([]byte, 9) // 8 bytes for Term + 1 byte boolean flag
	binary.LittleEndian.PutUint64(buf[0:8], uint64(reply.Term))
	if reply.VoteGranted {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	return buf
}

func DecodeRequestVoteReply(buf []byte) RequestVoteReply {
	granted := false
	if buf[8] == 1 {
		granted = true
	}
	return RequestVoteReply{
		Term:        int(binary.LittleEndian.Uint64(buf[0:8])),
		VoteGranted: granted,
	}
}

// AppendEntriesArgs serves log transmission and Heartbeats.
type AppendEntriesArgs struct {
	Term         int // Leader's term
	LeaderID     int // So followers can redirect clients
	PrevLogTerm  int
	PrevLogIndex int
	Entries      [][]byte
	LeaderCommit int
}

func (args *AppendEntriesArgs) Encode() []byte {
	totalLen := 40 + 4 // 5 * 8-byte integers + 4-byte slice length counter
	for _, entry := range args.Entries {
		totalLen += 4 + len(entry) // 4 bytes for length prefix + actual payload size
	}
	buf := make([]byte, totalLen)

	binary.LittleEndian.PutUint64(buf[0:8], uint64(args.Term))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(args.LeaderID))
	binary.LittleEndian.PutUint64(buf[16:24], uint64(args.PrevLogTerm))
	binary.LittleEndian.PutUint64(buf[24:32], uint64(args.PrevLogIndex))
	binary.LittleEndian.PutUint64(buf[32:40], uint64(args.LeaderCommit))
	binary.LittleEndian.PutUint32(buf[40:44], uint32(len(args.Entries)))

	offset := 44
	for _, entry := range args.Entries {
		binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(entry)))
		offset += 4
		copy(buf[offset:offset+len(entry)], entry)
		offset += len(entry)
	}

	return buf
}

// AppendEntriesReply is returned by followers after processing replication.
type AppendEntriesReply struct {
	Term    int
	Success bool
}

// DecodeAppendEntriesArgs unpacks raw bytes back into the Go struct safely.
func DecodeAppendEntriesArgs(payload []byte) AppendEntriesArgs {
	args := AppendEntriesArgs{
		Term:         int(binary.LittleEndian.Uint64(payload[0:8])),
		LeaderID:     int(binary.LittleEndian.Uint64(payload[8:16])),
		PrevLogTerm:  int(binary.LittleEndian.Uint64(payload[16:24])),
		PrevLogIndex: int(binary.LittleEndian.Uint64(payload[24:32])),
		LeaderCommit: int(binary.LittleEndian.Uint64(payload[32:40])),
	}

	numEntries := int(binary.LittleEndian.Uint32(payload[40:44]))
	args.Entries = make([][]byte, numEntries)

	offset := 44
	for i := range numEntries {
		entryLen := int(binary.LittleEndian.Uint32(payload[offset : offset+4]))
		offset += 4

		// Allocate dedicated memory space for individual log entry slices
		entryBuf := make([]byte, entryLen)
		copy(entryBuf, payload[offset:offset+entryLen])
		args.Entries[i] = entryBuf

		offset += entryLen
	}

	return args
}

// Encode packs the heartbeat execution response cleanly.
func (reply *AppendEntriesReply) Encode() []byte {
	buf := make([]byte, 9) // 8 bytes Term + 1 byte success flag
	binary.LittleEndian.PutUint64(buf[0:8], uint64(reply.Term))
	if reply.Success {
		buf[8] = 1
	} else {
		buf[8] = 0
	}
	return buf
}

// DecodeAppendEntriesReply extracts the peer response parameters.
func DecodeAppendEntriesReply(payload []byte) AppendEntriesReply {
	success := false
	if payload[8] == 1 {
		success = true
	}
	return AppendEntriesReply{
		Term:    int(binary.LittleEndian.Uint64(payload[0:8])),
		Success: success,
	}
}
