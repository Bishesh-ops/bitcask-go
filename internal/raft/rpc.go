package raft

// Request sent by candidates to request for votes.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// Response sent by the peers
type ResponseVoteReply struct {
	Term        int  // This Term of candidate currently so it can update it
	VoteGranted bool // Wheather or not the candidate was voted for True meaning it received the vote.
}

// AppendEntriesArgs  serves two purposes: transmitting log entries, and acting as Heartbeats.
type AppendEntriesArgs struct {
	Term         int // Leader's term
	LeaderID     int // So followers can redirect the clients
	PrevLogIndex int
	PrevLogTerm  int
	Entries      [][]byte
	LeaderCommit int
}

// AppendEntriesReply is retured by followers after processing relication.
type AppendEntriesReply struct {
	Term    int
	Success bool
}
