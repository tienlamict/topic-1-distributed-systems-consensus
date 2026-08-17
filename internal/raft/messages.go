package raft

// Các struct dưới đây bám sát Figure 2 của Ongaro & Ousterhout 2014
// (03_1_raft.pdf) và mục §3.1 trong tài liệu lý thuyết.
//
// Demo này CỐ Ý không cài đặt InstallSnapshot RPC (log compaction) — xem phần
// "Phạm vi" trong docs/demo-proposal_v1.md.

// Entry là một mục trong replicated log.
type Entry struct {
	Term int    `json:"term"`
	Cmd  string `json:"cmd"`
	// WID là id của client write sinh ra entry này, dùng để đối chiếu
	// write đã được ack hay chưa. 0 nghĩa là entry nội bộ.
	WID int `json:"wid,omitempty"`
}

// RequestVote — invoked bởi candidate để gom phiếu (§3.1.1).
type RequestVote struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
	// Reason chỉ để hiển thị trên UI, không có trong protocol thật.
	Reason string `json:"reason,omitempty"`
}

// AppendEntries — replicate log entries; entries rỗng chính là heartbeat (§3.1.2).
type AppendEntries struct {
	Term         int     `json:"term"`
	LeaderID     string  `json:"leaderId"`
	PrevLogIndex int     `json:"prevLogIndex"`
	PrevLogTerm  int     `json:"prevLogTerm"`
	Entries      []Entry `json:"entries"`
	LeaderCommit int     `json:"leaderCommit"`
}

type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
	// MatchIndex không có trong Figure 2 (ở đó leader tự suy ra từ
	// prevLogIndex + len(entries)). Gửi kèm cho gọn và tránh nhầm khi
	// reply về trễ/không đúng thứ tự.
	MatchIndex int `json:"matchIndex"`
}

const (
	TypeRequestVote      = "RequestVote"
	TypeRequestVoteReply = "RequestVoteReply"
	TypeAppendEntries    = "AppendEntries"
	TypeAppendReply      = "AppendEntriesReply"
)
