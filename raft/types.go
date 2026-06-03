package raft

// ============================================================================
// types.go — Các kiểu dữ liệu nền tảng của Raft
//
// Liên hệ lý thuyết (file topic-1-distributed-systems-consensus.md):
//   - §2.1  Server States và Terms
//   - §3.1  Raft RPCs (RequestVote, AppendEntries)
//   - §1.3  Replicated State Machine (Log + State Machine)
// ============================================================================

// State — một server tại mỗi thời điểm ở đúng MỘT trong ba trạng thái.
// Liên hệ §2.1: "Tại mỗi thời điểm, mỗi server ở một trong ba trạng thái".
type State int

const (
	Follower  State = iota // passive, chỉ respond request từ leader/candidate
	Candidate              // dùng để elect leader mới
	Leader                 // xử lý mọi client request
)

func (s State) String() string {
	switch s {
	case Follower:
		return "FOLLOWER"
	case Candidate:
		return "CANDIDATE"
	case Leader:
		return "LEADER"
	default:
		return "UNKNOWN"
	}
}

// LogEntry — một mục trong replicated log.
// Liên hệ §1.3: "Log: chuỗi command (ví dụ: set x to 3)".
// Mỗi entry mang theo Term (logical clock) để phục vụ Log Matching Property (§5.2).
type LogEntry struct {
	Term    int    // term của leader tại thời điểm tạo entry này
	Command string // command áp vào state machine, ví dụ "set x = 3"
}

// ----------------------------------------------------------------------------
// RequestVote RPC — §3.1.1
// Invoked bởi candidate để gather votes trong leader election.
// ----------------------------------------------------------------------------

type RequestVoteArgs struct {
	Term         int // term của candidate
	CandidateID  int // id candidate đang xin vote
	LastLogIndex int // index của last log entry của candidate
	LastLogTerm  int // term của last log entry (phục vụ "up-to-date" check, §2.2)
}

type RequestVoteReply struct {
	Term        int  // currentTerm của receiver, để candidate tự cập nhật
	VoteGranted bool // true nếu candidate nhận được vote
}

// ----------------------------------------------------------------------------
// AppendEntries RPC — §3.1.2
// Invoked bởi leader để replicate log entries; entries rỗng = heartbeat.
// ----------------------------------------------------------------------------

type AppendEntriesArgs struct {
	Term         int        // term của leader
	LeaderID     int        // để follower biết redirect client về đâu
	PrevLogIndex int        // index của entry ngay trước các entries mới
	PrevLogTerm  int        // term của entry tại PrevLogIndex
	Entries      []LogEntry // entries cần lưu (rỗng khi là heartbeat)
	LeaderCommit int        // commitIndex của leader
}

type AppendEntriesReply struct {
	Term    int  // currentTerm của receiver
	Success bool // true nếu follower khớp PrevLogIndex/PrevLogTerm
}
