package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ============================================================================
// node.go — Lõi thuật toán Raft cho một server.
//
// Cài đặt 3 sub-problem của Raft (§1.4):
//   1. Leader election  (§2.2, §4.1) — randomized election timeout
//   2. Log replication  (§2.2)        — AppendEntries RPC
//   3. Safety           (§5.2)        — up-to-date check, commit current-term only
//
// LƯU Ý VỀ TIMING (§4.1): Paper khuyến nghị election timeout 150–300ms,
// heartbeat ~50ms. Ở demo này ta CỐ Ý phóng đại lên (election 1000–2000ms,
// heartbeat 300ms) để mắt người đọc kịp theo dõi log. Quan hệ
// broadcastTime ≪ electionTimeout ≪ MTBF (§4.1) vẫn được giữ nguyên.
// ============================================================================

const (
	heartbeatInterval = 300 * time.Millisecond  // leader gửi heartbeat mỗi 300ms
	electionTimeoutMin = 1000 * time.Millisecond // cận dưới randomized timeout
	electionTimeoutMax = 2000 * time.Millisecond // cận trên randomized timeout
	tickInterval       = 20 * time.Millisecond   // chu kỳ kiểm tra của vòng lặp chính
)

var startTime = time.Now()

type Node struct {
	id    int
	peers []int // id của các node khác trong cluster
	net   *Network

	mu sync.Mutex

	// --- Persistent state (trên server thật phải ghi xuống stable storage) ---
	currentTerm int        // term hiện tại, tăng monotonically (§2.1 logical clock)
	votedFor    int        // candidate đã vote trong term này; -1 = chưa vote
	log         []LogEntry // log[0] là sentinel; entry thật bắt đầu từ index 1

	// --- Volatile state ---
	state       State
	commitIndex int       // index cao nhất đã biết được committed
	lastApplied int       // index cao nhất đã apply vào state machine
	lastHeard   time.Time // lần cuối nhận liên lạc hợp lệ từ leader / cấp vote
	electTimeout time.Duration
	lastHbSent  time.Time // leader: lần cuối gửi heartbeat

	// --- Leader-only volatile state (reset sau mỗi election) ---
	nextIndex  map[int]int // với mỗi peer: index entry kế tiếp sẽ gửi
	matchIndex map[int]int // với mỗi peer: index cao nhất đã replicate chắc chắn

	dead bool // mô phỏng node bị crash / tắt máy

	stateMachine map[string]string // state machine: kết quả apply command
}

func NewNode(id int, peers []int, net *Network) *Node {
	n := &Node{
		id:           id,
		peers:        peers,
		net:          net,
		currentTerm:  0,
		votedFor:     -1,
		log:          []LogEntry{{Term: 0, Command: "<sentinel>"}}, // index 0
		state:        Follower,
		commitIndex:  0,
		lastApplied:  0,
		stateMachine: make(map[string]string),
		nextIndex:    make(map[int]int),
		matchIndex:   make(map[int]int),
	}
	n.resetElectionTimeout()
	net.addNode(n)
	return n
}

// Start: khởi động vòng lặp chính của node trong một goroutine.
func (n *Node) Start() {
	go n.loop()
}

// ----------------------------------------------------------------------------
// Tiện ích
// ----------------------------------------------------------------------------

func (n *Node) isDead() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.dead
}

// Kill: mô phỏng node crash — ngừng respond mọi RPC.
func (n *Node) Kill() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dead = true
	n.logLocked("đã bị KILL (mô phỏng crash)")
}

// Revive: node khởi động lại, quay về Follower (state volatile mất, nhưng
// currentTerm/votedFor/log giả định khôi phục từ stable storage — §1.2).
func (n *Node) Revive() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.dead = false
	n.state = Follower
	n.resetElectionTimeout()
	n.logLocked("đã REVIVE — quay lại FOLLOWER")
}

func (n *Node) resetElectionTimeout() {
	d := electionTimeoutMax - electionTimeoutMin
	n.electTimeout = electionTimeoutMin + time.Duration(rand.Int63n(int64(d)))
	n.lastHeard = time.Now()
}

func (n *Node) clusterSize() int { return len(n.peers) + 1 }

func (n *Node) majority() int { return n.clusterSize()/2 + 1 }

func (n *Node) lastLogIndex() int { return len(n.log) - 1 }

func (n *Node) lastLogTerm() int { return n.log[len(n.log)-1].Term }

// logLocked: in log có timestamp tương đối. Gọi khi ĐANG giữ n.mu.
func (n *Node) logLocked(format string, a ...interface{}) {
	elapsed := time.Since(startTime).Seconds()
	msg := fmt.Sprintf(format, a...)
	fmt.Printf("[+%6.2fs][node %d][term %2d][%-9s] %s\n",
		elapsed, n.id, n.currentTerm, n.state, msg)
}

// ----------------------------------------------------------------------------
// Vòng lặp chính — điều phối theo trạng thái (§2.1)
// ----------------------------------------------------------------------------

func (n *Node) loop() {
	for {
		time.Sleep(tickInterval)
		n.mu.Lock()
		if n.dead {
			n.mu.Unlock()
			continue
		}
		switch n.state {
		case Follower, Candidate:
			// Hết election timeout mà không nghe gì từ leader → khởi động election (§4.1)
			if time.Since(n.lastHeard) >= n.electTimeout {
				n.mu.Unlock()
				n.startElection()
				continue
			}
		case Leader:
			// Leader gửi heartbeat định kỳ để duy trì authority (§2.2)
			if time.Since(n.lastHbSent) >= heartbeatInterval {
				n.lastHbSent = time.Now()
				n.mu.Unlock()
				n.broadcastAppendEntries()
				continue
			}
		}
		n.mu.Unlock()
	}
}

// ----------------------------------------------------------------------------
// Leader Election (§2.2, §4.1)
// ----------------------------------------------------------------------------

func (n *Node) startElection() {
	n.mu.Lock()
	n.state = Candidate
	n.currentTerm++       // (1) tăng term
	n.votedFor = n.id     // (2) vote cho chính mình
	n.resetElectionTimeout()
	term := n.currentTerm
	lastIdx := n.lastLogIndex()
	lastTerm := n.lastLogTerm()
	n.logLocked("election timeout → trở thành CANDIDATE, xin vote (vote cho chính mình)")
	n.mu.Unlock()

	votes := 1 // đã có 1 vote của chính mình

	// (3) Gửi RequestVote RPC song song tới mọi peer
	for _, peer := range n.peers {
		go func(peer int) {
			args := RequestVoteArgs{
				Term:         term,
				CandidateID:  n.id,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply, ok := n.net.sendRequestVote(n.id, peer, args)
			if !ok {
				return // gói rớt (partition / node chết)
			}

			n.mu.Lock()
			defer n.mu.Unlock()
			// Reply đến muộn, ta đã đổi term/state → bỏ qua
			if n.state != Candidate || n.currentTerm != term {
				return
			}
			// Phát hiện term của mình stale → lùi về follower (§2.1)
			if reply.Term > n.currentTerm {
				n.becomeFollowerLocked(reply.Term)
				return
			}
			if reply.VoteGranted {
				votes++
				n.logLocked("nhận vote từ node %d (%d/%d)", peer, votes, n.clusterSize())
				if votes >= n.majority() {
					n.becomeLeaderLocked()
				}
			}
		}(peer)
	}
}

func (n *Node) becomeFollowerLocked(term int) {
	if term > n.currentTerm {
		n.votedFor = -1 // term mới → được vote lại
	}
	n.currentTerm = term
	if n.state != Follower {
		n.logLocked("lùi về FOLLOWER (term %d)", term)
	}
	n.state = Follower
	n.resetElectionTimeout()
}

func (n *Node) becomeLeaderLocked() {
	if n.state == Leader {
		return
	}
	n.state = Leader
	n.logLocked(">>> THẮNG ELECTION — trở thành LEADER của term %d <<<", n.currentTerm)
	// Khởi tạo nextIndex/matchIndex cho mọi follower
	for _, peer := range n.peers {
		n.nextIndex[peer] = n.lastLogIndex() + 1
		n.matchIndex[peer] = 0
	}
	n.lastHbSent = time.Time{} // buộc gửi heartbeat ngay ở tick kế tiếp
}

// ----------------------------------------------------------------------------
// Log Replication + Heartbeat (§2.2, §3.1.2)
// ----------------------------------------------------------------------------

// Submit: client gửi command tới node. Chỉ leader chấp nhận; follower từ chối
// (thực tế follower sẽ redirect về leader — §2.1). Trả về (index, isLeader).
func (n *Node) Submit(command string) (int, bool) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader {
		return -1, false
	}
	entry := LogEntry{Term: n.currentTerm, Command: command}
	n.log = append(n.log, entry)
	idx := n.lastLogIndex()
	n.logLocked("CLIENT command \"%s\" → append vào log tại index %d", command, idx)
	return idx, true
}

func (n *Node) broadcastAppendEntries() {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return
	}
	term := n.currentTerm
	n.mu.Unlock()

	for _, peer := range n.peers {
		go n.replicateTo(peer, term)
	}
}

func (n *Node) replicateTo(peer, term int) {
	n.mu.Lock()
	if n.state != Leader || n.currentTerm != term {
		n.mu.Unlock()
		return
	}
	prevLogIndex := n.nextIndex[peer] - 1
	if prevLogIndex < 0 {
		prevLogIndex = 0
	}
	prevLogTerm := n.log[prevLogIndex].Term
	entries := append([]LogEntry(nil), n.log[prevLogIndex+1:]...) // copy phần còn thiếu
	args := AppendEntriesArgs{
		Term:         term,
		LeaderID:     n.id,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	reply, ok := n.net.sendAppendEntries(n.id, peer, args)
	if !ok {
		return
	}

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state != Leader || n.currentTerm != term {
		return
	}
	if reply.Term > n.currentTerm {
		n.becomeFollowerLocked(reply.Term)
		return
	}
	if reply.Success {
		n.matchIndex[peer] = prevLogIndex + len(entries)
		n.nextIndex[peer] = n.matchIndex[peer] + 1
		n.advanceCommitLocked()
	} else {
		// Follower không khớp → lùi nextIndex, thử lại ở vòng sau (§3.1.2 rule 2)
		if n.nextIndex[peer] > 1 {
			n.nextIndex[peer]--
		}
	}
}

// advanceCommitLocked: leader nâng commitIndex khi một entry đã được replicate
// trên majority. AN TOÀN: chỉ commit entry thuộc TERM HIỆN TẠI (§2.2 safety).
func (n *Node) advanceCommitLocked() {
	for idx := n.lastLogIndex(); idx > n.commitIndex; idx-- {
		if n.log[idx].Term != n.currentTerm {
			continue // không commit entry của term cũ bằng cách đếm replica
		}
		count := 1 // leader đã có entry này
		for _, peer := range n.peers {
			if n.matchIndex[peer] >= idx {
				count++
			}
		}
		if count >= n.majority() {
			n.commitIndex = idx
			n.logLocked("entry index %d đã COMMITTED (replicate trên %d/%d node)",
				idx, count, n.clusterSize())
			n.applyLocked()
			break
		}
	}
}

// applyLocked: apply các entry [lastApplied+1 .. commitIndex] vào state machine.
func (n *Node) applyLocked() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		cmd := n.log[n.lastApplied].Command
		n.applyCommand(cmd)
		n.logLocked("APPLY index %d: \"%s\" → state machine = %v",
			n.lastApplied, cmd, n.stateMachine)
	}
}

// applyCommand: parse command dạng "set k v" và áp vào state machine.
func (n *Node) applyCommand(cmd string) {
	var k, v string
	if _, err := fmt.Sscanf(cmd, "set %s %s", &k, &v); err == nil {
		n.stateMachine[k] = v
	}
}

// ----------------------------------------------------------------------------
// RPC Handlers (phía receiver) — theo đúng Receiver rules trong Figure 2
// ----------------------------------------------------------------------------

// handleRequestVote — §3.1.1 Receiver rules
func (n *Node) handleRequestVote(args RequestVoteArgs) RequestVoteReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := RequestVoteReply{Term: n.currentTerm, VoteGranted: false}

	// Rule 1: reply false nếu term của candidate cũ hơn ta
	if args.Term < n.currentTerm {
		return reply
	}
	// Thấy term lớn hơn → cập nhật và lùi về follower (§2.1)
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	reply.Term = n.currentTerm

	// Rule 2: chưa vote (hoặc đã vote cho chính candidate này) VÀ log candidate
	// "at least as up-to-date" → grant (§2.2 safety restriction)
	upToDate := args.LastLogTerm > n.lastLogTerm() ||
		(args.LastLogTerm == n.lastLogTerm() && args.LastLogIndex >= n.lastLogIndex())
	if (n.votedFor == -1 || n.votedFor == args.CandidateID) && upToDate {
		n.votedFor = args.CandidateID
		reply.VoteGranted = true
		n.resetElectionTimeout() // đã thấy candidate hợp lệ → không tự ứng cử vội
		n.logLocked("CẤP VOTE cho node %d (term %d)", args.CandidateID, args.Term)
	} else {
		n.logLocked("TỪ CHỐI vote node %d (votedFor=%d, upToDate=%v)",
			args.CandidateID, n.votedFor, upToDate)
	}
	return reply
}

// handleAppendEntries — §3.1.2 Receiver rules
func (n *Node) handleAppendEntries(args AppendEntriesArgs) AppendEntriesReply {
	n.mu.Lock()
	defer n.mu.Unlock()

	reply := AppendEntriesReply{Term: n.currentTerm, Success: false}

	// Rule 1: reply false nếu term leader cũ hơn ta
	if args.Term < n.currentTerm {
		return reply
	}
	// Term hợp lệ → công nhận leader, reset election timeout (đây là "heartbeat")
	if args.Term > n.currentTerm {
		n.becomeFollowerLocked(args.Term)
	}
	n.state = Follower
	n.resetElectionTimeout()
	reply.Term = n.currentTerm

	// Rule 2: log phải có entry tại PrevLogIndex khớp PrevLogTerm
	if args.PrevLogIndex > n.lastLogIndex() ||
		n.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		return reply // consistency check fail → leader sẽ lùi nextIndex
	}

	// Rule 3 & 4: xoá entry xung đột, append entry mới (Log Matching Property §5.2)
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx <= n.lastLogIndex() {
			if n.log[idx].Term != entry.Term {
				n.log = n.log[:idx] // cắt bỏ từ điểm xung đột
				n.log = append(n.log, entry)
			}
			// nếu đã khớp thì giữ nguyên
		} else {
			n.log = append(n.log, entry)
		}
	}
	if len(args.Entries) > 0 {
		n.logLocked("nhận %d entry từ leader %d → log dài %d",
			len(args.Entries), args.LeaderID, n.lastLogIndex())
	}

	// Rule 5: cập nhật commitIndex theo leader
	if args.LeaderCommit > n.commitIndex {
		n.commitIndex = min(args.LeaderCommit, n.lastLogIndex())
		n.applyLocked()
	}

	reply.Success = true
	return reply
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// ----------------------------------------------------------------------------
// Quan sát trạng thái (dùng cho demo / kiểm tra safety)
// ----------------------------------------------------------------------------

func (n *Node) Snapshot() (state State, term, commitIndex, logLen int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state, n.currentTerm, n.commitIndex, n.lastLogIndex()
}

func (n *Node) IsLeader() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state == Leader && !n.dead
}

func (n *Node) ID() int { return n.id }
