package raft

import (
	"fmt"
	"sort"

	"github.com/tienlamict/topic1-consensus-demo/internal/sim"
)

type State string

const (
	Follower  State = "follower"
	Candidate State = "candidate"
	Leader    State = "leader"
)

// Node là một server Raft. Cài đặt bám Figure 2 của paper; mỗi luật đều có
// comment trỏ về đúng ô trong figure để đối chiếu khi review.
type Node struct {
	ID    string
	peers []string // đã sắp xếp — LUÔN duyệt slice này, không duyệt map (xem sim doc)
	c     *Cluster

	// --- Persistent state (sống sót qua crash) ---
	currentTerm int
	votedFor    string
	log         []Entry // log[0] là sentinel term 0; index thật bắt đầu từ 1

	// --- Volatile state ---
	state       State
	commitIndex int
	lastApplied int

	// --- Volatile state trên leader ---
	nextIndex  map[string]int
	matchIndex map[string]int

	// --- Phục vụ election ---
	votes map[string]bool

	alive bool

	// Timer huỷ được bằng generation counter: mỗi lần reset thì gen++,
	// callback cũ khi bắn ra thấy gen lệch thì tự bỏ qua.
	timerGen      int
	timerStart    sim.Time
	timerDeadline sim.Time
	hbGen         int
}

func (n *Node) lastIndex() int { return len(n.log) - 1 }
func (n *Node) lastTerm() int  { return n.log[len(n.log)-1].Term }

// ---------------------------------------------------------------- snapshot

type snapshot struct {
	State         string   `json:"state"`
	Term          int      `json:"term"`
	VotedFor      string   `json:"votedFor"`
	CommitIndex   int      `json:"commitIndex"`
	Alive         bool     `json:"alive"`
	Log           []Entry  `json:"log"`
	TimerStart    sim.Time `json:"timerStart"`
	TimerDeadline sim.Time `json:"timerDeadline"`
	Votes         []string `json:"votes"`
	NextIndex     []int    `json:"nextIndex,omitempty"`
	MatchIndex    []int    `json:"matchIndex,omitempty"`
}

// emit ghi snapshot trạng thái node vào trace. Gọi sau mọi thay đổi trạng thái.
func (n *Node) emit() {
	sn := snapshot{
		State: string(n.state), Term: n.currentTerm, VotedFor: n.votedFor,
		CommitIndex: n.commitIndex, Alive: n.alive,
		Log:        append([]Entry(nil), n.log[1:]...), // bỏ sentinel khi gửi cho UI
		TimerStart: n.timerStart, TimerDeadline: n.timerDeadline,
	}
	for _, p := range n.peers { // duyệt slice để giữ thứ tự ổn định
		if n.votes[p] {
			sn.Votes = append(sn.Votes, p)
		}
	}
	if n.votes[n.ID] {
		sn.Votes = append(sn.Votes, n.ID)
	}
	sort.Strings(sn.Votes)
	if n.state == Leader {
		for _, p := range n.peers {
			sn.NextIndex = append(sn.NextIndex, n.nextIndex[p])
			sn.MatchIndex = append(sn.MatchIndex, n.matchIndex[p])
		}
	}
	n.c.s.Emit(sim.Record{Kind: "node", Node: n.ID, State: sn})
}

// ---------------------------------------------------------------- timers

// resetElectionTimer đặt lại election timeout với giá trị ngẫu nhiên trong
// [ElectionBase, ElectionBase+ElectionJitter).
//
// ElectionJitter chính là "randomized election timeout" ở §4.1 tài liệu lý
// thuyết. Đặt Jitter=0 để tái hiện thí nghiệm split vote của paper §9.3.
func (n *Node) resetElectionTimer() {
	n.timerGen++
	gen := n.timerGen
	to := n.c.cfg.ElectionBase
	if n.c.cfg.ElectionJitter > 0 {
		to += sim.Time(n.c.s.Rand().Int63n(int64(n.c.cfg.ElectionJitter)))
	}
	n.timerStart = n.c.s.Now()
	n.timerDeadline = n.timerStart + to
	n.c.s.After(to, func() {
		if n.timerGen != gen || !n.alive {
			return
		}
		n.onElectionTimeout(to)
	})
}

func (n *Node) stopElectionTimer() {
	n.timerGen++
	n.timerStart = 0
	n.timerDeadline = 0
}

// ---------------------------------------------------------------- election

func (n *Node) onElectionTimeout(waited sim.Time) {
	// Figure 2, Rules for Servers — Followers & Candidates:
	// "If election timeout elapses ... convert to candidate", và candidate
	// hết giờ thì bắt đầu election MỚI (term tăng tiếp).
	n.becomeCandidate(waited)
}

func (n *Node) becomeCandidate(waited sim.Time) {
	// Figure 2, Candidates: tăng currentTerm, vote cho chính mình, reset
	// election timer, gửi RequestVote tới mọi server khác.
	n.currentTerm++
	n.state = Candidate
	n.votedFor = n.ID
	n.votes = map[string]bool{n.ID: true}
	n.resetElectionTimer()
	n.c.metrics.ElectionsStarted++
	n.c.onLeadershipMaybeChanged()

	if waited > 0 {
		n.c.s.Log("warn", fmt.Sprintf("%s hết election timeout (%dms) → Candidate, term %d, tự vote cho mình",
			n.ID, waited, n.currentTerm))
	} else {
		n.c.s.Log("warn", fmt.Sprintf("%s được ép ứng cử → Candidate, term %d", n.ID, n.currentTerm))
	}
	n.emit()

	req := RequestVote{
		Term: n.currentTerm, CandidateID: n.ID,
		LastLogIndex: n.lastIndex(), LastLogTerm: n.lastTerm(),
	}
	for _, p := range n.peers {
		n.c.net.Send(n.ID, p, TypeRequestVote, req)
	}
}

func (n *Node) becomeLeader() {
	n.state = Leader
	n.stopElectionTimer()
	n.nextIndex = map[string]int{}
	n.matchIndex = map[string]int{}
	for _, p := range n.peers {
		// Figure 2, Leaders: nextIndex khởi tạo = last log index + 1,
		// matchIndex = 0.
		n.nextIndex[p] = n.lastIndex() + 1
		n.matchIndex[p] = 0
	}
	n.c.metrics.LeaderChanges++
	n.c.s.Log("good", fmt.Sprintf("%s THẮNG election term %d (%d/%d phiếu) → Leader",
		n.ID, n.currentTerm, len(n.votes), len(n.peers)+1))
	n.c.onLeadershipMaybeChanged()
	n.emit()

	// Gửi heartbeat rỗng ngay lập tức để chặn các election khác (Figure 2).
	n.hbGen++
	n.heartbeatTick(n.hbGen)
}

func (n *Node) stepDown(term int, why string) {
	changed := n.state != Follower
	if term > n.currentTerm {
		// Figure 2, Rules for Servers — All Servers: thấy term lớn hơn thì
		// cập nhật currentTerm và chuyển về follower. votedFor phải xoá vì
		// đây là term mới, chưa vote cho ai.
		n.currentTerm = term
		n.votedFor = ""
		changed = true
	}
	n.state = Follower
	n.votes = nil
	n.resetElectionTimer()
	if changed && why != "" {
		n.c.s.Log("warn", fmt.Sprintf("%s %s → về Follower, term %d", n.ID, why, n.currentTerm))
	}
	n.c.onLeadershipMaybeChanged()
	n.emit()
}

// ---------------------------------------------------------------- RPC handlers

func (n *Node) recv(from, typ string, msg any) {
	if !n.alive {
		return
	}
	switch typ {
	case TypeRequestVote:
		n.handleRequestVote(from, msg.(RequestVote))
	case TypeRequestVoteReply:
		n.handleRequestVoteReply(from, msg.(RequestVoteReply))
	case TypeAppendEntries:
		n.handleAppendEntries(from, msg.(AppendEntries))
	case TypeAppendReply:
		n.handleAppendReply(from, msg.(AppendEntriesReply))
	}
}

func (n *Node) handleRequestVote(from string, req RequestVote) {
	reply := RequestVoteReply{Term: n.currentTerm}

	// Figure 2, RequestVote RPC, Receiver rule 1.
	if req.Term < n.currentTerm {
		reply.Reason = fmt.Sprintf("term %d < currentTerm %d", req.Term, n.currentTerm)
		n.c.net.Send(n.ID, from, TypeRequestVoteReply, reply)
		return
	}
	if req.Term > n.currentTerm {
		n.stepDown(req.Term, fmt.Sprintf("thấy term %d cao hơn từ %s", req.Term, from))
	}
	reply.Term = n.currentTerm

	// Figure 2, RequestVote RPC, Receiver rule 2: chưa vote cho ai (hoặc đã
	// vote đúng candidate này) VÀ log của candidate ít nhất mới bằng log của
	// mình thì mới grant.
	alreadyVoted := n.votedFor != "" && n.votedFor != req.CandidateID
	upToDate := n.candidateUpToDate(req)

	switch {
	case alreadyVoted:
		reply.Reason = fmt.Sprintf("đã vote cho %s ở term %d", n.votedFor, n.currentTerm)
	case !upToDate:
		// Đây là Election Restriction ở §5.4.1 của paper / §2.2 tài liệu.
		reply.Reason = fmt.Sprintf("log candidate cũ hơn: (term %d, idx %d) < của tôi (term %d, idx %d)",
			req.LastLogTerm, req.LastLogIndex, n.lastTerm(), n.lastIndex())
		n.c.s.Log("bad", fmt.Sprintf("%s TỪ CHỐI vote cho %s — %s", n.ID, from, reply.Reason))
	default:
		reply.VoteGranted = true
		n.votedFor = req.CandidateID
		// Chỉ reset timer khi thực sự grant vote (Figure 2 footnote).
		n.resetElectionTimer()
		n.emit()
	}
	n.c.net.Send(n.ID, from, TypeRequestVoteReply, reply)
}

// candidateUpToDate cài đặt định nghĩa "at least as up-to-date" ở §5.4.1:
// so sánh term của entry cuối trước, term bằng nhau thì log dài hơn thắng.
func (n *Node) candidateUpToDate(req RequestVote) bool {
	if req.LastLogTerm != n.lastTerm() {
		return req.LastLogTerm > n.lastTerm()
	}
	return req.LastLogIndex >= n.lastIndex()
}

func (n *Node) handleRequestVoteReply(from string, rep RequestVoteReply) {
	if rep.Term > n.currentTerm {
		n.stepDown(rep.Term, fmt.Sprintf("nhận term %d cao hơn từ %s", rep.Term, from))
		return
	}
	// Reply đến trễ từ term cũ thì bỏ qua.
	if n.state != Candidate || rep.Term != n.currentTerm {
		return
	}
	if !rep.VoteGranted {
		return
	}
	n.votes[from] = true
	n.emit()
	if len(n.votes) > (len(n.peers)+1)/2 {
		n.becomeLeader()
	}
}

func (n *Node) handleAppendEntries(from string, req AppendEntries) {
	reply := AppendEntriesReply{Term: n.currentTerm}

	// Figure 2, AppendEntries RPC, Receiver rule 1.
	if req.Term < n.currentTerm {
		n.c.net.Send(n.ID, from, TypeAppendReply, reply)
		return
	}
	// Term hợp lệ ⇒ đây là leader đương nhiệm. Candidate phải lùi về follower
	// (Figure 2, Candidates: "If AppendEntries RPC received from new leader").
	if req.Term > n.currentTerm || n.state != Follower {
		n.stepDown(req.Term, fmt.Sprintf("nhận AppendEntries từ leader %s (term %d)", from, req.Term))
	} else {
		n.resetElectionTimer() // heartbeat hợp lệ → hoãn election
	}
	reply.Term = n.currentTerm

	// Figure 2, Receiver rule 2: log consistency check.
	if req.PrevLogIndex > n.lastIndex() || n.log[req.PrevLogIndex].Term != req.PrevLogTerm {
		n.emit()
		n.c.net.Send(n.ID, from, TypeAppendReply, reply)
		return
	}

	// Figure 2, Receiver rule 3 & 4: cắt bỏ entry xung đột rồi append phần mới.
	if len(req.Entries) > 0 {
		idx := req.PrevLogIndex + 1
		i := 0
		for ; i < len(req.Entries) && idx+i <= n.lastIndex(); i++ {
			if n.log[idx+i].Term != req.Entries[i].Term {
				// Xung đột: xoá entry này và TOÀN BỘ entry phía sau.
				removed := append([]Entry(nil), n.log[idx+i:]...)
				n.log = n.log[:idx+i]
				n.c.onTruncate(n.ID, idx+i, removed)
				break
			}
		}
		if idx+i > n.lastIndex() {
			n.log = append(n.log, req.Entries[i:]...)
		}
	}

	// Figure 2, Receiver rule 5.
	if req.LeaderCommit > n.commitIndex {
		last := req.PrevLogIndex + len(req.Entries)
		n.commitIndex = min(req.LeaderCommit, last)
		n.applyCommitted()
	}

	reply.Success = true
	reply.MatchIndex = req.PrevLogIndex + len(req.Entries)
	n.emit()
	n.c.net.Send(n.ID, from, TypeAppendReply, reply)
}

func (n *Node) handleAppendReply(from string, rep AppendEntriesReply) {
	if rep.Term > n.currentTerm {
		n.stepDown(rep.Term, fmt.Sprintf("nhận term %d cao hơn từ %s", rep.Term, from))
		return
	}
	if n.state != Leader || rep.Term != n.currentTerm {
		return
	}
	if rep.Success {
		if rep.MatchIndex > n.matchIndex[from] {
			n.matchIndex[from] = rep.MatchIndex
		}
		n.nextIndex[from] = n.matchIndex[from] + 1
		n.advanceCommit()
	} else {
		// Figure 2, Leaders: AppendEntries fail vì log không khớp → giảm
		// nextIndex và thử lại.
		if n.nextIndex[from] > 1 {
			n.nextIndex[from]--
		}
		n.sendAppendTo(from)
	}
	n.emit()
}

// ---------------------------------------------------------------- leader work

func (n *Node) heartbeatTick(gen int) {
	if n.hbGen != gen || n.state != Leader || !n.alive {
		return
	}
	for _, p := range n.peers {
		n.sendAppendTo(p)
	}
	n.c.s.After(n.c.cfg.HeartbeatInterval, func() { n.heartbeatTick(gen) })
}

func (n *Node) sendAppendTo(p string) {
	next := n.nextIndex[p]
	if next < 1 {
		next = 1
	}
	prev := next - 1
	req := AppendEntries{
		Term: n.currentTerm, LeaderID: n.ID,
		PrevLogIndex: prev, PrevLogTerm: n.log[prev].Term,
		Entries:      append([]Entry(nil), n.log[next:]...),
		LeaderCommit: n.commitIndex,
	}
	n.c.net.Send(n.ID, p, TypeAppendEntries, req)
}

// advanceCommit cài đặt Figure 2, Leaders, luật cuối:
// tìm N lớn nhất sao cho majority matchIndex ≥ N VÀ log[N].Term == currentTerm.
//
// Điều kiện term là chỗ tinh vi nhất của Raft (§5.4.2): leader KHÔNG được
// commit entry của term cũ chỉ bằng cách đếm replica.
func (n *Node) advanceCommit() {
	idx := []int{n.lastIndex()} // chính leader luôn có đủ log của mình
	for _, p := range n.peers {
		idx = append(idx, n.matchIndex[p])
	}
	sort.Sort(sort.Reverse(sort.IntSlice(idx)))
	N := idx[len(idx)/2] // phần tử giữa = index mà majority đã có

	if N > n.commitIndex && n.log[N].Term == n.currentTerm {
		n.commitIndex = N
		n.applyCommitted()
	}
}

// applyCommitted áp dụng các entry đã commit vào state machine.
func (n *Node) applyCommitted() {
	for n.lastApplied < n.commitIndex {
		n.lastApplied++
		e := n.log[n.lastApplied]
		n.c.onCommit(n.ID, n.lastApplied, e, n.state == Leader)
	}
}
