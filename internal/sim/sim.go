// Package sim là nền tảng mô phỏng discrete-event dùng chung cho mọi engine
// consensus trong demo (Raft trước, Redis Cluster sau).
//
// Nguyên tắc bất di bất dịch: KHÔNG dùng time.Now(), KHÔNG dùng goroutine,
// KHÔNG dùng time.Sleep. Toàn bộ thời gian là ảo và mọi nguồn ngẫu nhiên đi qua
// một *rand.Rand duy nhất có seed. Nhờ đó cùng seed => cùng kết quả từng
// millisecond, và 60 giây mô phỏng chạy xong trong vài ms thực.
//
// Hệ quả quan trọng cho người viết engine: TUYỆT ĐỐI không được duyệt map khi
// thứ tự duyệt ảnh hưởng tới thứ tự gọi Sim.After hoặc Sim.Rand — thứ tự duyệt
// map trong Go là ngẫu nhiên và sẽ phá vỡ tính deterministic. Luôn duyệt qua
// slice đã sắp xếp.
package sim

import (
	"container/heap"
	"math/rand"
)

// Time là thời gian ảo, đơn vị millisecond.
type Time int64

type event struct {
	at  Time
	seq uint64
	fn  func()
}

// pq là hàng đợi ưu tiên theo (at, seq). seq là bộ phá hoà đảm bảo hai event
// cùng timestamp luôn chạy theo đúng thứ tự đã lên lịch.
type pq []*event

func (p pq) Len() int { return len(p) }
func (p pq) Less(i, j int) bool {
	if p[i].at != p[j].at {
		return p[i].at < p[j].at
	}
	return p[i].seq < p[j].seq
}
func (p pq) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p *pq) Push(x any)   { *p = append(*p, x.(*event)) }
func (p *pq) Pop() any {
	old := *p
	n := len(old)
	e := old[n-1]
	*p = old[:n-1]
	return e
}

// Record là một dòng trong trace. Toàn bộ trace được gửi sang browser dưới dạng
// JSON và browser dựng lại trạng thái bằng cách áp dụng tuần tự các record.
type Record struct {
	T    Time   `json:"t"`
	Kind string `json:"kind"` // node | msg | deliver | drop | note | commit | truncate | ack | fault
	Node string `json:"node,omitempty"`
	From string `json:"from,omitempty"`
	To   string `json:"to,omitempty"`
	MID  uint64 `json:"mid,omitempty"`
	Type string `json:"type,omitempty"` // RequestVote, AppendEntries, ...

	// Deliver chỉ có ở kind=="msg": thời điểm ảo message sẽ tới nơi. Browser
	// dùng cặp (T, Deliver) để nội suy vị trí message đang bay.
	Deliver Time `json:"deliver,omitempty"`

	Text  string         `json:"text,omitempty"`  // dòng hiển thị trong event log (tiếng Việt)
	Level string         `json:"level,omitempty"` // info | good | warn | bad
	Msg   any            `json:"msg,omitempty"`   // payload đầy đủ của RPC
	State any            `json:"state,omitempty"` // snapshot node khi kind=="node"
	Data  map[string]any `json:"data,omitempty"`
}

// Sim là bộ máy mô phỏng.
type Sim struct {
	now   Time
	seq   uint64
	q     pq
	rng   *rand.Rand
	trace []Record
}

func New(seed int64) *Sim {
	s := &Sim{rng: rand.New(rand.NewSource(seed))}
	heap.Init(&s.q)
	return s
}

func (s *Sim) Now() Time         { return s.now }
func (s *Sim) Rand() *rand.Rand  { return s.rng }
func (s *Sim) Trace() []Record   { return s.trace }

// After lên lịch fn chạy sau d millisecond ảo.
func (s *Sim) After(d Time, fn func()) {
	if d < 0 {
		d = 0
	}
	s.seq++
	heap.Push(&s.q, &event{at: s.now + d, seq: s.seq, fn: fn})
}

// At lên lịch fn chạy tại thời điểm tuyệt đối t (dùng cho script kịch bản).
func (s *Sim) At(t Time, fn func()) {
	s.After(t-s.now, fn)
}

// Emit ghi một record vào trace tại thời điểm hiện tại.
func (s *Sim) Emit(r Record) {
	r.T = s.now
	s.trace = append(s.trace, r)
}

// Note ghi một lời giải thích để hiển thị nổi bật trên UI — đây là lớp "dạy
// học" nối demo với tài liệu lý thuyết.
func (s *Sim) Note(level, text string) {
	s.Emit(Record{Kind: "note", Level: level, Text: text})
}

// Log ghi một dòng event log.
func (s *Sim) Log(level, text string) {
	s.Emit(Record{Kind: "log", Level: level, Text: text})
}

// RunUntil chạy mô phỏng cho tới thời điểm end.
func (s *Sim) RunUntil(end Time) {
	for s.q.Len() > 0 {
		e := s.q[0]
		if e.at > end {
			break
		}
		heap.Pop(&s.q)
		s.now = e.at
		e.fn()
	}
	s.now = end
}
