package sim

import "sort"

// Handler nhận message đã tới nơi.
type Handler func(from, typ string, msg any)

// Network mô hình hoá mạng giữa các node: độ trễ ngẫu nhiên, mất gói, network
// partition và node chết.
//
// Điều kiện giao được message được kiểm tra tại THỜI ĐIỂM TỚI NƠI, không phải
// lúc gửi. Nhờ vậy một message đang bay dở mà mạng bị cắt sẽ bị mất — đúng như
// thực tế, và UI sẽ vẽ nó bay được nửa đường rồi biến mất.
type Network struct {
	s        *Sim
	handlers map[string]Handler
	ids      []string

	MinLatency Time
	MaxLatency Time
	DropRate   float64

	// group[id] = số hiệu phân vùng. Hai node liên lạc được khi cùng group.
	// Khi không có partition, mọi node đều ở group 0.
	group map[string]int
	down  map[string]bool

	mid  uint64
	Sent int
	Lost int
}

func NewNetwork(s *Sim, minLat, maxLat Time) *Network {
	return &Network{
		s:          s,
		handlers:   map[string]Handler{},
		group:      map[string]int{},
		down:       map[string]bool{},
		MinLatency: minLat,
		MaxLatency: maxLat,
	}
}

func (n *Network) Register(id string, h Handler) {
	n.handlers[id] = h
	n.ids = append(n.ids, id)
	sort.Strings(n.ids)
	n.group[id] = 0
}

func (n *Network) SetDown(id string, down bool) { n.down[id] = down }
func (n *Network) IsDown(id string) bool        { return n.down[id] }

// Partition chia cluster thành các nhóm. Node trong cùng nhóm liên lạc được
// với nhau, khác nhóm thì không.
func (n *Network) Partition(groups [][]string) {
	for i, g := range groups {
		for _, id := range g {
			n.group[id] = i
		}
	}
}

// Heal gỡ bỏ mọi partition.
func (n *Network) Heal() {
	for _, id := range n.ids {
		n.group[id] = 0
	}
}

// Groups trả về ánh xạ node -> nhóm hiện tại (cho UI vẽ đường phân vùng).
func (n *Network) Groups() map[string]int {
	out := map[string]int{}
	for k, v := range n.group {
		out[k] = v
	}
	return out
}

func (n *Network) reachable(from, to string) bool {
	if n.down[from] || n.down[to] {
		return false
	}
	return n.group[from] == n.group[to]
}

// Send gửi message. Ghi một record kind="msg" ngay lúc gửi (kèm thời điểm sẽ
// tới) để UI có thể animate; khi tới nơi ghi thêm "deliver" hoặc "drop".
func (n *Network) Send(from, to, typ string, msg any) {
	// Node đã chết thì không gửi được gì.
	if n.down[from] {
		return
	}

	span := int64(n.MaxLatency - n.MinLatency)
	lat := n.MinLatency
	if span > 0 {
		lat += Time(n.s.Rand().Int63n(span + 1))
	}
	// Rút thăm mất gói ngay lúc gửi để số lần gọi rng không phụ thuộc vào
	// trạng thái mạng lúc tới nơi (giữ determinism ổn định hơn).
	lost := n.DropRate > 0 && n.s.Rand().Float64() < n.DropRate

	n.mid++
	id := n.mid
	n.Sent++
	deliverAt := n.s.Now() + lat

	n.s.Emit(Record{
		Kind: "msg", From: from, To: to, Type: typ,
		MID: id, Deliver: deliverAt, Msg: msg,
	})

	n.s.After(lat, func() {
		if lost || !n.reachable(from, to) {
			n.Lost++
			n.s.Emit(Record{Kind: "drop", From: from, To: to, Type: typ, MID: id})
			return
		}
		n.s.Emit(Record{Kind: "deliver", From: from, To: to, Type: typ, MID: id})
		if h := n.handlers[to]; h != nil {
			h(from, typ, msg)
		}
	})
}
