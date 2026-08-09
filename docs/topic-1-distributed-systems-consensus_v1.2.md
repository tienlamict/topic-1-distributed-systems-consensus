# Topic 1 — Nền tảng Distributed Systems & Consensus
## Tài liệu tổng hợp từ project knowledge (v2.1 — đã rà soát & hiệu đính)

> **Phạm vi:** Tổng hợp các nội dung liên quan đến consensus, strong/eventual consistency, Redis failover voting, và các trade-off distributed systems làm nền tảng cho phần khảo sát Kubernetes Operator cho Redis Cluster.
>
> **Thay đổi so với v1:** Bổ sung 2 nguồn: (1) Raft paper extended version (Ongaro & Ousterhout 2014) — lấp gap về message types, leader election, safety properties, log compaction; (2) Redis Cluster Specification — lấp gap về Redis failover voting, epoch system, PFAIL/FAIL, FAILOVER_AUTH_REQUEST.

---

## 0. Coverage Map — File nào nói gì về Topic 1

| File | Mức độ liên quan | Nội dung cụ thể về Topic 1 |
|------|------------------|----------------------------|
| `03_1_raft.pdf` ⭐ **MỚI** | ⭐⭐⭐⭐ Nguồn lõi | Paper gốc Ongaro & Ousterhout 2014 (extended version, 18 trang). Chứa: Raft basics (states, terms), Leader election chi tiết, Log replication, Safety properties (5 properties với proof sketch), Cluster membership changes (joint consensus), Log compaction (InstallSnapshot), Timing requirements, Performance benchmarks. |
| `03_2_Redis_cluster_specification.md` ⭐ **MỚI** | ⭐⭐⭐⭐ Nguồn lõi | Official Redis Cluster Specification. Chứa: cluster goals, hash slot model (CRC16 mod 16384), cluster bus/gossip, heartbeat packets, PFAIL/FAIL failure detection, currentEpoch/configEpoch, replica election và promotion (FAILOVER_AUTH_REQUEST/ACK), replica rank, write safety, availability analysis. |
| `03_raft-consensus-algorithm.md` | ⭐⭐ High-level | Landing page của raft.github.io — chỉ có definition high-level + bảng implementations. Bị paper gốc thay thế cho phần kỹ thuật. |
| `10_Rearchitecting_Kubernetes_for_the_Edge.md` | ⭐⭐⭐ Trực tiếp | Etcd dùng Raft với majority quorum; CAP theorem; benchmark etcd latency/throughput theo cluster size; phê phán strong consistency; đề xuất eventual consistency dùng CRDT. |
| `01`, `08` | ⭐ Gián tiếp | Redis master-slave architecture, role-based management — bổ sung context thực tiễn cho Redis consensus. |
| Các file 02, 04-07, 09 | ❌ Không liên quan | Không có nội dung về consensus/consistency ở cấp độ lý thuyết. |

**Kết luận coverage sau khi bổ sung:** ~**90%** (trước là ~40%). Giờ có thể viết đầy đủ phần lý thuyết nền tảng của luận văn.

---

## 1. Định nghĩa & Mục đích

### 1.1. Consensus là gì?

Consensus algorithms cho phép một tập máy tính hoạt động như một nhóm thống nhất có thể sống sót qua các failure của một số thành viên. Vì vậy, chúng đóng vai trò then chốt trong việc xây dựng các hệ thống phần mềm quy mô lớn, đáng tin cậy. [03_1_raft.pdf, §1]

Consensus là bài toán mà nhiều server phải đồng ý về một giá trị; một khi đã đạt được quyết định, nó là final. Các thuật toán consensus điển hình tiến triển được khi đa số (majority) server còn hoạt động — cluster 5 server vẫn hoạt động khi 2 server fail. Nếu nhiều hơn majority fail, hệ thống dừng lại nhưng **không bao giờ trả kết quả sai**. [03_raft-consensus-algorithm.md]

### 1.2. Các thuộc tính tiêu biểu của consensus algorithm cho hệ thống thực tế

Raft paper liệt kê các thuộc tính bắt buộc: [03_1_raft.pdf, §2]

1. **Safety** — không bao giờ trả kết quả sai trong mọi điều kiện non-Byzantine (network delays, partitions, packet loss, duplication, reordering).
2. **Fully functional (available)** — hoạt động miễn là majority server còn running và communicate được với nhau. Cluster 5 server tolerate được 2 failure. Server được giả định fail bằng cách stop; có thể recover từ stable storage và rejoin cluster.
3. **Không phụ thuộc timing cho correctness** — clock faulty và message delay cực đại chỉ có thể gây availability issues (không gây safety violation).
4. **Common case**: command hoàn thành ngay khi majority cluster đã respond. Minority node chậm không ảnh hưởng performance hệ thống.

### 1.3. Replicated State Machine

Consensus thường xuất hiện trong bối cảnh **replicated state machine**. Mỗi server có một state machine và một log: [03_1_raft.pdf, §2] [03_raft-consensus-algorithm.md]

- **State machine**: thành phần cần fault-tolerance (ví dụ: hash table).
- **Log**: chuỗi command (ví dụ: *set x to 3*).
- **Consensus module**: nhận command từ client, thêm vào log, communicate với consensus module trên server khác để đảm bảo mọi log cuối cùng chứa cùng request theo cùng thứ tự.

Kết quả: các server xuất hiện như một state machine đơn, highly reliable. State machine deterministic → cùng input → cùng output → cùng state.

**Ứng dụng thực tế:** GFS, HDFS, RAMCloud dùng replicated state machine riêng để quản lý leader election và lưu config. Chubby và ZooKeeper là ví dụ điển hình. [03_1_raft.pdf, §2]

### 1.4. Raft — Định nghĩa và Novel Features

Raft là consensus algorithm để quản lý replicated log. Tương đương (multi-)Paxos về correctness và efficiency, nhưng **cấu trúc khác với Paxos** — làm cho Raft dễ hiểu hơn và là foundation tốt hơn để xây dựng hệ thống thực tế. [03_1_raft.pdf, Abstract]

Raft tương đồng nhiều mặt với các thuật toán consensus hiện có (gần nhất là **Viewstamped Replication** của Oki & Liskov), nhưng có một số **đặc trưng novel** so với các thuật toán consensus nói chung: [03_1_raft.pdf, §1]

- **Strong leader**: log entries chỉ flow từ leader sang các server khác. Điều này đơn giản hoá quản lý replicated log và làm Raft dễ hiểu hơn.
- **Leader election**: dùng **randomized timers** để elect leader. Chỉ thêm một ít mechanism vào heartbeat vốn đã bắt buộc cho mọi consensus algorithm.
- **Membership changes**: mechanism mới gọi là **joint consensus** — majority của hai configuration khác nhau overlap trong transition. Cluster tiếp tục hoạt động normal trong khi thay đổi config.

Raft decompose consensus thành **3 sub-problem tương đối độc lập**: [03_1_raft.pdf, §5]

1. **Leader election** (§5.2) — chọn leader mới khi leader hiện tại fail.
2. **Log replication** (§5.3) — leader nhận log entries từ client và replicate cho các server khác.
3. **Safety** (§5.4) — key safety property là State Machine Safety Property.

### 1.5. Mục đích trong ngữ cảnh đề tài Redis Operator

- **Với Kubernetes**: etcd dùng Raft làm nền tảng consistency cho toàn bộ cluster state → mọi API server request cuối cùng đều đi qua consensus layer. [10_Rearchitecting_Kubernetes_for_the_Edge.md]
- **Với Redis Cluster**: Dùng epoch-based voting — một biến thể *tương tự* Raft term nhưng không phải Raft. Chi tiết ở §2.3 và §4.2.

---

## 2. Cơ chế hoạt động

### 2.1. Raft — Server States và Terms

**Server states:** Tại mỗi thời điểm, mỗi server ở một trong ba trạng thái: [03_1_raft.pdf, §5.1]

- **Follower**: passive, chỉ respond request từ leader/candidate.
- **Candidate**: dùng để elect leader mới.
- **Leader**: xử lý mọi client request. Nếu client contact follower, follower redirect sang leader.

Trong normal operation có **đúng 1 leader** và tất cả còn lại là follower.

**Terms:** Raft chia thời gian thành các **term** có độ dài bất kỳ, đánh số bằng số nguyên liên tiếp. [03_1_raft.pdf, §5.1]

- Mỗi term bắt đầu bằng một **election**.
- Nếu candidate thắng, nó làm leader cho phần còn lại của term.
- Nếu **split vote** → term kết thúc không có leader → term mới bắt đầu với election mới.
- **Raft đảm bảo tối đa 1 leader trong một term** (Election Safety Property).

**Terms = logical clock**: cho phép server phát hiện stale information (ví dụ: stale leader). Mỗi server lưu `currentTerm` tăng monotonically. Khi 2 server communicate, nếu term của server A nhỏ hơn B → A update term lên B. Candidate/leader phát hiện term của mình stale → revert về follower ngay. Request với stale term → reject. [03_1_raft.pdf, §5.1]

### 2.2. Raft subproblems — Tóm tắt cơ chế

**Leader Election:** [03_1_raft.pdf, §5.2]

1. Server start up ở follower state.
2. Leader gửi periodic heartbeat (empty AppendEntries) để duy trì authority.
3. Follower không nhận communication trong `election timeout` → assume leader chết → bắt đầu election.
4. Follower tăng `currentTerm`, chuyển sang candidate, vote cho chính mình, gửi RequestVote RPC song song tới mọi server khác.
5. Kết quả có thể: (a) wins election — nhận vote từ majority cluster cho same term → trở thành leader; (b) nhận AppendEntries từ server khác với term ≥ current term → recognize legitimate leader và revert follower; (c) không ai thắng → election timeout → bắt đầu election mới.

**Log Replication:** [03_1_raft.pdf, §5.3]

1. Client request → leader append command vào local log thành new entry.
2. Leader gửi AppendEntries RPC song song cho mọi server.
3. Khi entry đã được replicated an toàn (trên majority server) → leader apply entry vào state machine → return kết quả cho client.
4. Nếu follower crash/slow hoặc packet loss → leader retry AppendEntries indefinitely (kể cả sau khi đã respond client) cho đến khi mọi follower eventually lưu mọi log entry.
5. Log entry được **committed** khi leader đã replicate nó trên majority server.

**Safety Restrictions:** [03_1_raft.pdf, §5.4]

- Không phải mọi server đều eligible làm leader — Raft thêm restriction: candidate log phải "at least as up-to-date" so với majority. Up-to-date được định nghĩa: so sánh index và term của log entry cuối cùng; term lớn hơn thắng, cùng term thì log dài hơn thắng.
- Leader không commit entry từ term trước bằng cách đếm replica — chỉ commit entry từ term hiện tại. Entry cũ được commit gián tiếp qua Log Matching Property.

### 2.3. Redis Cluster — Epoch System (analog với Raft term)

Redis Cluster dùng concept tương tự Raft "term" nhưng gọi là **epoch**. Epoch dùng để đánh version tăng dần cho events; khi nhiều node cung cấp thông tin conflict, node khác có thể biết state nào mới nhất. [03_2_Redis_cluster_specification.md]

Có **hai loại epoch:**

| Loại | Scope | Khi nào tăng |
|------|-------|--------------|
| `currentEpoch` | Global cho cluster | Khi nhận packet với epoch lớn hơn → update theo. Eventually mọi node agree về largest currentEpoch. |
| `configEpoch` | Gắn với từng master | Được cấp giá trị mới chủ yếu khi replica promotion (election) thành công; ngoài ra còn được sinh qua *configEpoch conflicts resolution algorithm* và lan truyền/cập nhật qua UPDATE message. Mỗi master advertise configEpoch trong ping/pong cùng với bitmap slots nó serve. |

**Quan trọng:** [03_2_Redis_cluster_specification.md]
- Khi replica win election → obtains new unique incremental `configEpoch` cao hơn mọi master hiện có.
- `configEpoch` giúp resolve conflict khi các node claim divergent configurations (sau network partition/node failure).
- `configEpoch` được lưu xuống disk (`nodes.conf`) và `fsync` trước khi node tiếp tục operations.

### 2.4. Redis Cluster Bus & Gossip

Mọi cluster node kết nối nhau qua **Cluster Bus** (TCP, binary protocol). Dùng gossip protocol để: [03_2_Redis_cluster_specification.md]

- Propagate thông tin về cluster (discover new nodes).
- Gửi ping packet kiểm tra node khác còn running.
- Signal specific conditions (như failure).
- Propagate Pub/Sub messages.
- Orchestrate manual failovers.

**Heartbeat pattern:** [03_2_Redis_cluster_specification.md]
- Mỗi node **ping vài random node mỗi giây** → tổng số ping độc lập với cluster size.
- Tuy nhiên, **mọi node đảm bảo ping mọi node khác đã không ping/pong trong > NODE_TIMEOUT/2**.
- Ví dụ: cluster 100 nodes với NODE_TIMEOUT=60s → mỗi node gửi 99 ping/30s → ~3.3 ping/s/node → 330 ping/s cho cluster.

### 2.5. Etcd áp dụng Raft

- Etcd là strongly consistent, distributed key-value store, dùng Raft để maintain consistency, yêu cầu majority quorum. [10_Rearchitecting_Kubernetes_for_the_Edge.md]
- Khuyến nghị: cluster 3 hoặc 5 node — đủ đạt HA tránh overhead scale.
- Etcd **không horizontal scalable** vì overhead Raft tăng theo số node.
- Khi một Pod được schedule trong Kubernetes: nhiều quorum writes diễn ra trên critical path (ReplicaSet update → Pod create → schedule → Kubelet notify → events) — mỗi lần write cần quorum; latency bị chi phối bởi node chậm nhất. [10_Rearchitecting_Kubernetes_for_the_Edge.md]

### 2.6. Redis Cluster Key Distribution (bonus — context cho Operator)

Không trực tiếp là consensus, nhưng quan trọng cho hiểu Redis Cluster: [03_2_Redis_cluster_specification.md]

- Keyspace chia thành **16384 slots** — giới hạn cluster max 16384 master nodes (khuyến nghị thực tế ~1000 nodes).
- `HASH_SLOT = CRC16(key) mod 16384`.
- Mỗi master node serve một subset trong 16384 slots.
- **Hash tag**: nếu key chứa pattern `{...}`, chỉ substring nằm giữa `{` và `}` (theo quy tắc occurrence đầu tiên) được hash → các key có cùng hash tag rơi vào cùng slot → enable multi-key ops. Hash tag có thể nằm ở bất kỳ vị trí nào trong key (không phải prefix).

---

## 3. Message Types

### 3.1. Raft RPCs — đầy đủ 3 loại

Raft basic consensus chỉ cần **2 loại RPC**, tổng protocol là **3 loại**: [03_1_raft.pdf, §5.1 và §7]

#### 3.1.1. RequestVote RPC (leader election)

Invoked bởi candidate để gather votes. [03_1_raft.pdf, Figure 2]

**Arguments:**
- `term` — term của candidate
- `candidateId` — id candidate request vote
- `lastLogIndex` — index của last log entry của candidate
- `lastLogTerm` — term của last log entry

**Results:**
- `term` — currentTerm (cho candidate update mình)
- `voteGranted` — true nếu candidate nhận được vote

**Receiver rules:**
1. Reply false nếu `term < currentTerm`.
2. Nếu `votedFor` là null hoặc `candidateId`, **và** log của candidate at-least-as-up-to-date như log của receiver → grant vote.

#### 3.1.2. AppendEntries RPC (log replication + heartbeat)

Invoked bởi leader để replicate log entries; cũng dùng làm heartbeat (với entries rỗng). [03_1_raft.pdf, Figure 2]

**Arguments:**
- `term` — term của leader
- `leaderId` — để follower redirect client
- `prevLogIndex` — index của log entry ngay trước entries mới
- `prevLogTerm` — term của prevLogIndex entry
- `entries[]` — log entries để store (empty cho heartbeat)
- `leaderCommit` — leader's commitIndex

**Results:**
- `term` — currentTerm
- `success` — true nếu follower chứa entry matching `prevLogIndex` và `prevLogTerm`

**Receiver rules (rút gọn):**
1. Reply false nếu `term < currentTerm`.
2. Reply false nếu log không có entry tại `prevLogIndex` khớp `prevLogTerm`.
3. Xung đột với entry mới → delete existing entry và all following.
4. Append entries mới không có trong log.
5. Nếu `leaderCommit > commitIndex` → set `commitIndex = min(leaderCommit, index last new entry)`.

#### 3.1.3. InstallSnapshot RPC (log compaction)

Invoked bởi leader để gửi chunk snapshot cho follower lag quá xa. Leader luôn gửi chunk in order. [03_1_raft.pdf, Figure 13]

**Arguments:**
- `term`, `leaderId`
- `lastIncludedIndex` — snapshot thay thế mọi entry đến index này
- `lastIncludedTerm` — term của lastIncludedIndex
- `offset` — byte offset của chunk
- `data[]` — raw bytes
- `done` — true nếu là chunk cuối

**Receiver rules (rút gọn):**
1. Reply ngay nếu `term < currentTerm`.
2. Tạo snapshot file mới nếu first chunk.
3. Write data vào offset; nếu `done=false` → wait for more chunks.
4. Lưu snapshot, discard snapshot cũ có index nhỏ hơn.
5. Nếu existing log entry có same index+term với lastIncluded → retain log entries following và reply.
6. Ngược lại → discard toàn bộ log, reset state machine bằng snapshot content.

### 3.2. Redis Cluster Messages

Các loại packet trên Cluster Bus: [03_2_Redis_cluster_specification.md]

| Packet Type | Mục đích |
|-------------|----------|
| `PING` / `PONG` | Heartbeat. Cùng structure. |
| `FAIL` message | Force nodes mark một node là FAIL (khác với FAIL flag bên trong heartbeat). |
| `FAILOVER_AUTH_REQUEST` | Replica request vote từ masters khi initiate election. |
| `FAILOVER_AUTH_ACK` | Master reply positive cho replica vote request. |
| `UPDATE` | Force node cập nhật config khi detect stale info. |
| `MFSTART` / manual failover | Orchestrate manual failover. |
| Pub/Sub | Propagate pub/sub messages. |

#### 3.2.1. Heartbeat Packet Content (Ping/Pong)

Common header chứa: [03_2_Redis_cluster_specification.md]

- **Node ID** — 160-bit pseudorandom string, gán khi tạo node, bất biến trong đời node.
- **`currentEpoch`** và **`configEpoch`** của sender.
- **Node flags** — replica/master, các flag 1-bit khác.
- **Bitmap hash slots** served by sender (hoặc bởi master nếu sender là replica).
- **TCP base port** (client port) và **Cluster port** (node-to-node).
- **Cluster state** (down/ok) từ góc nhìn sender.
- **Master node ID** nếu sender là replica.

**Gossip section** (đặc thù cho ping/pong): chứa view của sender về few random nodes trong cluster. Số nodes mentioned proportional với cluster size. Với mỗi node trong gossip: Node ID, IP+port, flags.

### 3.3. So sánh Raft RPCs vs. Redis Cluster Messages

| Chức năng | Raft | Redis Cluster |
|-----------|------|---------------|
| Heartbeat | Empty AppendEntries RPC | PING/PONG trên Cluster Bus |
| Leader/Promotion vote request | RequestVote RPC | FAILOVER_AUTH_REQUEST |
| Vote reply | RequestVote result (term + voteGranted) | FAILOVER_AUTH_ACK |
| Log replication | AppendEntries RPC | (Không có — Redis dùng async replication riêng, không đi qua consensus) |
| Snapshot transfer | InstallSnapshot RPC | (Không có tương ứng trong consensus layer) |
| Failure notification | (Không explicit — inferred từ missing heartbeat) | FAIL message (explicit, force propagation) |
| Config update | (Membership change qua log entries đặc biệt) | UPDATE message (gossip với configEpoch) |

**Nhận xét quan trọng:** Redis Cluster consensus **CHỈ áp dụng cho replica promotion (failover voting)**, không apply cho data replication. Data được replicate async giữa master-slave **không qua consensus**. Đây là trade-off đổi safety lấy performance.

---

## 4. Failure Detection

### 4.1. Raft — Failure Detection qua Heartbeat Timeout

**Cơ chế:** [03_1_raft.pdf, §5.2]

1. Leader gửi periodic heartbeat (empty AppendEntries) tới mọi follower để duy trì authority.
2. Follower start `election timeout` timer mỗi khi nhận valid RPC từ leader/candidate.
3. Nếu timer expire mà không nhận communication → follower assume leader chết → start election.

**Randomized Election Timeout** — thiết kế thiên tài của Raft: [03_1_raft.pdf, §5.2]

- Election timeout chọn **randomly từ fixed interval** (ví dụ: 150–300ms).
- Mục đích: spread out servers → trong đa số case chỉ 1 server timeout trước → wins election → gửi heartbeat trước khi server khác timeout.
- Cùng mechanism xử lý split vote: mỗi candidate restart randomized timeout ở start of election.

**Timing Requirement cho Raft hoạt động ổn định:** [03_1_raft.pdf, §5.6]

```
broadcastTime ≪ electionTimeout ≪ MTBF
```

- `broadcastTime` — thời gian server gửi RPC song song tới mọi server và nhận respond. Typical: 0.5–20ms (phụ thuộc storage).
- `electionTimeout` — chọn bởi developer. Typical: 10–500ms.
- `MTBF` — Mean Time Between Failures của single server.

`broadcastTime` phải **order of magnitude nhỏ hơn** `electionTimeout` để leader kịp gửi heartbeat trước khi follower timeout. `electionTimeout` phải nhiều orders of magnitude nhỏ hơn MTBF để system tiến triển.

**Performance measurements (từ paper):** [03_1_raft.pdf, §9.3]

- Cluster 5 server với broadcast time ~15ms.
- Không có randomization → leader election > 10s do nhiều split vote.
- Chỉ 5ms randomness → median downtime 287ms.
- 50ms randomness → worst-case 513ms.
- Election timeout 12-24ms → elect leader trung bình 35ms, worst 152ms — nhưng quá thấp có thể gây unnecessary leader changes.
- **Khuyến nghị conservative: election timeout 150–300ms** — tránh leader changes không cần thiết, availability tốt.

### 4.2. Redis Cluster — PFAIL/FAIL Two-Phase Detection

Redis dùng **hai cấp độ flag** cho failure detection: [03_2_Redis_cluster_specification.md]

#### 4.2.1. PFAIL (Possible Failure) — Local Information

Node A flag node B là `PFAIL` khi B không reachable trong > `NODE_TIMEOUT`. Cả master và replica đều có thể flag node khác là PFAIL.

**Non-reachability** định nghĩa: có active ping (ping đã gửi, chưa nhận reply) pending lâu hơn NODE_TIMEOUT. Để cơ chế work, NODE_TIMEOUT phải đủ lớn so với network RTT.

**Reliability measure**: Node cố gắng reconnect với node khác ngay khi nửa NODE_TIMEOUT trôi qua không có reply → đảm bảo connection alive, tránh false failure report do TCP issue.

#### 4.2.2. FAIL — Majority-Confirmed Failure

PFAIL một mình **không đủ** trigger replica promotion. Phải escalate thành FAIL.

**Điều kiện escalate PFAIL → FAIL:**
- Node A đã flag B là PFAIL.
- A đã collect (qua gossip) info về B từ góc nhìn của **majority masters** trong cluster.
- Majority masters signal PFAIL hoặc FAIL condition trong `NODE_TIMEOUT × FAIL_REPORT_VALIDITY_MULT` time (hệ số = 2, tức 2×NODE_TIMEOUT).

Khi đủ điều kiện, A sẽ:
- Mark node là FAIL.
- Send `FAIL` message (khác với FAIL condition trong heartbeat) tới mọi reachable node.

FAIL message **force** mọi receiver mark node là FAIL, bất kể đã flag PFAIL hay chưa.

**FAIL flag là mostly one-way.** Có thể clear FAIL trong 3 trường hợp:
1. Node reachable lại và là replica (replicas không bị failover).
2. Node reachable lại và là master không serve slot nào.
3. Node reachable lại là master, đã trôi qua N×NODE_TIMEOUT mà không có replica promotion nào detected.

#### 4.2.3. Weak Agreement

Redis thừa nhận agreement cho PFAIL→FAIL transition là **weak**: [03_2_Redis_cluster_specification.md]

- Nodes collect view của node khác trên khoảng thời gian → ngay cả khi "majority masters agree", thực ra chỉ là state collected ở different times từ different nodes. Không đảm bảo, cũng không yêu cầu, tại thời điểm nào majority masters cùng agree. Tuy nhiên failure report cũ bị discard.
- FAIL message có thể không reach mọi node (do partition).

Nhưng Redis có **liveness requirement**: eventually mọi node sẽ agree về state của node. Hai case split-brain:
- **Case 1**: Majority masters đã flag FAIL → chain effect → eventually mọi node flag FAIL.
- **Case 2**: Chỉ minority flag FAIL → replica promotion không xảy ra → eventually mọi node clear FAIL.

### 4.3. So sánh Raft vs. Redis Cluster Failure Detection

| Tiêu chí | Raft | Redis Cluster |
|----------|------|---------------|
| Cơ chế chính | Heartbeat timeout (election timeout) | PFAIL/FAIL two-phase với gossip |
| Layer detection | Single (election timeout) | Dual (local PFAIL + majority-confirmed FAIL) |
| Explicit fail message | Không (implicit qua missing heartbeat) | Có (FAIL message) |
| Agreement strength | Strong (quorum-based) | Weak (gossip-based, collected over time) |
| False positive prevention | Randomized timeout + timing requirement | Reconnect attempt at NODE_TIMEOUT/2 |
| Split-brain handling | Election Safety + term | configEpoch conflict resolution + Write safety trade-off |
| Granularity | Detect leader failure (start election) | Detect bất kỳ master/replica failure |

**Quan sát quan trọng:** Raft thiên về *safety* (cùng với availability qua majority), Redis thiên về *performance với acceptable safety* — ưu tiên không block nếu có uncertainty về failure.

### 4.4. Liên quan đến Operator design

Khi thiết kế Redis Operator:

- **Không nên** replicate Redis failover logic — Redis Cluster tự failover qua voting.
- **Nên** observe role qua probe (kbagent sidecar trong KubeBlocks làm việc này) — thông tin cần reflected trong Pod labels để Kubernetes Service selector work đúng [08_Resource_Utilization...md].
- **Cần** detect được "effective" failure mode ngoài phạm vi Redis (ví dụ: pod không healthy nhưng Redis node vẫn vote) — dùng liveness/readiness probe của K8s.
- **Cẩn thận** với tension giữa K8s failure detection (liveness probe) và Redis cluster failure detection (PFAIL/FAIL). Pod restart có thể lại làm Redis re-join mà không cần failover — tối ưu cost nhưng cần đồng bộ state.

---

## 5. Ưu / Nhược điểm

### 5.1. Raft — Ưu điểm

**Từ paper gốc:** [03_1_raft.pdf]

- **Understandability verified by experiment**: User study 43 students — Raft trung bình 4.9 điểm cao hơn Paxos (25.7 vs 20.8 trong thang điểm 60). 33/43 student trả lời Raft quiz tốt hơn Paxos quiz. [03_1_raft.pdf, §9.1]
- **Complete foundation**: description đủ đầy đủ để build practical system (vs. Paxos phải fill-in nhiều detail).
- **Strong leader** → log entries chỉ flow một chiều → ít mechanism hơn VR hay ZooKeeper.
- **Formal correctness**: TLA+ specification ~400 lines, mechanical proof của Log Completeness Property; informal proof (~3500 words) của State Machine Safety.
- **Performance comparable Paxos**: Raft cũng achieve consensus với minimal messages (single round-trip từ leader tới half cluster). Support batching và pipelining.
- **Adoption**: 25+ independent open-source implementations; các công ty deploying Raft-based systems.

### 5.2. Raft — Safety Properties (5 bảo đảm)

[03_1_raft.pdf, Figure 3]

| Property | Nội dung | Section |
|----------|----------|---------|
| **Election Safety** | Tối đa 1 leader được elected trong một term | §5.2 |
| **Leader Append-Only** | Leader không bao giờ overwrite hoặc delete entry trong log của mình; chỉ append | §5.3 |
| **Log Matching** | Nếu 2 log chứa entry cùng index và term → logs giống hệt nhau ở mọi entry cho đến index đó | §5.3 |
| **Leader Completeness** | Nếu log entry đã committed ở term T → entry đó sẽ có mặt trong log của mọi leader có term cao hơn T | §5.4 |
| **State Machine Safety** | Nếu server đã apply log entry tại index I vào state machine → không server nào khác sẽ apply entry KHÁC tại cùng index | §5.4.3 |

### 5.3. Redis Cluster — Ưu điểm

[03_2_Redis_cluster_specification.md, Main Properties]

- **High performance + linear scalability** up to 1000 nodes. Không có proxy, dùng async replication, không merge operations.
- **No consensus on data path** → không bottleneck write với quorum.
- **Configurable write safety vs availability** — trade-off có thể tune.

### 5.4. Redis Cluster — Nhược điểm (Acknowledged)

Redis spec công khai thừa nhận các weakness: [03_2_Redis_cluster_specification.md, Write safety]

**"Last failover wins" implicit merge** — có window có thể lose writes:

1. Client connect với majority → write tới master → master reply OK trước khi replicate async → master die → replica promoted → **write lost mãi mãi**.
2. Master unreachable do partition → bị failover → sau đó reachable lại → client với routing table cũ write tới old master trước khi được convert thành replica → write lost. Rất hiếm vì master không communicate với majority sẽ stop accept writes.

**Writes tới minority side** có window mất lớn hơn: sau NODE_TIMEOUT, minority side stop accept writes — nhưng trước đó mọi write có thể lost.

**Availability analysis:**

- Không available trong minority side.
- Majority side: available nếu có majority masters và mỗi unreachable master có ít nhất 1 reachable replica.
- Công thức xác suất: cluster N masters, 1 replica/master, sau khi 2 node partition away từ majority → xác suất cluster không available = `1/(N*2-1)`.
- Ví dụ: 5 masters → 11.11% probability không available sau 2 partitions.

### 5.5. Strong Consistency (etcd/Raft) — Nhược điểm ở quy mô lớn

Benchmark cụ thể từ file 10: [10_Rearchitecting_Kubernetes_for_the_Edge.md]

**Điều kiện:** etcd v3.4.13, 2 CPU + 1GB RAM container, SSD, Intel Xeon 4112 16-core, 1,000 clients × 100 connections × 100,000 ops, median 10 runs.

| Metric | Xu hướng khi tăng cluster size |
|--------|-------------------------------|
| Put latency | Tăng **tuyến tính** — phải write tới majority mỗi lần |
| Range latency | Tăng chậm — read không cần flush write |
| Put throughput | **Giảm nghiêm trọng** — quorum cần nhiều inter-node requests |
| Range throughput | Giảm (ít hơn put) |

**Lý do cơ bản:**
- Cluster lớn → leader sync với nhiều follower → work tăng.
- Mọi quorum write bị bottleneck bởi node chậm nhất trong majority.
- Put chiếm ~30% Kubernetes requests → etcd là bottleneck thực tế.

### 5.6. Eventual Consistency (proposed alternative) — Ưu/Nhược

[10_Rearchitecting_Kubernetes_for_the_Edge.md, §4-5]

**Ưu:**
- Read/write single node không cần communicate ngay với node khác.
- CRDT resolve conflict khi sync (lazy, không eager).
- Throughput tăng khi scale (không coordination trên critical path).
- Partition-tolerant cho datacenter hoặc edge.

**Nhược:**
- Transaction có thể operate trên stale data.
- Report "leader node" trở nên vô nghĩa — mỗi node có thể được report là leader.
- Cần JSON CRDT translation layer cho Kubernetes resources (protobuf).

### 5.7. Tổng hợp Trade-off 3 chiều

| Tiêu chí | etcd (Raft, strong) | Redis Cluster (epoch-based, weak) | Proposed CRDT (eventual) |
|----------|---------------------|-----------------------------------|---------------------------|
| Data path consensus | Có (mọi write) | Không (async replication) | Không |
| Failover consensus | Có (Raft leader election) | Có (epoch voting) | N/A |
| Write latency khi scale | Tăng tuyến tính | Flat (không consensus) | Thấp, có thể giảm |
| Write throughput khi scale | Giảm nghiêm trọng | Cao (linear scalability đến 1000 nodes) | Tăng |
| Data loss on failure | Không (committed entries persistent) | Có window (last-failover-wins) | Conflict cần resolve |
| Horizontal scalability | Không (3-5 nodes) | Có (tới 1000) | Có |
| Split-brain handling | Strong (Election Safety) | Eventual (configEpoch conflict resolution) | CRDT merge |
| Phù hợp với | Control plane, metadata | Data cache, session store | Edge, large-scale |

---

## 6. Các điểm MÂU THUẪN giữa các nguồn

### 6.1. Mâu thuẫn thực sự

**Không có mâu thuẫn factual trực tiếp** giữa các nguồn. Các nguồn mô tả các hệ thống khác nhau (Raft, Redis, etcd) nên không chồng chéo claim.

### 6.2. Tension về positioning (không phải mâu thuẫn)

| Chủ đề | Quan điểm của nguồn |
|--------|---------------------|
| **Raft vs Paxos** | [03_1_raft.pdf] claim Raft equivalent về correctness/performance nhưng dễ hiểu hơn, có empirical evidence (user study). Đây là quan điểm của authors — cần caveat khi trích dẫn. |
| **Strong consistency for K8s** | [03_1_raft.pdf] và [03_raft-consensus-algorithm.md] positive về Raft. [10_Rearchitecting_Kubernetes_for_the_Edge.md] phê phán strong consistency cho edge. Không mâu thuẫn — mỗi nguồn nói về use case khác. |
| **Consensus ở data path** | [03_2_Redis_cluster_specification.md] chủ động tránh consensus cho data (để linear scale). [03_1_raft.pdf] implicitly assume consensus cần cho replicated state machine. Đây là design choice khác nhau cho target use case khác nhau. |

### 6.3. Terminology conflict nhỏ

- **"Term" (Raft) vs "Epoch" (Redis)**: Redis spec thừa nhận đây là concept *tương tự* Raft term. [03_2_Redis_cluster_specification.md]
- **Chống double-vote**: Raft không có `lastVoteEpoch`. Trong Raft, mỗi server persist `currentTerm` và `votedFor` — mỗi term chỉ vote cho tối đa 1 candidate [03_1_raft.pdf, Figure 2]. Trong Redis, mỗi master có field `lastVoteEpoch`, từ chối vote nếu `currentEpoch` trong auth request không lớn hơn `lastVoteEpoch`; khi vote, `lastVoteEpoch` được update và lưu xuống disk [03_2_Redis_cluster_specification.md]. Analog đúng: Raft `votedFor` (per `currentTerm`) ↔ Redis `lastVoteEpoch` — cùng purpose chống double-vote, cơ chế lưu trữ khác nhau.

---

## 7. Các điểm CHỈ 1 NGUỒN NHẮC TỚI (cần verify)

### 7.1. Chỉ có trong Raft paper (03_1_raft.pdf)

- **Timing requirement formula** `broadcastTime ≪ electionTimeout ≪ MTBF` — chỉ có ở paper.
- **Paxos comparison claims** (14% longer video, user study numbers) — chỉ authors khẳng định.
- **Khuyến nghị 150–300ms election timeout** — con số cụ thể từ paper benchmark.
- **Joint consensus membership change** — Raft's novel contribution; chỉ paper nhắc.

### 7.2. Chỉ có trong Redis Cluster Spec (03_2_Redis_cluster_specification.md)

- **`FAIL_REPORT_VALIDITY_MULT = 2`** — hệ số cụ thể, không có nguồn thứ 2 verify.
- **Replica rank formula**: `DELAY = 500ms + random(0-500ms) + REPLICA_RANK * 1000ms` — chỉ spec nhắc.
- **Công thức availability probability** `1/(N*2-1)` — chỉ spec nhắc.
- **Khuyến nghị max ~1000 nodes** — chỉ spec.
- **NODE_TIMEOUT × 2** cho wait vote replies — chỉ spec.
- **Sharded Pub/Sub từ Redis 7.0** — version-specific claim.

### 7.3. Chỉ có trong Edge paper (10)

- **Con số ~30% Kubernetes requests là put** — dựa trên benchmark của nhóm tác giả.
- **Cluster size 3-5 recommendation** — trích dẫn etcd docs 2021 (có thể đã outdated).
- **CAP theorem → etcd sacrifices availability** — nhận định standard nhưng nên verify lại với etcd team.

### 7.4. Các khái niệm được nhắc nhưng chưa đi sâu

- **Probabilistically bounded staleness** (Bailis et al. 2012) — cần đọc paper gốc.
- **JSON CRDT** (Kleppmann & Beresford 2017) — cần đọc nếu viết phần proposal.
- **Anna KVS** (Wu et al. 2018) — analogy throughput scale.
- **Viewstamped Replication** — được Raft paper compare nhưng không giải thích chi tiết.

---

## 8. Câu hỏi mở / Thông tin còn thiếu (sau khi bổ sung nguồn)

### 8.1. Gaps ĐÃ LẤP với 2 nguồn mới

- ✅ **Raft message types** — đủ với RequestVote, AppendEntries, InstallSnapshot từ paper.
- ✅ **Raft failure detection** — heartbeat + randomized election timeout từ paper.
- ✅ **Raft safety proof** — 5 safety properties với proof sketch từ paper.
- ✅ **Redis failover voting** — FAILOVER_AUTH_REQUEST/ACK từ spec.
- ✅ **Redis epoch system** — currentEpoch, configEpoch từ spec.
- ✅ **Redis failure detection** — PFAIL/FAIL two-phase từ spec.

### 8.2. Gaps còn lại (ít critical hơn)

**Về Raft:**
- **Pre-vote extension** (Ongaro dissertation) — cách tránh disruption khi partition recover. Paper không đề cập.
- **Leadership transfer** — extension cho planned leader change. Paper không đề cập.
- **Membership change đơn giản hơn** (dissertation vs paper). Paper dùng joint consensus phức tạp; dissertation có phiên bản single-server.
- **Leader lease optimization** — paper chỉ nhắc thoáng qua trong §8.

**Về Redis:**
- **Cluster upgrade / migration procedure** — spec không chi tiết cho online upgrade có breaking changes.
- **configEpoch conflicts resolution algorithm** — có section trong spec nhưng tôi chưa đọc sâu.
- **Replica migration algorithm** — có section trong spec, có thể quan trọng cho Operator design.

**Về CAP / Consistency Models:**
- **Formal statement của CAP theorem** — chưa có trong project knowledge.
- **PACELC extension** — chưa có.
- **Linearizability vs Sequential Consistency vs Causal** hierarchy — chưa có.

### 8.3. Câu hỏi nghiên cứu cụ thể cho luận văn

1. **Layer consensus tách biệt — K8s control plane vs Redis failover**: Kubernetes dùng etcd/Raft cho control plane (Operator state). Redis Cluster có voting riêng cho failover. Đây là 2 layer độc lập. Operator design có nên "tôn trọng" layering này không, hay cố gắng unify? *Case study Kuaishou split data plane khỏi control plane — có ý nghĩa gì trong lý thuyết?*

2. **Redis có thực sự là "consensus-based" system không?** Redis chỉ dùng consensus cho failover, không cho data. Có thể phân loại Redis là "quorum-based failover với eventual consistency data" — đây là một điểm học thuật quan trọng khi review các Redis Operator hiện có.

3. **Operator tolerance với eventual consistency từ etcd**: Nếu [10] đúng — etcd là bottleneck — và nếu Operator reconcile loop có thể rectify errors (theo [10] §4.3), thì Operator liệu có thể được design để tolerate stale reads từ etcd? Implication cho P10K case của Kuaishou.

4. **Redis Cluster failover timing vs. Operator reconcile timing**:
   - Redis `NODE_TIMEOUT` thường 15s (default)
   - Replica election delay: 500ms + random(0-500ms) + rank × 1000ms
   - Typical Operator reconcile: 10-30s
   - **Câu hỏi:** Nếu Operator reconcile nhanh hơn Redis failover, có thể xảy ra tình huống Operator "thấy" master down và quyết định action trước khi Redis tự failover?

5. **Split-brain scenarios crossover**: K8s network partition có thể tạo split brain ở cả 2 layer (etcd và Redis). Làm thế nào Operator xử lý tình huống etcd reports 1 state, Redis Cluster gossip reports state khác?

### 8.4. Connection tới các Topic sau

Nền tảng Topic 1 này sẽ được apply:

- **Topic 2 (K8s + Operator)**: Dùng hiểu biết về etcd/Raft để giải thích control plane behavior.
- **Topic 3 (Reconciliation)**: Idempotency + majority-based eventual convergence — rooted in consensus theory.
- **Topic 4 (InstanceSet)**: Role-based management + failover — liên quan Redis epoch voting.
- **Topic 5 (Kuaishou case)**: "Split data plane from control plane" là direct application của understanding này.

---

## 9. Tài liệu đọc thêm khuyến nghị

Sau khi đã có 2 nguồn lõi, các tài liệu supplementary ưu tiên thấp hơn:

| Priority | Nguồn | Lý do |
|----------|-------|-------|
| ⭐⭐ P1 | **Ongaro PhD Dissertation** (2014) | Bổ sung pre-vote, leadership transfer, membership change đơn giản hơn |
| ⭐⭐ P1 | **Bailis et al. 2012** - PBS | Giải thích claim "eventual consistency thường fresh" |
| ⭐ P2 | **CAP theorem formal** (Brewer/Gilbert-Lynch) | Bổ sung rigor cho phần consistency model |
| ⭐ P2 | **Kleppmann & Beresford 2017** - JSON CRDT | Nếu luận văn có phần compare alternatives cho etcd |
| ⭐ P2 | **Redis source code** (cluster.c) | Verify các claim từ spec với code thực tế |

---

## 10. Kết luận sau khi bổ sung

**Độ phủ project knowledge cho Topic 1:** ~**90%** (tăng từ 40%).

- ✅ **Cover rất tốt**: Raft algorithm (states, terms, RPCs, safety, timing, benchmark); Redis failover voting (epoch, PFAIL/FAIL, election); etcd trade-off analysis; eventual consistency alternative.
- ✅ **Cover tốt**: So sánh Raft vs Redis failover; use case differences (control plane vs data plane).
- ⚠️ **Cover một phần**: CAP theorem formal; các consensus variants (EPaxos, CASPaxos, WPaxos) được reference nhưng không chi tiết.
- ❌ **Chưa cover**: Formal consistency hierarchy; Byzantine fault tolerance (không cần cho đề tài này).

**Implication cho luận văn:** Topic 1 giờ có thể viết thành chương nền tảng ~15-20 trang đầy đủ, với core sources là 2 file vừa bổ sung. Chỉ cần 2-3 nguồn bên ngoài (dissertation, CAP paper) để hoàn thiện các concept còn thiếu.

**Điểm mạnh nổi bật:** Hai cơ chế consensus khác nhau (Raft và Redis epoch-based) giờ có thể được trình bày song song với đầy đủ detail để so sánh — đây là một chương thú vị cho đóng góp learning.

---

*Tài liệu này được tổng hợp trung thực từ nội dung có trong project knowledge (tổng hợp 24/04/2026; rà soát & hiệu đính 09/08/2026). Mọi claim đều có source tag ở dạng [file_name] hoặc [file_name, §section]; các suy luận (không phải trích dẫn trực tiếp) được đánh dấu rõ.*
