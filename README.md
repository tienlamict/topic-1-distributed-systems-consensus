# Demo Raft Consensus — minh họa Topic 1

Project Go minh họa **trực quan** các cơ chế consensus trong tài liệu
[`topic-1-distributed-systems-consensus.md`](topic-1-distributed-systems-consensus.md):
leader election, log replication, failover, network partition, và các safety property.

Cluster 3 node chạy trong **một tiến trình** (mỗi node là một goroutine, liên lạc qua
mạng giả lập in-memory) nên chỉ cần **một lệnh** để xem toàn bộ luồng.

## Chạy

```powershell
go run ./cmd/demo
```

Demo tự diễn 5 màn (~13s), in log theo thời gian thực:

1. **Leader election** lần đầu — randomized timeout (§2.2, §4.1)
2. **Log replication** + commit qua majority (§2.2, §3.1.2)
3. **Kill leader → re-election** — failure detection qua missing heartbeat (§4.1)
4. **Network partition** — minority KHÔNG commit được (§1.2, §5.5)
5. **Heal** → cluster hội tụ, committed entry không mất (Leader Completeness §5.2)

## Cấu trúc

| File | Nội dung |
|------|----------|
| `raft/types.go` | States, terms, log entry, RPC messages (§2.1, §3.1) |
| `raft/network.go` | Mạng in-memory + partition (§4, §5.5) |
| `raft/node.go` | Lõi Raft: election, replication, commit an toàn |
| `cmd/demo/main.go` | Kịch bản 5 màn |
| `docs/raft-demo-giai-thich.md` | **Giải thích chi tiết luồng + liên hệ lý thuyết** |

## Tài liệu

Đọc [`docs/raft-demo-giai-thich.md`](docs/raft-demo-giai-thich.md) để hiểu từng bước
hoạt động và đối chiếu với từng mục **§x.y** trong file lý thuyết gốc.
