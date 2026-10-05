# Distributed Key-Value Store

A Redis-like distributed key-value store built entirely from scratch in **C++17** — no external libraries, no frameworks. Uses raw TCP sockets, OS-level threading, and a custom replication protocol to distribute data across a 3-node cluster.

Built as a placement project to demonstrate practical systems programming: data structures, concurrency, networking, and distributed systems — all in one codebase.

---

## What It Does

Clients connect over TCP and send simple text commands:

```
SET city Chennai      → OK
GET city              → Chennai
DEL city              → OK
PING                  → PONG
STATUS                → live replication health report
KEYS                  → all keys currently stored
```

Every write is **synchronously replicated** to backup nodes before the client receives `OK`. A background Health Monitor continuously watches replica health and drives recovery: if a backup goes down and comes back — even while the primary keeps running, with no restart required — it is automatically detected and caught up on missed writes using sequence-numbered WAL entries, typically within a few seconds.

---

## Architecture

```
                    ┌─────────────┐
                    │   Client    │
                    │ (PowerShell │
                    │  / telnet)  │
                    └──────┬──────┘
                           │ TCP :9001
                    ┌──────▼──────┐
                    │   Primary   │  ← handles all writes
                    │  :9001      │  ← sequences every write
                    │  WAL log    │  ← persists to disk
                    └──────┬──────┘
               ┌───────────┴───────────┐
               │ SEQWRITE (replicate)  │
        ┌──────▼──────┐         ┌──────▼──────┐
        │  Backup 1   │         │  Backup 2   │
        │   :9002     │         │   :9003     │
        │  WAL log    │         │  WAL log    │
        └─────────────┘         └─────────────┘
```

**Write path (strict order):**
1. Client sends `SET city Chennai` to primary
2. Primary sends `SEQWRITE 5 SET city Chennai` to every **HEALTHY** backup — waits for OK (a backup that's still catching up is skipped, not blocked on)
3. Primary applies write to its own LRU engine
4. Primary appends `5 SET city Chennai` to WAL file
5. Primary replies `OK` to client

Reads (`GET`, `KEYS`) are served locally by the primary — no network call to backups.

A backup only receives live writes while its tracked state is `HEALTHY`. A backup that's unreachable, still initializing, or mid-recovery is excluded from step 2 until the Health Monitor brings it back — see [Replica State Machine & Health Monitor](#replica-state-machine--health-monitor) below.

---

## Technical Design Decisions

### LRU Cache — O(1) for everything

The storage engine uses a doubly-linked list + hashmap combination. The list tracks recency (front = most recently used, back = evict next). The hashmap stores each key's value **and** an iterator pointing to its exact position in the list.

Storing the iterator is the key insight: erasing a node from the middle of a linked list is O(1) only if you already hold a pointer to it. Without this, every access would require an O(n) scan.

```cpp
unordered_map<string, pair<string, list<string>::iterator>> cacheMap;
//                          ▲ value        ▲ direct pointer into list
```

### Thread Safety — one mutex, both structures

`std::lock_guard` protects both the list and the map together as a unit. Two separate locks would create a window where one structure is updated and the other is not — a classic TOCTOU race condition. `lock_guard` is used over manual `lock()/unlock()` because it is RAII: the lock releases automatically even if an exception is thrown.

```cpp
string get(const string& key) {
    lock_guard<mutex> lock(mtx);  // releases on scope exit, even on exception
    // safe to touch both list and map here
}
```

### TCP Stream Problem — leftover buffer

TCP is a byte stream, not a message protocol. One `send()` on the client does not guarantee one `recv()` on the server. A command can arrive split across multiple `recv()` calls:

```
recv #1:  "SET ci"          ← incomplete
recv #2:  "ty Chennai\n"    ← now complete
```

Every `recv()` chunk is appended to a `leftover` string. Commands are only extracted when a `\n` is found. Anything after the last `\n` stays in `leftover` for the next `recv()`.

### Multi-Client — thread-per-connection with detach()

The main thread loops on `accept()` forever. Each new client gets a `std::thread` running `handleClient()`, then `detach()` is called so the main thread returns to `accept()` immediately.

`join()` would block the main thread until client 1 disconnects — defeating the purpose entirely. `detach()` lets each worker run independently.

Lambda captures `clientSock` and `clientId` **by value** — capturing by reference would be a data race since the loop variables change on the next iteration before the thread reads them.

### Replication — fresh connection per write

The primary opens a fresh TCP connection to each backup for every write, sends the command, reads the ACK, and closes the connection. This avoids Winsock thread-affinity issues on Windows where a socket created on one thread cannot be safely reused on another.

### Sequence-Numbered WAL

Every write is assigned a monotonically increasing sequence number. WAL entries are stored as:
```
1 SET city Chennai
2 SET lang C++
3 DEL lang
```

On startup, the primary asks each backup for its current sequence (`GETSEQ`). If a backup is behind, the primary reads all WAL entries after the backup's last sequence and sends them as `SEQWRITE` commands. This is the same catch-up mechanism used in MySQL binlog replication.

The same routine also runs continuously at **runtime**, not just at startup — see the next section.

### Replica State Machine & Health Monitor

Each backup the primary tracks has a `ReplicaState`:

```
INITIALIZING → HEALTHY                         (startup, reachable)
INITIALIZING → UNREACHABLE                      (startup, unreachable)
UNREACHABLE  → NEEDS_SYNC                       (Health Monitor: GETSEQ succeeds)
NEEDS_SYNC   → RECOVERING → HEALTHY             (catch-up reaches the primary's live seq)
NEEDS_SYNC   → RECOVERING → NEEDS_SYNC          (catch-up still behind — retried next tick)
HEALTHY      → NEEDS_SYNC                       (live write gets a GAP reply)
HEALTHY      → UNREACHABLE                      (live write fails / times out)
```

Only a `HEALTHY` backup receives live writes — `replicateWrite()` skips any backup whose state isn't `HEALTHY`, so a recovering or unreachable replica never blocks or corrupts the live write path.

A background **Health Monitor thread** (`ReplicationManager::healthMonitorLoop`), started once alongside the server, wakes up every 2 seconds (`HEALTH_CHECK_INTERVAL_SECONDS`) and drives the transitions above:
- `UNREACHABLE` → sends `GETSEQ`; if the backup answers, moves to `NEEDS_SYNC`.
- `NEEDS_SYNC` → calls `catchUpBackup()` — **the exact same routine `checkAndCatchUp()` uses at startup** (startup is now just a loop calling `catchUpBackup()` once per registered backup). It re-queries the backup's real sequence, replays whatever's missing from the WAL, and resolves to `HEALTHY` once the backup reaches the primary's *live* sequence counter, or leaves it at `NEEDS_SYNC` to retry on the next tick.

Practical effect: a backup that crashes and restarts **while the primary keeps running** is now recovered automatically, typically within a few seconds — no primary restart required. All of this network I/O (`GETSEQ`/`SEQWRITE`) runs with the backups lock released, so a slow or recovering backup never blocks writes going to the other, healthy backups.

### Gap Detection & Idempotent Writes

Every `SEQWRITE`/`GETSEQ` round-trip the primary makes is classified into a `ReplicationResult`: `SUCCESS`, `GAP`, `UNREACHABLE`, or `FAILED`.

The backup enforces strict ordering on `SEQWRITE <seq> ...` against its own `lastSeq`:

| Condition | Backup does | Reply |
|---|---|---|
| `seq == lastSeq + 1` | Applies the write, persists it, advances `lastSeq` | `OK` |
| `seq <= lastSeq` | Nothing — already applied | `OK` (idempotent no-op) |
| `seq > lastSeq + 1` | Nothing — refuses to apply out of order | `GAP <lastSeq>` |

The duplicate case makes `SEQWRITE` safe to receive twice (e.g. an entry re-sent during catch-up). The gap case means a live write can never silently create a hole in a backup's data — if the primary sees a `GAP` reply, it marks that backup `NEEDS_SYNC` (not `UNREACHABLE` — the socket is fine, the backup is just behind) and the Health Monitor picks it up on its next tick.

### SO_REUSEADDR

Added immediately after `socket()` on both primary and backup. Without it, killing the server with Ctrl+C and restarting within 60 seconds gives "Address already in use" because the OS holds the port in TIME_WAIT. This option bypasses TIME_WAIT for immediate restart.

---

## Project Structure

```
distributed-kv-store/
├── primary_node.cpp      # Primary node — port 9001
│                         # Handles client writes, replicates to backups
│                         # Sequence-numbered WAL, STATUS, KEYS commands
│
├── backup_node.cpp       # Backup node — port 9002 or 9003
│                         # Applies SEQWRITE commands from primary
│                         # GETSEQ protocol for catch-up on restart
│
├── kv_server_week4.cpp   # Standalone multi-client server — port 8080
│                         # Week 4 deliverable — no replication
│                         # Good for testing the core engine in isolation
│
└── test_week5.ps1        # PowerShell test script for Windows
                          # Tests primary writes, backup replication,
                          # WAL persistence — all with coloured output
```

---

## Build Instructions

### Windows (Sublime Text + MinGW-w64)

**Build system** (`Tools → Build System → New Build System`):
```json
{
  "cmd": [
    "C:\\mingw64\\mingw64\\bin\\g++.exe",
    "-std=c++17",
    "-o", "${file_base_name}.exe",
    "${file}",
    "-lws2_32"
  ],
  "working_dir": "${file_path}",
  "selector": "source.c++"
}
```

Open each `.cpp` file in Sublime Text → `Ctrl+B` to compile.

### Linux / WSL

```bash
g++ -std=c++17 -pthread -o backup_node  backup_node.cpp
g++ -std=c++17 -pthread -o primary_node primary_node.cpp
g++ -std=c++17 -pthread -o kv_server    kv_server_week4.cpp
```

---

## Running the Cluster

**Start in this order — backups must be ready before primary starts:**

```bash
# Terminal 1
./backup_node 9002

# Terminal 2
./backup_node 9003

# Terminal 3
./primary_node
```

Primary output confirms both backups connected:
```
╔══════════════════════════════════════════╗
║   PRIMARY NODE  listening on port 9001  ║
║   Starting at sequence: 0               ║
╚══════════════════════════════════════════╝

[Replication] Registered backup 127.0.0.1:9002
[Replication] Registered backup 127.0.0.1:9003
[Startup] Checking backups and catching up...
[Replication] Backup :9002 is at seq 0 (primary at 0)
[Replication] Backup :9003 is at seq 0 (primary at 0)
```

---

## Testing

### Windows — PowerShell test script

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\test_week5.ps1
```

Expected output:
```
PASS: city=Chennai replicated to backup 9002
PASS: DEL propagated to backup 9002
PASS: city=Chennai replicated to backup 9003
PASS: WAL file kv_primary.log exists
PASS: WAL contains 4 entries
```

### Linux — manual test

```bash
# Write to primary
echo -e "SET city Chennai\nSET lang C++\nGET city" | nc 127.0.0.1 9001

# Read from backup (proves replication)
echo -e "GET city\nGET lang" | nc 127.0.0.1 9002
```

### STATUS command — live replication health

```
STATUS
PRIMARY seq=5
BACKUP :9002 seq=5 state=HEALTHY behind=0
BACKUP :9003 seq=5 state=HEALTHY behind=0
END
```

### KEYS command — list all stored keys

```
KEYS
city
project
lang
END
```

---

## Catch-Up Replication Demo

This shows **live** recovery when a backup crashes and restarts — the primary is never restarted:

```bash
# 1. Write 3 keys — all nodes at seq=3
SET city Chennai
SET lang C++
SET year 2025

# 2. Kill backup :9002 (Ctrl+C in its terminal)
#    Its next write attempt fails → primary marks it UNREACHABLE

# 3. Write 2 more keys — only :9003 receives these (replicateWrite
#    skips any backup that isn't HEALTHY)
SET college IIT
SET project KVStore

# 4. STATUS shows :9002 is behind
STATUS
→ BACKUP :9002 seq=3 state=UNREACHABLE behind=2
→ BACKUP :9003 seq=5 state=HEALTHY     behind=0

# 5. Restart backup :9002 — the PRIMARY IS NOT RESTARTED
./backup_node 9002

# 6. Within a few seconds, the Health Monitor (ticks every 2s) notices
#    :9002 is reachable again, moves it UNREACHABLE → NEEDS_SYNC, then
#    calls the same catchUpBackup() routine startup uses to replay
#    entries 4 and 5 — resolving to HEALTHY once it matches the
#    primary's live sequence.

# 7. Verify :9002 caught up
GET college   → IIT        (was written while backup was down)
GET project   → KVStore    (was written while backup was down)

# 8. Confirm via STATUS — no manual intervention was needed
STATUS
→ BACKUP :9002 seq=5 state=HEALTHY behind=0
→ BACKUP :9003 seq=5 state=HEALTHY behind=0
```

---

## Command Reference

| Command | Example | Response | Notes |
|---------|---------|----------|-------|
| `SET` | `SET city Chennai` | `OK` | Values can contain spaces |
| `GET` | `GET city` | `Chennai` or `NOT_FOUND` | Promotes key to MRU in LRU |
| `DEL` | `DEL city` | `OK` or `NOT_FOUND` | Propagated to all backups |
| `PING` | `PING` | `PONG` | Health check |
| `STATUS` | `STATUS` | Multi-line report | Shows seq numbers, replica state |
| `KEYS` | `KEYS` | List of keys + `END` | All current keys in MRU order |

Commands are **case-insensitive** — `set`, `Set`, `SET` all work.

---

## Known Limitations and Future Scope

**Current limitations (intentional trade-offs for simplicity):**

- **No Raft consensus** — if the primary crashes, a backup must be manually promoted. Raft would automate leader election. (Backup *recovery* is now automatic — see [Replica State Machine & Health Monitor](#replica-state-machine--health-monitor) — but primary failover is not.)
- **Thread-per-connection** — spawns one OS thread per client. A production system uses a thread pool to cap resource usage.
- **No client library** — clients must speak the raw TCP protocol. A thin Python or C++ client library would simplify integration.
- **Fixed-interval health polling** — the Health Monitor ticks every 2 seconds (`HEALTH_CHECK_INTERVAL_SECONDS`) regardless of how many backups are down; there's no exponential backoff and no push-based "I'm back" notification from the backup, just periodic polling.
- **Connect-per-write replication** — each live write still opens a fresh TCP connection to every `HEALTHY` backup rather than reusing a persistent one, so every write pays a handshake per backup.

**Planned improvements:**

| Feature | Why | How |
|---------|-----|-----|
| Raft consensus | Automatic *primary* failover if it dies (backup recovery is already automatic) | Implement leader election + log replication |
| Thread pool | Bounded resource usage under load | Fixed-size worker queue, condition variables |
| Persistent replication connections | Avoid a handshake on every write | Keep one open socket per backup, reconnect on failure |
| TTL / expiry | Redis-style `EXPIRE key seconds` | Background thread + expiry map |
| Bloom filter | Skip lock acquisition for missing keys | Probabilistic membership check before LRU |
| HTTP/REST API | Language-agnostic client access | Minimal HTTP parser on top of existing TCP layer |
| Benchmarking | Measure throughput and latency | Multi-threaded load generator, p50/p99 metrics |

---

## Key Concepts Demonstrated

| Concept | Where | What it shows |
|---------|-------|---------------|
| LRU Cache | `LRUStorageEngine` | O(1) ops using list iterator stored in hashmap |
| Thread safety | `lock_guard<mutex>` | RAII locking protecting two structures as one unit |
| TCP streams | `leftover` buffer | Accumulation until `\n` — solves fragmentation |
| Thread-per-connection | `accept()` loop + `detach()` | Multi-client without blocking the accept loop |
| Synchronous replication | `replicateWrite()` | Write-ahead replication for strong consistency |
| WAL persistence | `PersistenceManager` | Append-only O(1) write, O(n) replay on restart |
| Sequence numbers | `atomic<long long> seq` | Total ordering of all writes across the cluster |
| Catch-up replication | `GETSEQ` + `SEQWRITE` | Stale backup detects gap, primary fills it |
| Replica state machine | `ReplicaState`, `ReplicationResult` | INITIALIZING/HEALTHY/NEEDS_SYNC/RECOVERING/UNREACHABLE — only HEALTHY gets live writes |
| Background health monitoring | `ReplicationManager::healthMonitorLoop` | Joinable thread (joined in `~ReplicationManager`), 2s tick, drives recovery without a primary restart |
| Idempotent + gap-safe replication | `SEQWRITE` handler in `backup_node.cpp` | Duplicate `seq` is a no-op; out-of-order `seq` is rejected with `GAP <lastSeq>` |
| Lock-free recovery I/O | `catchUpBackup()` | Never holds `backupMtx` across a blocking socket call, so recovery can't stall live writes |
| Cross-platform sockets | `#ifdef _WIN32` | Same source compiles on Windows and Linux |

---

## Why This Project

Most placement projects are web CRUD apps — create a user, read it back, update it, delete it, over HTTP with a database doing all the hard work. This project goes one level lower: **the database itself**, with the networking, the thread management, and the consistency protocol all written from scratch.

Every design decision here has a concrete reason: why `lock_guard` over `atomic`, why `detach()` over `join()`, why append-only over overwrite, why synchronous replication over async. Being able to explain the *why* behind each choice is what separates systems engineers from web developers in placement interviews.

---

*Built in C++17 — no external libraries — all 22 automated tests passing*
