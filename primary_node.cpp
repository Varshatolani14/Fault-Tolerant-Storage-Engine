/*
 * ============================================================
 *  Distributed KV Store — Primary Node (Final Version)
 * ============================================================
 *
 *  NEW IN FINAL VERSION
 *  --------------------
 *  1. Sequence-numbered WAL
 *     Every write gets a monotonically increasing sequence
 *     number. WAL entries: "1 SET city Chennai"
 *     Primary tracks its own seq; backups report theirs.
 *
 *  2. STATUS command
 *     Returns primary seq, each backup's last-known seq,
 *     and how many writes each backup is behind.
 *     Usage: STATUS
 *
 *  3. KEYS command
 *     Returns all keys currently in the LRU store.
 *     Usage: KEYS
 *
 *  4. Backup catch-up on reconnect
 *     When a backup responds to PING with its sequence
 *     number, the primary replays missing WAL entries.
 *
 *  COMPILE (Windows):
 *     g++ -std=c++17 -o primary_node.exe primary_node.cpp -lws2_32
 *
 *  COMPILE (Linux/WSL):
 *     g++ -std=c++17 -pthread -o primary_node primary_node.cpp
 *
 *  USAGE:
 *     primary_node.exe <primaryPort> <backup1Port> <backup2Port>
 *     WAL file is auto-named "kv_primary_<primaryPort>.log", so
 *     multiple independent shard primaries can share one directory.
 *
 *  RUN ORDER (per shard):
 *     1. backup_node.exe <backup1Port>
 *     2. backup_node.exe <backup2Port>
 *     3. primary_node.exe <primaryPort> <backup1Port> <backup2Port>
 *
 *  EXAMPLE (two shards):
 *     backup_node.exe 9002   backup_node.exe 9003
 *     primary_node.exe 9001 9002 9003
 *
 *     backup_node.exe 9012   backup_node.exe 9013
 *     primary_node.exe 9011 9012 9013
 *
 *  COMMANDS:
 *     SET <key> <value>   GET <key>   DEL <key>
 *     PING                STATUS      KEYS
 *
 * ============================================================
 */

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <thread>
#include <mutex>
#include <chrono>
#include <list>
#include <unordered_map>
#include <algorithm>
#include <cstring>
#include <atomic>

#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #pragma comment(lib, "Ws2_32.lib")
    using socket_t = SOCKET;
    #define CLOSE_SOCKET(s) closesocket(s)
    #define SOCK_INVALID    INVALID_SOCKET
    #define SOCK_ERR        SOCKET_ERROR
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    using socket_t = int;
    #define CLOSE_SOCKET(s) close(s)
    #define SOCK_INVALID    (-1)
    #define SOCK_ERR        (-1)
#endif

using namespace std;

// ============================================================
//  LRUStorageEngine
//  Added: keys() method for KEYS command
// ============================================================
class LRUStorageEngine {
private:
    int capacity;
    list<string> usageList;
    unordered_map<string, pair<string, list<string>::iterator>> cacheMap;
    mutex mtx;

public:
    explicit LRUStorageEngine(int cap) : capacity(cap) {}

    void put(const string& key, const string& value) {
        lock_guard<mutex> lock(mtx);
        auto it = cacheMap.find(key);
        if (it != cacheMap.end()) {
            usageList.erase(it->second.second);
            usageList.push_front(key);
            it->second = {value, usageList.begin()};
            return;
        }
        if ((int)cacheMap.size() >= capacity) {
            cout << "[Eviction] Removing: " << usageList.back() << "\n";
            cacheMap.erase(usageList.back());
            usageList.pop_back();
        }
        usageList.push_front(key);
        cacheMap[key] = {value, usageList.begin()};
    }

    string get(const string& key) {
        lock_guard<mutex> lock(mtx);
        auto it = cacheMap.find(key);
        if (it == cacheMap.end()) return "NOT_FOUND";
        usageList.erase(it->second.second);
        usageList.push_front(key);
        it->second.second = usageList.begin();
        return it->second.first;
    }

    string del(const string& key) {
        lock_guard<mutex> lock(mtx);
        auto it = cacheMap.find(key);
        if (it == cacheMap.end()) return "NOT_FOUND";
        usageList.erase(it->second.second);
        cacheMap.erase(it);
        return "OK";
    }

    // NEW: return all keys in MRU order
    vector<string> keys() {
        lock_guard<mutex> lock(mtx);
        return vector<string>(usageList.begin(), usageList.end());
    }

    int size() {
        lock_guard<mutex> lock(mtx);
        return (int)cacheMap.size();
    }
};

// ============================================================
//  PersistenceManager
//  NEW: sequence-numbered WAL entries
//  Format: "<seq> SET <key> <value>"  or  "<seq> DEL <key>"
// ============================================================
class PersistenceManager {
private:
    string logPath;
    mutex  fileMtx;

public:
    explicit PersistenceManager(const string& p) : logPath(p) {}

    //
    // Replay the WAL into the engine on startup.
    // Returns the highest sequence number found in the log
    // so the primary knows where to continue from.
    //
    long long replay(LRUStorageEngine& engine) {
        ifstream in(logPath);
        if (!in.is_open()) {
            cout << "[WAL] No log found — starting fresh.\n";
            return 0;
        }
        string line;
        int count = 0;
        long long maxSeq = 0;

        while (getline(in, line)) {
            if (line.empty()) continue;
            istringstream iss(line);

            // Try to parse new format: "<seq> <cmd> <key> [value]"
            // Fall back to old format: "<cmd> <key> [value]"
            long long seq = 0;
            string first;
            iss >> first;

            string cmd, key, value;
            // If first token is a number it is the sequence
            bool hasSeq = !first.empty() &&
                          first.find_first_not_of("0123456789") == string::npos;
            if (hasSeq) {
                seq = stoll(first);
                iss >> cmd;
            } else {
                cmd = first;
            }
            iss >> key;

            if (cmd == "SET") {
                getline(iss, value);
                if (!value.empty() && value[0]==' ') value.erase(0,1);
                engine.put(key, value);
            } else if (cmd == "DEL") {
                engine.del(key);
            }

            if (seq > maxSeq) maxSeq = seq;
            ++count;
        }
        cout << "[WAL] Replayed " << count
             << " entries. Last sequence: " << maxSeq << "\n";
        return maxSeq;
    }

    // Read the full WAL as raw lines (for catch-up replication)
    vector<pair<long long, string>> readFrom(long long afterSeq) {
        vector<pair<long long, string>> entries;
        ifstream in(logPath);
        if (!in.is_open()) return entries;
        string line;
        while (getline(in, line)) {
            if (line.empty()) continue;
            istringstream iss(line);
            string first; iss >> first;
            bool hasSeq = !first.empty() &&
                          first.find_first_not_of("0123456789") == string::npos;
            if (hasSeq) {
                long long seq = stoll(first);
                if (seq > afterSeq) {
                    // rest of line after the sequence number
                    string rest;
                    getline(iss, rest);
                    if (!rest.empty() && rest[0]==' ') rest.erase(0,1);
                    entries.push_back({seq, rest});
                }
            }
        }
        return entries;
    }

    // Append a sequenced write entry
    void append(long long seq, const string& cmd,
                const string& key, const string& value = "") {
        lock_guard<mutex> lock(fileMtx);
        ofstream out(logPath, ios::app);
        if (value.empty())
            out << seq << " " << cmd << " " << key << "\n";
        else
            out << seq << " " << cmd << " " << key << " " << value << "\n";
    }
};

// ============================================================
//  ReplicaState
//  Tracks the lifecycle state of a single backup replica.
// ============================================================
enum class ReplicaState {
    INITIALIZING,
    HEALTHY,
    NEEDS_SYNC,
    RECOVERING,
    UNREACHABLE
};

static string replicaStateToString(ReplicaState s) {
    switch (s) {
        case ReplicaState::INITIALIZING: return "INITIALIZING";
        case ReplicaState::HEALTHY:      return "HEALTHY";
        case ReplicaState::NEEDS_SYNC:   return "NEEDS_SYNC";
        case ReplicaState::RECOVERING:   return "RECOVERING";
        case ReplicaState::UNREACHABLE:  return "UNREACHABLE";
    }
    return "UNKNOWN";
}

// ============================================================
//  ReplicationResult
//  Structured outcome of a single sendRaw() round-trip.
// ============================================================
enum class ReplicationResult {
    SUCCESS,
    GAP,
    UNREACHABLE,
    FAILED
};

// ============================================================
//  ReplicationManager
//  NEW: tracks each backup's last-known sequence number
//       and supports catch-up replication
// ============================================================
class ReplicationManager {
private:
    struct BackupInfo {
        string       host;
        int          port;
        long long    lastSeq = 0;   // last sequence this backup confirmed
        ReplicaState state   = ReplicaState::INITIALIZING;
    };

    vector<BackupInfo> backups;
    mutex backupMtx;

    // Send one command to one backup over a fresh connection.
    // Returns the response string or "" on failure.
    string sendRaw(const string& host, int port, const string& cmd) {
        socket_t s = socket(AF_INET, SOCK_STREAM, 0);
        if (s == SOCK_INVALID) return "";

#ifdef _WIN32
        DWORD tv = 2000;
        setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, (char*)&tv, sizeof(tv));
        setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, (char*)&tv, sizeof(tv));
#else
        struct timeval tv = {2, 0};
        setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(s, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
#endif

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_port   = htons(port);
        inet_pton(AF_INET, host.c_str(), &addr.sin_addr);

        if (connect(s, (sockaddr*)&addr, sizeof(addr)) != 0) {
            CLOSE_SOCKET(s);
            return "";
        }

        string msg = cmd + "\n";
        if (send(s, msg.c_str(), (int)msg.size(), 0) == SOCK_ERR) {
            CLOSE_SOCKET(s);
            return "";
        }

        char buf[256] = {0};
        int n = recv(s, buf, sizeof(buf)-1, 0);
        CLOSE_SOCKET(s);
        if (n <= 0) return "";

        string resp(buf, n);
        while (!resp.empty() &&
               (resp.back()=='\n'||resp.back()=='\r'||resp.back()==' '))
            resp.pop_back();
        return resp;
    }

    // Classify a sendRaw() response into a structured result.
    // "" (socket failure / timeout) -> UNREACHABLE
    // "OK..."  -> SUCCESS
    // "GAP..." -> GAP
    // anything else -> FAILED
    static ReplicationResult classifyResponse(const string& resp) {
        if (resp.empty())                return ReplicationResult::UNREACHABLE;
        if (resp.substr(0,2) == "OK")    return ReplicationResult::SUCCESS;
        if (resp.substr(0,3) == "GAP")   return ReplicationResult::GAP;
        return ReplicationResult::FAILED;
    }

    // ── Health Monitor ──────────────────────────────────────
    // Background thread: probes UNREACHABLE backups and drives
    // recovery for NEEDS_SYNC backups via catchUpBackup(). All
    // blocking network I/O below runs WITHOUT backupMtx held — the
    // mutex is only ever taken to copy fields out or commit results
    // back, so live writes in replicateWrite() never wait on
    // recovery I/O against another backup.
    static constexpr int HEALTH_CHECK_INTERVAL_SECONDS = 2;

    PersistenceManager* monitorPersistence = nullptr;
    atomic<long long>*  monitorPrimarySeq  = nullptr;
    atomic<bool>        monitorRunning{false};
    thread              monitorThread;

    // GETSEQ round-trip, no BackupInfo/lock involved.
    bool queryBackupSeq(const string& host, int port, long long& outSeq) {
        string resp = sendRaw(host, port, "GETSEQ");
        if (resp.empty() || resp.substr(0, 3) != "SEQ") return false;
        try { outSeq = stoll(resp.substr(4)); }
        catch (...) { return false; }
        return true;
    }

    // Single SEQWRITE round-trip, no BackupInfo/lock involved.
    ReplicationResult sendSeqWrite(const string& host, int port,
                                    long long seq, const string& cmd) {
        string resp = sendRaw(host, port, "SEQWRITE " + to_string(seq) + " " + cmd);
        return classifyResponse(resp);
    }

    // The single recovery routine, used by both checkAndCatchUp()
    // (startup, one call per backup) and the Health Monitor's
    // NEEDS_SYNC branch (runtime, one call per tick). Queries the
    // backup's real sequence, replays whatever it's missing from
    // the WAL, and resolves the final state — all unlocked during
    // I/O; backupMtx is only taken to read host/port and to commit
    // the outcome.
    //
    // Transitions: (INITIALIZING|NEEDS_SYNC) -> RECOVERING, then
    // RECOVERING -> HEALTHY on success or RECOVERING -> NEEDS_SYNC
    // if still behind. Unreachable backups go straight to UNREACHABLE.
    void catchUpBackup(size_t index, PersistenceManager& persistence,
                        atomic<long long>& currentSeq) {
        string host; int port = 0;
        {
            lock_guard<mutex> lock(backupMtx);
            if (index >= backups.size()) return;
            host = backups[index].host;
            port = backups[index].port;
        }

        long long backupSeq = 0;
        if (!queryBackupSeq(host, port, backupSeq)) {
            cerr << "[Replication] Backup " << host << ":" << port
                 << " unreachable.\n";
            lock_guard<mutex> lock(backupMtx);
            if (index < backups.size()) backups[index].state = ReplicaState::UNREACHABLE;
            return;
        }

        // Reachable — actively recovering from here on.
        {
            lock_guard<mutex> lock(backupMtx);
            if (index >= backups.size()) return;
            backups[index].state   = ReplicaState::RECOVERING;
            backups[index].lastSeq = backupSeq;
        }

        cout << "[Replication] Backup :" << port
             << " is at seq " << backupSeq
             << " (primary at " << currentSeq.load() << ")\n";

        long long newLastSeq = backupSeq;
        if (backupSeq < currentSeq.load()) {
            auto missing = persistence.readFrom(backupSeq);
            cout << "[Replication] Sending " << missing.size()
                 << " catch-up entries to backup :" << port << "\n";
            for (auto& [seq, cmd] : missing) {
                ReplicationResult r = sendSeqWrite(host, port, seq, cmd);
                if (r != ReplicationResult::SUCCESS) break;
                newLastSeq = seq;
            }
            cout << "[Replication] Catch-up complete for backup :"
                 << port << "\n";
        } else {
            cout << "[Replication] Backup :" << port << " is up to date.\n";
        }

        // Compare against the primary's latest committed sequence,
        // not a stale snapshot — writes may have landed while we
        // were replaying.
        long long primarySeq = currentSeq.load();

        lock_guard<mutex> lock(backupMtx);
        if (index >= backups.size()) return;
        BackupInfo& b = backups[index];
        b.lastSeq = newLastSeq;

        if (b.lastSeq == primarySeq) {
            b.state = ReplicaState::HEALTHY;
            cout << "[Replication] Backup :" << b.port
                 << " caught up to seq=" << primarySeq << " — HEALTHY\n";
        } else {
            b.state = ReplicaState::NEEDS_SYNC;
            cout << "[Replication] Backup :" << b.port
                 << " still behind (seq=" << b.lastSeq
                 << ", primary=" << primarySeq << ") — NEEDS_SYNC\n";
        }
    }

    void healthMonitorLoop() {
        while (monitorRunning.load()) {
            size_t count;
            {
                lock_guard<mutex> lock(backupMtx);
                count = backups.size();
            }

            for (size_t i = 0; i < count && monitorRunning.load(); ++i) {
                ReplicaState state = ReplicaState::INITIALIZING;
                string host; int port = 0;
                {
                    lock_guard<mutex> lock(backupMtx);
                    if (i >= backups.size()) break;
                    state = backups[i].state;
                    host  = backups[i].host;
                    port  = backups[i].port;
                }

                // RECOVERING only exists transiently inside a
                // catchUpBackup() call made by this same thread, so
                // it is never observed here — treated as a no-op
                // alongside HEALTHY/INITIALIZING for safety.
                if (state == ReplicaState::HEALTHY ||
                    state == ReplicaState::INITIALIZING ||
                    state == ReplicaState::RECOVERING) {
                    continue; // (a) nothing to do
                }

                if (state == ReplicaState::UNREACHABLE) {
                    long long backupSeq = 0;
                    if (queryBackupSeq(host, port, backupSeq)) {
                        lock_guard<mutex> lock(backupMtx);
                        if (i < backups.size() &&
                            backups[i].state == ReplicaState::UNREACHABLE) {
                            backups[i].state   = ReplicaState::NEEDS_SYNC;
                            backups[i].lastSeq = backupSeq;
                            cout << "[HealthMonitor] Backup :" << port
                                 << " reachable again (seq=" << backupSeq
                                 << ") — NEEDS_SYNC\n";
                        }
                    }
                    continue;
                }

                if (state == ReplicaState::NEEDS_SYNC) {
                    if (monitorPersistence && monitorPrimarySeq) {
                        catchUpBackup(i, *monitorPersistence, *monitorPrimarySeq);
                    }
                    continue;
                }
            }

            this_thread::sleep_for(chrono::seconds(HEALTH_CHECK_INTERVAL_SECONDS));
        }
    }

public:
    void addBackup(const string& host, int port) {
        lock_guard<mutex> lock(backupMtx);
        backups.push_back({host, port});
        cout << "[Replication] Registered backup "
             << host << ":" << port << "\n";
    }

    //
    // On startup: for each registered backup, run the same
    // catchUpBackup() routine the Health Monitor uses at runtime.
    //
    void checkAndCatchUp(PersistenceManager& persistence,
                         atomic<long long>& currentSeq) {
        size_t count;
        {
            lock_guard<mutex> lock(backupMtx);
            count = backups.size();
        }
        for (size_t i = 0; i < count; ++i) {
            catchUpBackup(i, persistence, currentSeq);
        }
    }

    // Forward a write to all backups, update their lastSeq
    vector<ReplicationResult> replicateWrite(long long seq, const string& cmd) {
        lock_guard<mutex> lock(backupMtx);
        vector<ReplicationResult> results;
        string fullCmd = "SEQWRITE " + to_string(seq) + " " + cmd;

        for (auto& b : backups) {
            // Only HEALTHY replicas receive live writes; a replica that
            // needs sync, is recovering, still initializing, or is
            // unreachable is skipped until the Health Monitor recovers it.
            if (b.state != ReplicaState::HEALTHY) {
                continue;
            }

            string resp = sendRaw(b.host, b.port, fullCmd);
            ReplicationResult result = classifyResponse(resp);
            results.push_back(result);

            if (result == ReplicationResult::SUCCESS) {
                b.state   = ReplicaState::HEALTHY;
                b.lastSeq = seq;
                cout << "[Replication] Backup :" << b.port
                     << " ACK seq=" << seq << "\n";
            } else if (result == ReplicationResult::GAP) {
                cerr << "[Replication] Backup :" << b.port
                     << " did not ACK seq=" << seq << "\n";
                b.state = ReplicaState::NEEDS_SYNC;
            } else {
                cerr << "[Replication] Backup :" << b.port
                     << " did not ACK seq=" << seq << "\n";
                b.state = ReplicaState::UNREACHABLE;
            }
        }
        return results;
    }

    // Build STATUS string
    string statusString(long long primarySeq) {
        lock_guard<mutex> lock(backupMtx);
        string s = "PRIMARY seq=" + to_string(primarySeq) + "\n";
        for (auto& b : backups) {
            long long behind = primarySeq - b.lastSeq;
            s += "BACKUP :" + to_string(b.port) +
                 " seq=" + to_string(b.lastSeq) +
                 " state=" + replicaStateToString(b.state) +
                 " behind=" + to_string(behind) + "\n";
        }
        return s;
    }

    int count() const { return (int)backups.size(); }

    // Starts the background Health Monitor. Must be called once,
    // after the primary's sequence counter and WAL are available.
    void startHealthMonitor(PersistenceManager& persistence,
                             atomic<long long>& primarySeq) {
        monitorPersistence = &persistence;
        monitorPrimarySeq  = &primarySeq;
        monitorRunning.store(true);
        monitorThread = thread([this] { healthMonitorLoop(); });
    }

    ~ReplicationManager() {
        monitorRunning.store(false);
        if (monitorThread.joinable()) monitorThread.join();
    }
};

// ============================================================
//  PrimaryServer
// ============================================================
class PrimaryServer {
private:
    int                    port;
    socket_t               serverSocket;
    LRUStorageEngine&      engine;
    PersistenceManager&    persistence;
    ReplicationManager&    replication;
    atomic<long long>      seq;   // monotonically increasing write counter

    string handleCommand(const string& raw) {
        istringstream iss(raw);
        vector<string> tokens;
        string word;
        while (tokens.size() < 2 && (iss >> word)) tokens.push_back(word);
        string rest;
        getline(iss, rest);
        if (!rest.empty() && rest[0]==' ') rest.erase(0,1);
        if (!rest.empty()) tokens.push_back(rest);

        if (tokens.empty()) return "ERROR: empty command\n";

        string cmd = tokens[0];
        transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

        // ── Read-only commands ────────────────────────────
        if (cmd == "PING") return "PONG\n";

        if (cmd == "GET") {
            if (tokens.size() < 2) return "ERROR: GET needs key\n";
            return engine.get(tokens[1]) + "\n";
        }

        // NEW: STATUS — show replication health
        if (cmd == "STATUS") {
            return replication.statusString(seq.load()) + "END\n";
        }

        // NEW: KEYS — list all keys
        if (cmd == "KEYS") {
            auto ks = engine.keys();
            if (ks.empty()) return "EMPTY\n";
            string out;
            for (auto& k : ks) out += k + "\n";
            out += "END\n";
            return out;
        }

        // ── Write commands (replicated + persisted) ───────
        if (cmd == "SET") {
            if (tokens.size() < 3) return "ERROR: SET needs key and value\n";
            long long s = ++seq;
            string writeCmd = "SET " + tokens[1] + " " + tokens[2];

            // 1. Replicate first (write-ahead replication)
            replication.replicateWrite(s, writeCmd);

            // 2. Apply locally
            engine.put(tokens[1], tokens[2]);

            // 3. Persist with sequence number
            persistence.append(s, "SET", tokens[1], tokens[2]);

            return "OK\n";
        }

        if (cmd == "DEL") {
            if (tokens.size() < 2) return "ERROR: DEL needs key\n";
            long long s = ++seq;
            string writeCmd = "DEL " + tokens[1];

            replication.replicateWrite(s, writeCmd);

            string result = engine.del(tokens[1]);
            if (result == "OK") persistence.append(s, "DEL", tokens[1]);
            return result + "\n";
        }

        return "ERROR: unknown command '" + cmd + "'\n";
    }

    void handleClient(socket_t clientSock, int id) {
        cout << "[Thread-" << id << "] Client connected.\n";
        char buffer[4096];
        string leftover;

        while (true) {
            memset(buffer, 0, sizeof(buffer));
            int n = recv(clientSock, buffer, sizeof(buffer)-1, 0);
            if (n <= 0) {
                cout << "[Thread-" << id << "] Client disconnected.\n";
                break;
            }
            leftover += string(buffer, n);

            size_t pos;
            while ((pos = leftover.find('\n')) != string::npos) {
                string line = leftover.substr(0, pos);
                leftover.erase(0, pos+1);
                if (!line.empty() && line.back()=='\r') line.pop_back();
                if (line.empty()) continue;

                cout << "[Thread-" << id << "] CMD: " << line << "\n";
                string resp = handleCommand(line);
                send(clientSock, resp.c_str(), (int)resp.size(), 0);
            }
        }
        CLOSE_SOCKET(clientSock);
    }

public:
    PrimaryServer(int p, LRUStorageEngine& eng,
                  PersistenceManager& pers, ReplicationManager& repl,
                  long long startSeq)
        : port(p), engine(eng), persistence(pers),
          replication(repl), serverSocket(SOCK_INVALID),
          seq(startSeq) {}

    // Exposes the live write counter so ReplicationManager's Health
    // Monitor can compare a recovering backup's lastSeq against it.
    atomic<long long>& sequenceRef() { return seq; }

    void start() {
#ifdef _WIN32
        WSADATA w; WSAStartup(MAKEWORD(2,2), &w);
#endif
        serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        int opt = 1;
        setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR,
                   (char*)&opt, sizeof(opt));

        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(port);

        if (bind(serverSocket, (sockaddr*)&addr, sizeof(addr)) == SOCK_ERR) {
            cerr << "[PrimaryServer] bind() failed on port " << port
                 << " — is another process already using it?\n";
            CLOSE_SOCKET(serverSocket);
#ifdef _WIN32
            WSACleanup();
#endif
            return;
        }

        if (listen(serverSocket, 10) == SOCK_ERR) {
            cerr << "[PrimaryServer] listen() failed on port " << port << "\n";
            CLOSE_SOCKET(serverSocket);
#ifdef _WIN32
            WSACleanup();
#endif
            return;
        }

        cout << "\n";
        cout << "╔══════════════════════════════════════════╗\n";
        cout << "║   PRIMARY NODE  listening on port " << port << "  ║\n";
        cout << "║   Starting at sequence: " << seq.load()
             << "               ║\n";
        cout << "╚══════════════════════════════════════════╝\n\n";
        cout << "Commands: SET GET DEL PING STATUS KEYS\n\n";

        int id = 0;
        while (true) {
            sockaddr_in ca{}; socklen_t cl = sizeof(ca);
            socket_t cs = accept(serverSocket, (sockaddr*)&ca, &cl);
            if (cs == SOCK_INVALID) break;
            ++id;
            cout << "[Main] Accepted client #" << id << "\n";
            thread([this, cs, id]{ handleClient(cs, id); }).detach();
        }
        CLOSE_SOCKET(serverSocket);
#ifdef _WIN32
        WSACleanup();
#endif
    }
};

// ============================================================
//  main
// ============================================================
int main(int argc, char* argv[]) {
    if (argc < 4) {
        cerr << "Usage: " << argv[0]
             << " <primaryPort> <backup1Port> <backup2Port>\n";
        return 1;
    }

    int primaryPort = stoi(argv[1]);
    int backupPort1 = stoi(argv[2]);
    int backupPort2 = stoi(argv[3]);
    string logFile  = "kv_primary_" + to_string(primaryPort) + ".log";

    LRUStorageEngine   engine(50);
    PersistenceManager persistence(logFile);
    ReplicationManager replication;

    replication.addBackup("127.0.0.1", backupPort1);
    replication.addBackup("127.0.0.1", backupPort2);

    // Replay WAL and get last sequence number
    long long startSeq = persistence.replay(engine);

    // Constructed early (before any I/O) so checkAndCatchUp() and the
    // Health Monitor can both share the one live sequence counter.
    PrimaryServer server(primaryPort, engine, persistence,
                         replication, startSeq);

    // Check backups and send any missing entries (catch-up)
    cout << "\n[Startup] Checking backups and catching up...\n";
    replication.checkAndCatchUp(persistence, server.sequenceRef());
    cout << "\n";

    // Start the background Health Monitor now that both the WAL
    // and the live sequence counter exist.
    replication.startHealthMonitor(persistence, server.sequenceRef());

    server.start();

    return 0;
}
