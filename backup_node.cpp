/*
 * ============================================================
 *  Distributed KV Store — Backup Node (Final Version)
 * ============================================================
 *
 *  NEW IN FINAL VERSION
 *  --------------------
 *  1. Tracks its own sequence number
 *     Every write applied has a sequence number.
 *     Stored in memory and persisted to WAL.
 *
 *  2. GETSEQ command
 *     Primary asks "GETSEQ" at startup — backup replies
 *     "SEQ <number>" so primary knows if catch-up is needed.
 *
 *  3. SEQWRITE command
 *     Primary sends "SEQWRITE <seq> SET city Chennai"
 *     instead of raw "SET city Chennai".
 *     Backup extracts seq, applies the write, updates lastSeq.
 *
 *  4. KEYS command
 *     Returns all keys currently stored in this backup.
 *
 *  COMPILE (Windows):
 *     g++ -std=c++17 -o backup_node.exe backup_node.cpp -lws2_32
 *
 *  RUN:
 *     backup_node.exe 9002
 *     backup_node.exe 9003
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

    vector<string> keys() {
        lock_guard<mutex> lock(mtx);
        return vector<string>(usageList.begin(), usageList.end());
    }
};

// ============================================================
//  PersistenceManager (backup)
// ============================================================
class PersistenceManager {
private:
    string logPath;
    mutex  fileMtx;

public:
    explicit PersistenceManager(const string& p) : logPath(p) {}

    long long replay(LRUStorageEngine& engine) {
        ifstream in(logPath);
        if (!in.is_open()) {
            cout << "[WAL] No log — starting fresh.\n";
            return 0;
        }
        string line;
        int count = 0;
        long long maxSeq = 0;

        while (getline(in, line)) {
            if (line.empty()) continue;
            istringstream iss(line);
            string first; iss >> first;
            bool hasSeq = !first.empty() &&
                          first.find_first_not_of("0123456789") == string::npos;
            long long seq = 0;
            string cmd, key, value;

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
             << " entries. Last seq: " << maxSeq << "\n";
        return maxSeq;
    }

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
//  BackupServer
// ============================================================
class BackupServer {
private:
    int                 port;
    socket_t            serverSocket;
    LRUStorageEngine&   engine;
    PersistenceManager& persistence;
    atomic<long long>   lastSeq;

    string applyCommand(const string& raw) {
        istringstream iss(raw);
        vector<string> tokens;
        string word;
        while (tokens.size() < 2 && (iss >> word)) tokens.push_back(word);
        string rest;
        getline(iss, rest);
        if (!rest.empty() && rest[0]==' ') rest.erase(0,1);
        if (!rest.empty()) tokens.push_back(rest);

        if (tokens.empty()) return "ERROR\n";

        string cmd = tokens[0];
        transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

        // ── Internal protocol commands (from primary) ─────

        // GETSEQ — primary asking for our current sequence
        if (cmd == "GETSEQ") {
            return "SEQ " + to_string(lastSeq.load()) + "\n";
        }

        // SEQWRITE <seq> <cmd> <key> [value]
        // Primary forwards writes with sequence number
        if (cmd == "SEQWRITE") {
            // tokens[1] = seq number, rest = actual command
            long long seq = 0;
            try { seq = stoll(tokens[1]); } catch(...) { return "ERROR: bad seq\n"; }

            // Re-parse the actual command from rest
            istringstream iss2(rest);
            string innerCmd, key, value;
            iss2 >> innerCmd >> key;
            getline(iss2, value);
            if (!value.empty() && value[0]==' ') value.erase(0,1);

            transform(innerCmd.begin(), innerCmd.end(),
                      innerCmd.begin(), ::toupper);

            long long current = lastSeq.load();

            // Out of order: primary is ahead of what we can apply next.
            if (seq > current + 1) {
                return "GAP " + to_string(current) + "\n";
            }

            // Duplicate/stale: already applied, idempotent no-op.
            if (seq <= current) {
                return "OK\n";
            }

            // seq == current + 1: apply in order.
            if (innerCmd == "SET") {
                engine.put(key, value);
                persistence.append(seq, "SET", key, value);
            } else if (innerCmd == "DEL") {
                engine.del(key);
                persistence.append(seq, "DEL", key);
            }

            lastSeq.store(seq);
            return "OK\n";
        }

        // ── Direct client commands (for testing) ──────────

        if (cmd == "PING") return "PONG\n";

        if (cmd == "GET") {
            if (tokens.size() < 2) return "ERROR: GET needs key\n";
            return engine.get(tokens[1]) + "\n";
        }

        if (cmd == "KEYS") {
            auto ks = engine.keys();
            if (ks.empty()) return "EMPTY\n";
            string out;
            for (auto& k : ks) out += k + "\n";
            out += "END\n";
            return out;
        }

        if (cmd == "STATUS") {
            return "BACKUP port=" + to_string(port) +
                   " seq=" + to_string(lastSeq.load()) + "\nEND\n";
        }

        return "ERROR: unrecognised command '" + cmd + "'\n";
    }

    void handleConnection(socket_t sock, int id) {
        cout << "[Backup:" << port << ":Thread-" << id << "] Connected.\n";
        char buffer[4096];
        string leftover;

        while (true) {
            memset(buffer, 0, sizeof(buffer));
            int n = recv(sock, buffer, sizeof(buffer)-1, 0);
            if (n <= 0) {
                cout << "[Backup:" << port << ":Thread-" << id
                     << "] Disconnected.\n";
                break;
            }
            leftover += string(buffer, n);

            size_t pos;
            while ((pos = leftover.find('\n')) != string::npos) {
                string line = leftover.substr(0, pos);
                leftover.erase(0, pos+1);
                if (!line.empty() && line.back()=='\r') line.pop_back();
                if (line.empty()) continue;

                cout << "[Backup:" << port << "] CMD: " << line << "\n";
                string resp = applyCommand(line);
                send(sock, resp.c_str(), (int)resp.size(), 0);
            }
        }
        CLOSE_SOCKET(sock);
    }

public:
    BackupServer(int p, LRUStorageEngine& eng,
                 PersistenceManager& pers, long long startSeq)
        : port(p), engine(eng), persistence(pers),
          serverSocket(SOCK_INVALID), lastSeq(startSeq) {}

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
        bind(serverSocket, (sockaddr*)&addr, sizeof(addr));
        listen(serverSocket, 5);

        cout << "\n";
        cout << "╔══════════════════════════════════════════╗\n";
        cout << "║   BACKUP NODE  listening on port " << port << "   ║\n";
        cout << "║   Starting at sequence: " << lastSeq.load()
             << "               ║\n";
        cout << "╚══════════════════════════════════════════╝\n\n";
        cout << "Commands: GET PING KEYS STATUS\n\n";

        int id = 0;
        while (true) {
            sockaddr_in ca{}; socklen_t cl = sizeof(ca);
            socket_t cs = accept(serverSocket, (sockaddr*)&ca, &cl);
            if (cs == SOCK_INVALID) break;
            ++id;
            thread([this, cs, id]{ handleConnection(cs, id); }).detach();
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
    int port = 9002;
    if (argc >= 2) port = stoi(argv[1]);

    string logFile = "kv_backup_" + to_string(port) + ".log";

    LRUStorageEngine   engine(50);
    PersistenceManager persistence(logFile);

    long long startSeq = persistence.replay(engine);

    BackupServer server(port, engine, persistence, startSeq);
    server.start();

    return 0;
}
