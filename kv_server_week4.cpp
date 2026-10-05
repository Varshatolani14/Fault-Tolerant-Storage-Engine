/*
 * ============================================================
 *  Distributed KV Store  —  Weeks 1 through 4
 *  Built on your existing LRUStorageEngine
 * ============================================================
 *
 *  WHAT EACH WEEK ADDED
 *  --------------------
 *  Week 1  →  LRUStorageEngine class (OOP + data structures)
 *  Week 2  →  mutex + lock_guard inside put() and get()
 *  Week 3  →  TCP socket, bind, listen, accept (now cross-platform)
 *  Week 4  →  Thread-per-connection so N clients work simultaneously
 *             + leftover buffer fixing the TCP stream problem
 *             + DEL command
 *             + SO_REUSEADDR so restart works immediately
 *
 *  COMPILE
 *  -------
 *  Linux / WSL  (recommended for placement demos):
 *      g++ -std=c++17 -pthread -o kv_server kv_server_week4.cpp
 *
 *  Windows native (MinGW or MSVC):
 *      g++ -std=c++17 -o kv_server kv_server_week4.cpp -lws2_32
 *
 *  TEST  (open two terminals after starting the server)
 *  ----
 *      nc 127.0.0.1 8080          <- terminal 2
 *      nc 127.0.0.1 8080          <- terminal 3 (both work at once!)
 *
 *  COMMANDS
 *  --------
 *      SET  <key> <value>    →  OK
 *      GET  <key>            →  <value>  or  NOT_FOUND
 *      DEL  <key>            →  OK       or  NOT_FOUND
 *      PING                  →  PONG
 *
 * ============================================================
 */

#include <iostream>
#include <unordered_map>
#include <list>
#include <string>
#include <thread>
#include <mutex>
#include <sstream>
#include <vector>
#include <algorithm>
#include <cstring>

// ─── Cross-platform socket headers ───────────────────────────
//
//  WHY the #ifdef?
//  Linux and Windows both speak TCP/IP but expose it through
//  completely different header files and APIs. The preprocessor
//  directive lets a single source file compile correctly on both
//  without you maintaining two copies. Every socket operation
//  below uses the SOCK_INVALID / CLOSE_SOCKET aliases defined
//  here so the rest of the code is OS-blind.
//
#ifdef _WIN32
    #include <winsock2.h>
    #include <ws2tcpip.h>
    #pragma comment(lib, "Ws2_32.lib")
    using socket_t = SOCKET;
    #define CLOSE_SOCKET(s)  closesocket(s)
    #define SOCK_INVALID     INVALID_SOCKET
    #define SOCK_ERR         SOCKET_ERROR
#else
    #include <sys/socket.h>
    #include <netinet/in.h>
    #include <arpa/inet.h>
    #include <unistd.h>
    using socket_t = int;
    #define CLOSE_SOCKET(s)  close(s)
    #define SOCK_INVALID     (-1)
    #define SOCK_ERR         (-1)
#endif

using namespace std;


// ============================================================
//  WEEK 1 + 2  —  LRUStorageEngine
//  (your original code, kept exactly — only added DEL)
// ============================================================
//
//  DATA STRUCTURE RECAP
//  --------------------
//  list<string>  usageList
//      Doubly-linked list of keys ordered by recency.
//      Front = most recently used.  Back = evict next.
//      Crucially, erasing any node is O(1) IF you hold an iterator.
//
//  unordered_map<string, pair<string, list::iterator>>  cacheMap
//      Maps key → (value, iterator-into-usageList).
//      Storing the iterator is the entire trick: it lets us
//      jump directly to the node in O(1) instead of searching.
//
//  mutex  mtx
//      One lock protects BOTH structures together.
//      They must always be consistent with each other, so you
//      cannot use two separate locks — that would create a window
//      where one is updated and the other is not.
//
//  WHY lock_guard over manual lock/unlock?
//      lock_guard is RAII: it releases the mutex automatically
//      when the function returns OR when an exception is thrown.
//      Manual unlock() would leak the lock on any exception,
//      freezing every other thread that tries to acquire it.
//
class LRUStorageEngine {
private:
    int capacity;
    list<string> usageList;
    unordered_map<string, pair<string, list<string>::iterator>> cacheMap;
    mutex mtx;

public:
    explicit LRUStorageEngine(int cap) : capacity(cap) {}

    // ── put ──────────────────────────────────────────────────
    //  O(1) average.  Updates existing key or inserts new one.
    //  Evicts LRU entry if at capacity before inserting.
    void put(const string& key, const string& value) {
        lock_guard<mutex> lock(mtx);

        auto it = cacheMap.find(key);
        if (it != cacheMap.end()) {
            // Key exists: update value and promote to front of list
            usageList.erase(it->second.second);
            usageList.push_front(key);
            it->second = {value, usageList.begin()};
            return;
        }

        // Cache full: remove least-recently-used (back of list)
        if ((int)cacheMap.size() >= capacity) {
            const string& lru = usageList.back();
            cout << "[Eviction] Removing key: \"" << lru << "\"\n";
            cacheMap.erase(lru);
            usageList.pop_back();
        }

        usageList.push_front(key);
        cacheMap[key] = {value, usageList.begin()};
    }

    // ── get ──────────────────────────────────────────────────
    //  O(1) average.  Promotes key to front on every access
    //  so frequently read keys are never evicted.
    string get(const string& key) {
        lock_guard<mutex> lock(mtx);

        auto it = cacheMap.find(key);
        if (it == cacheMap.end()) return "NOT_FOUND";

        // Promote: move to front of list
        usageList.erase(it->second.second);
        usageList.push_front(key);
        it->second.second = usageList.begin();

        return it->second.first;
    }

    // ── del ──────────────────────────────────────────────────
    //  NEW in Week 4.  A KV store without delete is incomplete.
    //  Returns "OK" if found and removed, "NOT_FOUND" otherwise.
    string del(const string& key) {
        lock_guard<mutex> lock(mtx);

        auto it = cacheMap.find(key);
        if (it == cacheMap.end()) return "NOT_FOUND";

        usageList.erase(it->second.second);
        cacheMap.erase(it);
        return "OK";
    }
};


// ============================================================
//  WEEK 3 + 4  —  KVServer
// ============================================================
class KVServer {
private:
    int               port;
    socket_t          serverSocket;
    LRUStorageEngine& engine;

    // ─────────────────────────────────────────────────────────
    //  Command parser
    //
    //  WHY istringstream instead of rfind + substr?
    //  Your Week-3 parser used rfind("SET ", 0) and two calls
    //  to find(' ') to locate the key and value. This breaks
    //  in two ways:
    //    1.  "SET city new delhi"  — value gets truncated to "new"
    //    2.  "set name alex"       — lowercase not recognised
    //
    //  istringstream lets us:
    //    - Read the command and key as individual tokens (>> operator)
    //    - Read the entire remaining line as the value via getline()
    //      so "SET city new delhi" correctly captures "new delhi"
    //    - Case-normalise the command with transform() so "set"=="SET"
    //
    string handleCommand(const string& raw) {
        istringstream iss(raw);
        vector<string> tokens;
        string word;

        // Read first two words (command + key)
        //
        // BUG FIX (root cause): check size() BEFORE extracting, not after.
        // "iss >> word && tokens.size() < 2" let operator>> silently
        // consume and discard the 3rd word (the value) before the size
        // check could stop the loop, leaving nothing for getline() below.
        while (tokens.size() < 2 && (iss >> word))
            tokens.push_back(word);

        // Everything remaining on the line is the value
        // (supports spaces in values like "SET city new delhi")
        string rest;
        getline(iss, rest);
        // getline leaves a leading space after the last >>; strip it
        if (!rest.empty() && rest[0] == ' ') rest.erase(0, 1);
        if (!rest.empty()) tokens.push_back(rest);

        if (tokens.empty()) return "ERROR: empty command\n";

        // Normalise to uppercase so "ping", "Ping", "PING" all work
        string cmd = tokens[0];
        transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

        if (cmd == "PING") {
            return "PONG\n";
        }
        if (cmd == "SET") {
            if (tokens.size() < 3)
                return "ERROR: usage: SET <key> <value>\n";
            engine.put(tokens[1], tokens[2]);
            return "OK\n";
        }
        if (cmd == "GET") {
            if (tokens.size() < 2)
                return "ERROR: usage: GET <key>\n";
            return engine.get(tokens[1]) + "\n";
        }
        if (cmd == "DEL") {
            if (tokens.size() < 2)
                return "ERROR: usage: DEL <key>\n";
            return engine.del(tokens[1]) + "\n";
        }

        return "ERROR: unknown command \"" + cmd + "\"\n";
    }

    // ─────────────────────────────────────────────────────────
    //  Per-client handler  —  runs in its own std::thread
    //
    //  WHY a leftover buffer?
    //  This is the most important fix over your Week-3 code.
    //
    //  TCP is a byte STREAM, not a message protocol.
    //  One send() on the client does NOT guarantee one recv()
    //  on the server. A command can arrive split across multiple
    //  recv() calls:
    //
    //      send: "SET name alex\n"
    //      recv #1: "SET na"          ← not a complete command yet
    //      recv #2: "me alex\n"       ← now it's complete
    //
    //  Without accumulation, you'd parse "SET na" as garbage.
    //
    //  The fix: append every recv() chunk to a string (leftover).
    //  Only when we find a '\n' in leftover do we extract and
    //  parse a complete command. Everything before '\n' gets
    //  processed; everything after stays in leftover for the next
    //  recv() call.
    //
    void handleClient(socket_t clientSock, int clientId) {
        cout << "[Thread-" << clientId << "] Client connected.\n";

        char   buffer[4096];
        string leftover;          // accumulates partial commands

        while (true) {
            memset(buffer, 0, sizeof(buffer));
            int bytesRead = recv(clientSock, buffer, sizeof(buffer) - 1, 0);

            if (bytesRead <= 0) {
                // 0 = client closed connection cleanly
                // <0 = network error
                cout << "[Thread-" << clientId << "] Client disconnected.\n";
                break;
            }

            // Append raw bytes to our accumulator
            leftover += string(buffer, bytesRead);

            // Extract and handle every complete line
            size_t pos;
            while ((pos = leftover.find('\n')) != string::npos) {
                string line = leftover.substr(0, pos);
                leftover.erase(0, pos + 1);

                // Strip Windows carriage-return if client uses \r\n
                if (!line.empty() && line.back() == '\r')
                    line.pop_back();

                if (line.empty()) continue;   // blank line, skip

                cout << "[Thread-" << clientId << "] CMD: " << line << "\n";
                string response = handleCommand(line);
                send(clientSock, response.c_str(), (int)response.size(), 0);
            }
        }

        CLOSE_SOCKET(clientSock);
    }

public:
    KVServer(int p, LRUStorageEngine& eng)
        : port(p), engine(eng), serverSocket(SOCK_INVALID) {}

    void start() {
        // ── 1. Windows-only init ──────────────────────────────
#ifdef _WIN32
        WSADATA wsaData;
        if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
            cerr << "Winsock init failed.\n"; return;
        }
#endif

        // ── 2. Create TCP socket ──────────────────────────────
        //
        //  AF_INET     = IPv4
        //  SOCK_STREAM = TCP (reliable ordered stream)
        //  0           = OS picks the right protocol (TCP for SOCK_STREAM)
        //
        serverSocket = socket(AF_INET, SOCK_STREAM, 0);
        if (serverSocket == SOCK_INVALID) {
            cerr << "socket() failed.\n"; return;
        }

        // ── 3. SO_REUSEADDR ──────────────────────────────────
        //
        //  WHY: When you kill the server with Ctrl-C, the OS
        //  keeps the port in TIME_WAIT state for ~60 seconds.
        //  Without this option, restarting immediately gives
        //  "Address already in use". SO_REUSEADDR tells the OS
        //  to bypass TIME_WAIT and reuse the port right away.
        //
        int opt = 1;
        setsockopt(serverSocket, SOL_SOCKET, SO_REUSEADDR,
                   (const char*)&opt, sizeof(opt));

        // ── 4. Bind ───────────────────────────────────────────
        //
        //  INADDR_ANY  = accept connections on all network interfaces
        //                (localhost AND any LAN IP the machine has)
        //  htons(port) = converts the port number from host byte order
        //                to network byte order (big-endian).
        //                Always required — skipping it causes the server
        //                to listen on a completely wrong port number.
        //
        sockaddr_in addr{};
        addr.sin_family      = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port        = htons(port);

        if (bind(serverSocket, (sockaddr*)&addr, sizeof(addr)) == SOCK_ERR) {
            cerr << "bind() failed — is port " << port << " in use?\n";
            CLOSE_SOCKET(serverSocket); return;
        }

        // ── 5. Listen ─────────────────────────────────────────
        //
        //  Backlog = 10: the OS queues up to 10 completed TCP
        //  handshakes that haven't been accept()-ed yet.
        //  If the queue fills, new SYN packets are dropped.
        //  10 is plenty for a dev server.
        //
        if (listen(serverSocket, 10) == SOCK_ERR) {
            cerr << "listen() failed.\n";
            CLOSE_SOCKET(serverSocket); return;
        }

        cout << "\n╔══════════════════════════════════════╗\n";
        cout <<   "║  KV Server  listening on port " << port << "  ║\n";
        cout <<   "╚══════════════════════════════════════╝\n";
        cout << "Commands:  SET <k> <v>   GET <k>   DEL <k>   PING\n\n";

        int clientId = 0;

        // ── 6. Accept loop ────────────────────────────────────
        //
        //  WEEK 4 CORE CHANGE: instead of handling one client
        //  and stopping, we loop forever. Each new connection
        //  gets its own std::thread running handleClient().
        //
        //  WHY detach() and not join()?
        //  join() blocks the main thread until the worker finishes.
        //  We can't block here — we need to go back to accept()
        //  immediately so the next client doesn't wait.
        //  detach() lets the worker thread run independently.
        //
        //  Trade-off: with detach() we lose direct control of the
        //  thread's lifetime. For Week 5 production code we'd store
        //  thread handles and join them on shutdown. For now,
        //  detach() is the correct Week-4 solution.
        //
        while (true) {
            sockaddr_in clientAddr{};
            socklen_t   addrLen = sizeof(clientAddr);

            socket_t clientSock = accept(
                serverSocket, (sockaddr*)&clientAddr, &addrLen);

            if (clientSock == SOCK_INVALID) {
                cerr << "accept() error — server shutting down.\n";
                break;
            }

            ++clientId;
            cout << "[Main] New connection → Thread-" << clientId << "\n";

            // Capture clientSock and clientId BY VALUE so each
            // thread has its own independent copies.
            // Capturing by reference would be a data race: the
            // loop variables change before the thread reads them.
            thread([this, clientSock, clientId]() {
                handleClient(clientSock, clientId);
            }).detach();
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
int main() {
    // Capacity 10: easy to trigger eviction during testing.
    // Change to 1000 for a less aggressive demo.
    LRUStorageEngine engine(10);

    KVServer server(8080, engine);
    server.start();

    return 0;
}
