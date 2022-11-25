#ifndef raft_protocol_h
#define raft_protocol_h

#include "rpc.h"
#include "raft_state_machine.h"
#include <random>

enum raft_rpc_opcodes {
    op_request_vote = 0x1212,
    op_append_entries = 0x3434,
    op_install_snapshot = 0x5656
};

enum raft_rpc_status {
    OK,
    RETRY,
    RPCERR,
    NOENT,
    IOERR
};

class request_vote_args {
public:
    int term;
    int candidateId;
    int lastLogIndex;
    int lastLogTerm;
};

marshall &operator<<(marshall &m, const request_vote_args &args);

unmarshall &operator>>(unmarshall &u, request_vote_args &args);

class request_vote_reply {
public:
    int currentTerm; // term for the target machine
    bool voteGranted;
};

marshall &operator<<(marshall &m, const request_vote_reply &reply);

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply);

template<typename command>
class log_entry {
public:
    int term;
    command cmd;
    log_entry() = default;
    log_entry(command cmd_, int term_) : cmd(cmd_), term(term_) {}
};

template<typename command>
marshall &operator<<(marshall &m, const log_entry<command> &entry) {
    m << entry.cmd << entry.term;
    return m;
}

template<typename command>
unmarshall &operator>>(unmarshall &u, log_entry<command> &entry) {
    u >> entry.cmd >> entry.term;
    return u;
}

template<typename command>
class append_entries_args {
public:
    // Your code here
    int term; // leader's term
    int leaderId; // leader's Id
    int prevLogIndex; // leader's prev log index, used for other's to check consistency
    int prevLogTerm;
    std::vector<log_entry<command>> entries; // empty for heartbeat
    int leaderCommit; // leader's commitIndex
    append_entries_args() = default;
};

template<typename command>
marshall &operator<<(marshall &m, const append_entries_args<command> &args) {
    // Lab3: Your code here
    m << args.term << args.leaderId << args.prevLogIndex << args.prevLogTerm << args.leaderCommit;
    m << args.entries;
    return m;
}

template<typename command>
unmarshall &operator>>(unmarshall &u, append_entries_args<command> &args) {
    u >> args.term >> args.leaderId >> args.prevLogIndex >> args.prevLogTerm >> args.leaderCommit;
    u >> args.entries;
    return u;
}

class append_entries_reply {
public:
    int term; // currentTerm, for leader to update himself
    bool success; // true indicates follower consistency
};

marshall &operator<<(marshall &m, const append_entries_reply &reply);

unmarshall &operator>>(unmarshall &m, append_entries_reply &reply);

class install_snapshot_args {
public:
    // Lab3: Your code here
};

marshall &operator<<(marshall &m, const install_snapshot_args &args);

unmarshall &operator>>(unmarshall &m, install_snapshot_args &args);

class install_snapshot_reply {
public:
    // Lab3: Your code here
};

marshall &operator<<(marshall &m, const install_snapshot_reply &reply);

unmarshall &operator>>(unmarshall &m, install_snapshot_reply &reply);

typedef std::chrono::duration<double, std::chrono::milliseconds> ms_dur;
typedef std::chrono::time_point<std::chrono::steady_clock, std::chrono::milliseconds> ms_t;

inline ms_t getCurrentTime() {
    return std::chrono::time_point_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now());
}

inline int getRandomNumber(int l, int r) {
    unsigned int cur_t = std::chrono::duration_cast<std::chrono::milliseconds>
            (std::chrono::system_clock::now().time_since_epoch()).count();
    static std::default_random_engine engine(cur_t);
    std::uniform_int_distribution<unsigned> u(l, r);
    return u(engine);
}

inline void my_mssleep(int ms) { usleep(ms * 1000); }

#endif // raft_protocol_h