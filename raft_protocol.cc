#include "raft_protocol.h"

marshall &operator<<(marshall &m, const request_vote_args &args) {
    m << args.term << args.candidateId << args.lastLogIndex << args.lastLogTerm;
    return m;
}
unmarshall &operator>>(unmarshall &u, request_vote_args &args) {
    u >> args.term >> args.candidateId >> args.lastLogIndex >> args.lastLogTerm;
    return u;
}

marshall &operator<<(marshall &m, const request_vote_reply &reply) {
    m << reply.currentTerm << reply.voteGranted;
    return m;
}

unmarshall &operator>>(unmarshall &u, request_vote_reply &reply) {
    u >> reply.currentTerm >> reply.voteGranted;
    return u;
}

marshall &operator<<(marshall &m, const append_entries_reply &args) {
    // Lab3: Your code here
    m << args.term << args.success;
    return m;
}

unmarshall &operator>>(unmarshall &m, append_entries_reply &args) {
    // Lab3: Your code here
    m >> args.term >> args.success;
    return m;
}

marshall &operator<<(marshall &m, const install_snapshot_args &args) {
    // Lab3: Your code here
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_args &args) {
    // Lab3: Your code here
    return u;
}

marshall &operator<<(marshall &m, const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return m;
}

unmarshall &operator>>(unmarshall &u, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return u;
}