#ifndef raft_h
#define raft_h

#include <atomic>
#include <mutex>
#include <chrono>
#include <thread>
#include <ctime>
#include <algorithm>
#include <thread>
#include <stdarg.h>
#include <set>

#include "rpc.h"
#include "raft_storage.h"
#include "raft_protocol.h"
#include "raft_state_machine.h"

const int DEBUG_MODE = 0;

template<typename state_machine, typename command>
class raft {
    static_assert(std::is_base_of<raft_state_machine, state_machine>(),
                  "state_machine must inherit from raft_state_machine");
    static_assert(std::is_base_of<raft_command, command>(), "command must inherit from raft_command");

    friend class thread_pool;

//#define RAFT_LOG(fmt, args...) \
//    do {                       \
//    } while (0);

#define RAFT_LOG(fmt, args...)                                                                                   \
     do {                                                                                                         \
         auto now =                                                                                               \
             std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                 std::chrono::system_clock::now().time_since_epoch())                                             \
                 .count();                                                                                        \
         printf("[%ld][%s:%d][node %d term %d] " fmt "\n", now, __FILE__, __LINE__, my_id, current_term, ##args); \
     } while (0);

public:
    raft(
            rpcs *rpc_server,
            std::vector<rpcc *> rpc_clients,
            int idx,
            raft_storage<command> *storage,
            state_machine *state);

    ~raft();

    // start the raft node.
    // Please make sure all of the rpc request handlers have been registered before this method.
    void start();

    // stop the raft node.
    // Please make sure all of the background threads are joined in this method.
    // Notice: you should check whether is server should be stopped by calling is_stopped().
    //         Once it returns true, you should break all of your long-running loops in the background threads.
    void stop();

    // send a new command to the raft nodes.
    // This method returns true if this raft node is the leader that successfully appends the log.
    // If this node is not the leader, returns false.
    bool new_command(command cmd, int &term, int &index);

    // returns whether this node is the leader, you should also set the current term;
    bool is_leader(int &term);

    // save a snapshot of the state machine and compact the log.
    bool save_snapshot();

private:
    std::mutex mtx; // A big lock to protect the whole data structure
    ThrPool *thread_pool;
    raft_storage<command> *storage; // To persist the raft log
    state_machine *state;           // The state machine that applies the raft log, e.g. a kv store

    rpcs *rpc_server;                // RPC server to recieve and handle the RPC requests
    std::vector<rpcc *> rpc_clients; // RPC clients of all raft nodes including this node
    int my_id;                       // The index of this node in rpc_clients, start from 0

    std::atomic_bool stopped;

    enum raft_role {
        follower,
        candidate,
        leader
    };
    raft_role role;
    int current_term;
    int leader_id;

    std::thread *background_election;
    std::thread *background_ping;
    std::thread *background_commit;
    std::thread *background_apply;

    // Your code here:

    /* ----Persistent state on all server----  */
    //term_t currentTerm;
    int *votedFor;
    std::vector<std::pair<command, int>> log;

    /* ---- Volatile state on all server----  */
    int commitIndex;
    int lastApplied;
    std::set<int> votes; // record who had voted me

    int electionTimeout;
    int retryTimeout;
    ms_t lastHeartBeat;
    int leaderTimeout;
    int heartBeatInterval;

    /* ---- Volatile state on leader----  */
    std::vector<int> nextIndex;
    std::vector<int> matchIndex;
private:
    // RPC handlers
    int request_vote(request_vote_args arg, request_vote_reply &reply);

    int append_entries(append_entries_args<command> arg, append_entries_reply &reply);

    int install_snapshot(install_snapshot_args arg, install_snapshot_reply &reply);

    // RPC helpers
    void send_request_vote(int target, request_vote_args arg);

    void handle_request_vote_reply(int target, const request_vote_args &arg, const request_vote_reply &reply);

    void send_append_entries(int target, append_entries_args<command> arg);

    void
    handle_append_entries_reply(int target, const append_entries_args<command> &arg, const append_entries_reply &reply);

    static void
    fuck_cxx(raft<state_machine, command> *, const append_entries_args<command> &arg,
             const append_entries_reply &reply) {
        return;
    }

    void send_install_snapshot(int target, install_snapshot_args arg);

    void
    handle_install_snapshot_reply(int target, const install_snapshot_args &arg, const install_snapshot_reply &reply);

private:
    bool is_stopped();

    int num_nodes() {
        return rpc_clients.size();
    }

    // background workers
    void run_background_ping();

    void run_background_election();

    void run_background_commit();

    void run_background_apply();

    // Your code here:
    bool should_start_vote();
};

template<typename state_machine, typename command>
raft<state_machine, command>::raft(rpcs *server, std::vector<rpcc *> clients, int idx, raft_storage<command> *storage,
                                   state_machine *state) :
        stopped(false),
        rpc_server(server),
        rpc_clients(clients),
        my_id(idx),
        storage(storage),
        state(state),
        background_election(nullptr),
        background_ping(nullptr),
        background_commit(nullptr),
        background_apply(nullptr),
        current_term(0),
        role(follower) {
    thread_pool = new ThrPool(32);

    // Register the rpcs.
    rpc_server->reg(raft_rpc_opcodes::op_request_vote, this, &raft::request_vote);
    rpc_server->reg(raft_rpc_opcodes::op_append_entries, this, &raft::append_entries);
    rpc_server->reg(raft_rpc_opcodes::op_install_snapshot, this, &raft::install_snapshot);

    // Your code here:
    // Do the initialization
    votedFor = nullptr;
    commitIndex = 0;
    lastApplied = 0;

    command _;
    log.push_back(std::pair<command, int>(_, -1)); // just to let index start from 1

    /*init some time*/
    lastHeartBeat = getCurrentTime(); // init the first heartbeat
    electionTimeout = getRandomNumber(150, 300);
    retryTimeout = electionTimeout + getRandomNumber(150, 300);
    leaderTimeout = 500;
    heartBeatInterval = 50;
}

template<typename state_machine, typename command>
raft<state_machine, command>::~raft() {
    if (background_ping) {
        delete background_ping;
    }
    if (background_election) {
        delete background_election;
    }
    if (background_commit) {
        delete background_commit;
    }
    if (background_apply) {
        delete background_apply;
    }
    if (votedFor) delete votedFor;
    delete thread_pool;
}

/******************************************************************

                        Public Interfaces

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::stop() {
    stopped.store(true);
    background_ping->join();
    background_election->join();
    background_commit->join();
    background_apply->join();
    thread_pool->destroy();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_stopped() {
    return stopped.load();
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::is_leader(int &term) {
    term = current_term;
    return role == leader;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::start() {
    // Lab3: Your code here

    RAFT_LOG("start");
    this->background_election = new std::thread(&raft::run_background_election, this);
    this->background_ping = new std::thread(&raft::run_background_ping, this);
    this->background_commit = new std::thread(&raft::run_background_commit, this);
    this->background_apply = new std::thread(&raft::run_background_apply, this);
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::new_command(command cmd, int &term, int &index) {
    // Lab3: Your code here
    // leader will add new command and return true, the other will return false
    mtx.lock();
    if (role != leader) {
        mtx.unlock();
        return false;
    }
    term = current_term;
    log.push_back(std::make_pair(cmd, current_term));
    index = log.size() - 1;
    mtx.unlock();
    return true;
}

template<typename state_machine, typename command>
bool raft<state_machine, command>::save_snapshot() {
    // Lab3: Your code here
    return true;
}

/******************************************************************

                         RPC Related

*******************************************************************/
template<typename state_machine, typename command>
int raft<state_machine, command>::request_vote(request_vote_args args, request_vote_reply &reply) {
    // Lab3: Your code here
    // This will be seen as callee's view, a caller send request with args, and i will reply it
    mtx.lock();
    if (DEBUG_MODE)
        RAFT_LOG("I am %d, term is %d, I got request vote from %d, term si %d\n", my_id, current_term, args.candidateId,
                 args.term);
    reply.currentTerm = current_term;
    if (args.term < current_term) { // should reply false and down the sender's role
        reply.voteGranted = false;
        mtx.unlock();
        return 0;
    }
    if (args.term > current_term) {
        if (DEBUG_MODE)
            RAFT_LOG("I am %d, term is %d, I got request vote from %d, down to follower\n", my_id, current_term, args.candidateId);
        role = follower; // arg's term > current_term, down this
    }
    current_term = args.term; // update my seen term
    if (votedFor != nullptr) { // had voted
        if (*votedFor != args.candidateId)
            reply.voteGranted = false;
        else
            reply.voteGranted = true;
        mtx.unlock();
        return 0;
    }
    int myLastLogTerm = log[log.size() - 1].second;
    int myLastLogIndex = log.size() - 1;
    if (args.lastLogTerm > myLastLogTerm ||
        (args.lastLogTerm == myLastLogTerm && args.lastLogIndex >= myLastLogIndex)) { // give vote
        votedFor = new int(args.candidateId);
        reply.voteGranted = true;
    }
    mtx.unlock();
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_request_vote_reply(int target, const request_vote_args &arg,
                                                             const request_vote_reply &reply) {
    // Lab3: Your code here
    // This will be seen as caller's view, callee gave a reply in reply
    mtx.lock();
    if (DEBUG_MODE)
        RAFT_LOG("I am candidate %d, I got reply of %d, success is %d\n", my_id, target, reply.voteGranted);
    if (reply.voteGranted) { // success to get a vote
        votes.insert(target);
    } else {
        if (reply.currentTerm > current_term) { // down
            if (DEBUG_MODE)
                RAFT_LOG("I am candidate %d, I got reply of %d, down to follower\n", my_id, target);
            role = follower;
        }
    }
    mtx.unlock();
    return;
}

template<typename state_machine, typename command>
int raft<state_machine, command>::append_entries(append_entries_args<command> arg, append_entries_reply &reply) {
    // Lab3: Your code here
    // It's callee's view
    mtx.lock();
    if (DEBUG_MODE)
        RAFT_LOG("I am %d, term %d, I got append entries from %d, term %d\n", my_id, current_term, arg.leaderId,
                 arg.term);
    if (arg.term < current_term) {
        reply.term = current_term;
        reply.success = false;
        goto release;
    }
    lastHeartBeat = getCurrentTime(); // update the heart-beat
    if (arg.entries.empty()) { // heartbeat from leader
        reply.success = true;
        reply.term = current_term;
        free(votedFor); votedFor = nullptr; // clean votedFor message for next election
        if (arg.leaderId != my_id) // got heartbeat from other, down to follower
            role = follower;
        goto release;
    }
    //TODO other logic for append_entries
    release:
    mtx.unlock();
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_append_entries_reply(int node, const append_entries_args<command> &arg,
                                                               const append_entries_reply &reply) {
    // Lab3: Your code here
    //TODO may need other logic
    mtx.lock();
    if (DEBUG_MODE)
        RAFT_LOG("I am leader %d, got reply from %d, success is %d", my_id, node, reply.success);
    if (reply.term > current_term) { // down
        role = follower;
        if (DEBUG_MODE)
            RAFT_LOG("I am leader %d, got reply from %d, down to follower", my_id, node);
    }
    mtx.unlock();
    return;
}

template<typename state_machine, typename command>
int raft<state_machine, command>::install_snapshot(install_snapshot_args args, install_snapshot_reply &reply) {
    // Lab3: Your code here
    return 0;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::handle_install_snapshot_reply(int node, const install_snapshot_args &arg,
                                                                 const install_snapshot_reply &reply) {
    // Lab3: Your code here
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_request_vote(int target, request_vote_args arg) {
    request_vote_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_request_vote, arg, reply) == 0) {
        handle_request_vote_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_append_entries(int target, append_entries_args<command> arg) {
    append_entries_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_append_entries, arg, reply) == 0) {
        handle_append_entries_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

template<typename state_machine, typename command>
void raft<state_machine, command>::send_install_snapshot(int target, install_snapshot_args arg) {
    install_snapshot_reply reply;
    if (rpc_clients[target]->call(raft_rpc_opcodes::op_install_snapshot, arg, reply) == 0) {
        handle_install_snapshot_reply(target, arg, reply);
    } else {
        // RPC fails
    }
}

/******************************************************************

                        Background Workers

*******************************************************************/

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_election() {
    // Periodly check the liveness of the leader.

    // Work for followers and candidates.
    // this thread work until stop;
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here
        mtx.lock();
        if (should_start_vote()) {
            role = candidate;
            current_term++;
            votes.clear();
            votes.insert(my_id);
            request_vote_args args{};
            args.term = current_term;
            args.candidateId = my_id;
            args.lastLogTerm = log[log.size() - 1].second;
            args.lastLogIndex = log.size() - 1;
            int nums = num_nodes();
            for (int target = 0; target < nums; ++target) {
                if (target != my_id)
                    thread_pool->addObjJob(this, &raft::send_request_vote, target, args);
            }
        }
        mtx.unlock();
        my_mssleep(10); // wait for reply
        mtx.lock();
        if (votes.size() * 2 > num_nodes()) { // become the leader
            if (DEBUG_MODE)
                RAFT_LOG("I am candidate %d, I become leader\n", my_id);
            role = leader;
        }
        mtx.unlock();
        my_mssleep(retryTimeout);
    }
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_commit() {
    // Periodly send logs to the follower.

    // Only work for the leader.

    /*
        while (true) {
            if (is_stopped()) return;
            // Lab3: Your code here
        }    
        */

    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_apply() {
    // Periodly apply committed logs the state machine

    // Work for all the nodes.

    /*
    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
    }    
    */
    return;
}

template<typename state_machine, typename command>
void raft<state_machine, command>::run_background_ping() {
    // Periodly send empty append_entries RPC to the followers.

    // Only work for the leader.


    while (true) {
        if (is_stopped()) return;
        // Lab3: Your code here:
        mtx.lock();
        if (role == leader) {
            //RAFT_LOG("I am leader %d", my_id)
            int nums = num_nodes();
            append_entries_args<command> args;
            args.term = current_term;
            args.leaderId = my_id;
            for (int target = 0; target < nums; ++target) { // include send ping to myself
                thread_pool->addObjJob(this, &raft::send_append_entries, target, args);
            }
        }
        mtx.unlock();
        my_mssleep(heartBeatInterval);
    }


    return;
}

/******************************************************************

                        Other functions

*******************************************************************/
template<typename state_machine, typename command>
bool raft<state_machine, command>::should_start_vote() {
    if (role == leader) return false;
    ms_t cur = getCurrentTime();
    auto dur = cur - lastHeartBeat;
    if (dur.count() > electionTimeout) return true;
    return false;
}

#endif // raft_hm