#include "extent_server_dist.h"

#include <utility>
//#include "chfs_state_machine.h"

const int DEBUG_DIST = 1;

chfs_raft *extent_server_dist::leader() const {
    int leader = this->raft_group->check_exact_one_leader();
    if (leader < 0) {
        return this->raft_group->nodes[0];
    } else {
        return this->raft_group->nodes[leader];
    }
}

int extent_server_dist::create(uint32_t type, extent_protocol::extentid_t &id) {
    // Lab3: your code here
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_CRT, 0, type, "");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;

    leader()->new_command(cmd, term, index); // asyn
    while (1) {
        if (!cmd.res->done) {
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(200));
        } else { // done
            id = cmd.res->id;
            if (DEBUG_DIST)
                printf("After to append create cmd, inode = %d\n", id);
            break;
        }
    }
    return extent_protocol::OK;
}

int extent_server_dist::put(extent_protocol::extentid_t id, std::string buf, int &actual_size) {
    // Lab3: your code here
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_PUT, id, 0, buf);
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;
    if (DEBUG_DIST)
        printf("Prepare to append put cmd, id=%d buflen=%lu\n", id, buf.size());
    leader()->new_command(cmd, term, index);
    while (1) {
        if (!cmd.res->done) {
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(200));
        } else { // done
            actual_size = 0; // it's useless for extent_client
            break;
        }
    }
    return extent_protocol::OK;
}

int extent_server_dist::get(extent_protocol::extentid_t id, std::string &buf) {
    // Lab3: your code here
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_GET, id, 0, "");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;
    leader()->new_command(cmd, term, index);
    while (1) {
        if (!cmd.res->done) {
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(200));
        } else { // done
            buf = cmd.res->buf;
            if (DEBUG_DIST)
                printf("After to apply get cmd, id=%d, buflen=%d\n", id, buf.size());
            break;
        }
    }
    return extent_protocol::OK;
}

int extent_server_dist::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a) {
    // Lab3: your code here
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_GETA, id, 0, "");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;
    leader()->new_command(cmd, term, index);
    while (1) {
        if (!cmd.res->done) {
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(200));
        } else { // done
            a = cmd.res->attr;
            if (DEBUG_DIST)
                printf("After to apply getattr cmd, id=%d sz=%d\n", id, a.size);
            break;
        }
    }
    return extent_protocol::OK;
}

int extent_server_dist::remove(extent_protocol::extentid_t id, int &size) {
    // Lab3: your code here
    chfs_command_raft cmd(chfs_command_raft::command_type::CMD_RMV, id, 0, "");
    std::unique_lock<std::mutex> lock(cmd.res->mtx);
    int term, index;
    if (DEBUG_DIST)
        printf("Prepare to append remove cmd, inode=%d\n", id);
    leader()->new_command(cmd, term, index);
    while (1) {
        if (!cmd.res->done) {
            cmd.res->cv.wait_until(lock, std::chrono::system_clock::now() + std::chrono::milliseconds(200));
        } else { // done
            size = 0;
            break;
        }
    }
    return extent_protocol::OK;
}

extent_server_dist::~extent_server_dist() {
    delete this->raft_group;
}