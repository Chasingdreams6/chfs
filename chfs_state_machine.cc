#include "chfs_state_machine.h"

#include <utility>

const int DEBUG_SM = 1;

chfs_command_raft::chfs_command_raft() {
    // Lab3: Your code here
    cmd_tp = CMD_NONE;
    res.reset(new struct result);
    res->done = false;
    //res->cnt = 0;
}

chfs_command_raft::chfs_command_raft(const chfs_command_raft &cmd) :
    cmd_tp(cmd.cmd_tp), type(cmd.type),  id(cmd.id), buf(cmd.buf), res(cmd.res) {
    // Lab3: Your code here
}
chfs_command_raft::~chfs_command_raft() {
    // Lab3: Your code here
    res.reset(); // free
}

int chfs_command_raft::size() const{ 
    // Lab3: Your code here
    int res = 4; // type
    switch (cmd_tp) {
        case CMD_CRT: // id;
        case CMD_GET:
        case CMD_GETA:
        case CMD_RMV:
            res += 8;
            break;
        case CMD_PUT:
            res += 12 + buf.size();
            break;
        default: break;
    }
    return res;
}

// transform data to buf_out
void chfs_command_raft::serialize(char *buf_out, int size) const {
    // Lab3: Your code here
    char *pos = buf_out;
    package_int(pos, cmd_tp); pos += 4;
    switch (cmd_tp) {
        case CMD_CRT: // type
            package_int(pos, type); pos += 4;
            break;
        case CMD_GET: // id
            package_ull(pos, id); pos += 8;
            break;
        case CMD_PUT: // id, len, buf
            package_ull(pos, id); pos += 8;
            package_int(pos, buf.size()); pos += 4;
            package_string(pos, buf); pos += buf.size();
            break;
        case CMD_GETA: // id
            package_ull(pos, id); pos += 8;
            break;
        case CMD_RMV: // id
            package_ull(pos, id); pos += 8;
            break;
        default: break;
    }
}

void chfs_command_raft::deserialize(const char *buf_in, int size) {
    // Lab3: Your code here
    const char *pos = buf_in;
    int tmp;
    extent_protocol::extentid_t tmp2;
    unpackage_int(pos, tmp); pos += 4;
    cmd_tp = static_cast<command_type>(tmp + CMD_NONE);
    switch (cmd_tp) {
        case CMD_CRT: // type
            unpackage_int(pos, tmp);
            type = tmp;
            break;
        case CMD_GET: // id
            unpackage_ull(pos, id);
            break;
        case CMD_GETA: // id
            unpackage_ull(pos, id);
            break;
        case CMD_PUT: // id, size,
            unpackage_ull(pos, id); pos += 8;
            unpackage_int(pos, tmp); pos += 4; // size
            unpackage_string(pos, tmp, buf);
            break;
        case CMD_RMV: // id
            unpackage_ull(pos, id);
            break;
        default: break;
    }
}

chfs_command_raft::chfs_command_raft(chfs_command_raft::command_type cmdtp1, extent_protocol::extentid_t id1,
                                     uint32_t type1, std::string buf1) {
    cmd_tp = cmdtp1;
    id = id1;
    type = type1;
    buf = std::move(buf1);
    res.reset(new struct result);
    //res->cnt = 0;
    res->done = false;
}

void chfs_command_raft::package_int(char *out, int x) const {
    out[0] = (x >> 24) & 0xff;
    out[1] = (x >> 16) & 0xff;
    out[2] = (x >> 8) & 0xff;
    out[3] = x & 0xff;
}

void chfs_command_raft::unpackage_int(const char *in, int &x) {
    x = (in[0] & 0xff) << 24;
    x |= (in[1] & 0xff) << 16;
    x |= (in[2] & 0xff) << 8;
    x |= in[3] & 0xff;
}

void chfs_command_raft::package_string(char *out, std::string s) const{
    int len = s.size();
    for (int i = 0; i < len; ++i) out[i] = s[i];
}

void chfs_command_raft::unpackage_string(const char *out, int len, std::string &s) {
    s.clear();
    for (int i = 0; i < len; ++i) s.push_back(out[i]);
}

void chfs_command_raft::package_ull(char *out, extent_protocol::extentid_t x) const {
    out[0] = (x >> 56) & 0xff;
    out[1] = (x >> 48) & 0xff;
    out[2]= (x >> 40) & 0xff;
    out[3] = (x >> 32) & 0xff;
    out[4] = (x >> 24) & 0xff;
    out[5] = (x >> 16) & 0xff;
    out[6] = (x >> 8) & 0xff;
    out[7] = x & 0xff;
}

void chfs_command_raft::unpackage_ull(const char *in, extent_protocol::extentid_t &x) {
    x = (long long)(in[0] & 0xff) << 56;
    x |= (long long)(in[1] & 0xff) << 48;
    x |= (long long)(in[2] & 0xff) << 40;
    x |= (long long)(in[3] & 0xff) << 32;
    x |= (in[4] & 0xff) << 24;
    x |= (in[5] & 0xff) << 16;
    x |= (in[6] & 0xff) << 8;
    x |= in[7] & 0xff;
}

marshall &operator<<(marshall &m, const chfs_command_raft &cmd) {
    // Lab3: Your code here
    m << cmd.cmd_tp;
    switch (cmd.cmd_tp) {
        case chfs_command_raft::CMD_CRT:
            m << cmd.type;
            break;
        case chfs_command_raft::CMD_GET:
        case chfs_command_raft::CMD_GETA:
            m << cmd.id;
            break;
        case chfs_command_raft::CMD_PUT:
            m << cmd.id;
            m << cmd.buf;
            break;
        case chfs_command_raft::CMD_RMV:
            m << cmd.id;
            break;
        default:break;
    }
    return m;
}

unmarshall &operator>>(unmarshall &u, chfs_command_raft &cmd) {
    // Lab3: Your code here
    int tmp;
    u >> tmp;
    cmd.cmd_tp = static_cast<chfs_command_raft::command_type>(tmp + chfs_command_raft::CMD_NONE);
    switch (cmd.cmd_tp) {
        case chfs_command_raft::CMD_CRT:
            u >> cmd.type;
            break;
        case chfs_command_raft::CMD_GET:
        case chfs_command_raft::CMD_GETA:
            u >> cmd.id;
            break;
        case chfs_command_raft::CMD_PUT:
            u >> cmd.id;
            u >> cmd.buf;
            break;
        case chfs_command_raft::CMD_RMV:
            u >> cmd.id;
            break;
        default:break;
    }
    return u;
}

void chfs_state_machine::apply_log(raft_command &cmd) {
    chfs_command_raft &chfs_cmd = dynamic_cast<chfs_command_raft &>(cmd);
    // Lab3: Your code here
    std::unique_lock<std::mutex> lock(chfs_cmd.res->mtx);
    int _;
    switch (chfs_cmd.cmd_tp) {
        case chfs_command_raft::CMD_CRT:
            es.create(chfs_cmd.type, chfs_cmd.res->id);
            //chfs_cmd.res->cnt++;
            if (DEBUG_SM)
                printf("SM: after create in es, id=%llu\n", chfs_cmd.res->id);
            break;
        case chfs_command_raft::CMD_GET:
            es.get(chfs_cmd.id, chfs_cmd.res->buf);
            //chfs_cmd.res->cnt++;
            if (DEBUG_SM)
                printf("SM: after get in es, id=%llu, sz=%lu\n", chfs_cmd.id, chfs_cmd.res->buf.size());
            break;
        case chfs_command_raft::CMD_GETA:
            es.getattr(chfs_cmd.id, chfs_cmd.res->attr);
            //chfs_cmd.res->cnt++;
            if (DEBUG_SM)
                printf("SM: after getA in es, id=%llu, sz=%u\n", chfs_cmd.id, chfs_cmd.res->attr.size);
            break;
        case chfs_command_raft::CMD_PUT:
            es.put(chfs_cmd.id, chfs_cmd.buf, _);
            //chfs_cmd.res->cnt++;
            if (DEBUG_SM)
                printf("SM: after put in es, id=%llu\n", chfs_cmd.id);
            break;
        case chfs_command_raft::CMD_RMV:
            es.remove(chfs_cmd.id, _);
            //chfs_cmd.res->cnt++;
            if (DEBUG_SM)
                printf("SM: after remove in es, id=%llu\n", chfs_cmd.id);
            break;
        default:break;
    }
    chfs_cmd.res->done = true;
    chfs_cmd.res->cv.notify_all();
}


