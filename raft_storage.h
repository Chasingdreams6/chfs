#ifndef raft_storage_h
#define raft_storage_h

#include "raft_protocol.h"
#include <fcntl.h>
#include <mutex>

const int DEBUG_PART3 = 0;

#define STORE_LOG(fmt, args...)                                                                                   \
     do {                                                                                                         \
         auto now =                                                                                               \
             std::chrono::duration_cast<std::chrono::milliseconds>(                                               \
                 std::chrono::system_clock::now().time_since_epoch())                                             \
                 .count();                                                                                        \
         printf("[%ld][%s:%d] " fmt "\n", now, __FILE__, __LINE__,  ##args); \
     } while (0);

template <typename command>
class raft_storage {
public:
    raft_storage(const std::string &file_dir);
    ~raft_storage();
    // Lab3: Your code here
    void persistMetaData(int, int, int);
    void persistLog(std::vector<log_entry<command>> const &);
    void appendLog(log_entry<command> const &, int);
    void restoreMetaData(int&, int&);
    void restoreLog(std::vector<log_entry<command>> &, int );
private:
    std::mutex mtx;
    // Lab3: Your code here
    std::string meta_name, log_name;
};

template <typename command>
raft_storage<command>::raft_storage(const std::string &dir) {
    // Lab3: Your code here
    meta_name = dir + "/meta.txt";
    log_name = dir + "/log.txt";
    //printf("%s %s\n", meta_name.c_str(), log_name.c_str());
}

template <typename command>
raft_storage<command>::~raft_storage() {
    // Lab3: Your code here
}

template<typename command>
void raft_storage<command>::persistMetaData(int term, int votedFor, int len) {
    mtx.lock();
    FILE *fd = fopen(meta_name.c_str(), "wb");
//    if (DEBUG_PART3)
//        STORE_LOG("Persist MetaData term = %d, votedFor=%d, len=%d", term, votedFor, len);
    assert(fwrite(&term, 4, 1, fd));
    assert(fwrite(&votedFor, 4, 1, fd));
    assert(fwrite(&len, 4, 1, fd));
    fclose(fd);
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::restoreMetaData(int &term, int &votedFor) {
    mtx.lock();
    FILE *fd = fopen(meta_name.c_str(), "rb");
    if (!fd) { // no file
        mtx.unlock();
        return ;
    }
    assert(fread(&term, 4, 1, fd));
    assert(fread(&votedFor, 4, 1, fd));
    if (DEBUG_PART3)
        STORE_LOG("Restore MetaData term=%d, votedFor=%d", term, votedFor);
    fclose(fd);
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::persistLog(std::vector<log_entry<command>> const &log) {
    mtx.lock();
    if (log.size() <= 1) {
        mtx.unlock();
        return ;
    }
    FILE *fd = fopen(log_name.c_str(), "wb");
    int len = log.size(), size = log[0].cmd.size();
    if (DEBUG_PART3)
        STORE_LOG("Persist All log, len = %d", len);
    for (int i = 1; i < len; ++i) {
        assert(fwrite(&log[i].term, 4, 1, fd));
        //char *ptr = (char*) malloc(size * sizeof(char));
        char ptr[1024];
        log[i].cmd.serialize(ptr, size);
        assert(fwrite(ptr, 1, size, fd) == size);
        //free(ptr);
    }
    fclose(fd);
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::restoreLog(std::vector<log_entry<command>> &log, int size) {
    mtx.lock();
    FILE *fd = fopen(log_name.c_str(), "rb");
    FILE *md = fopen(meta_name.c_str(), "rb");
    if ((!fd) || (!md)) { // no log
        mtx.unlock();
        return ;
    }
    int len;
    fseek(md, 8, SEEK_SET);
    assert(fread(&len, 4, 1, md));
    if (DEBUG_PART3)
        STORE_LOG("Restore All log, len = %d, size = %d", len, size);
    log.clear();
    command _;
    log.push_back(log_entry<command>(_, -1));
    for (int i = 1; i < len; ++i) {
        int term;
        command cmd;
        assert(fread(&term, 4, 1, fd));
        char ptr[1024];
        assert(fread(ptr, 1, size, fd) == size);
        assert(ptr);
        cmd.deserialize(ptr, size);
        assert(ptr);
        log.push_back(log_entry<command>(cmd, term));
        //free(ptr);
    }

    fclose(fd);
    fclose(md);
    mtx.unlock();
}

template<typename command>
void raft_storage<command>::appendLog(const log_entry<command> &logEntry, int size) {
    mtx.lock();
    FILE *fd = fopen(log_name.c_str(), "ab");
    assert(fwrite(&(logEntry.term), 4, 1, fd));
    //char *ptr = (char*) malloc(size * sizeof(char));
    char ptr[1024];
    assert(ptr);
    logEntry.cmd.serialize(ptr, size);
    assert(fwrite(ptr, 1, size, fd) == size);
    fclose(fd);
    if (DEBUG_PART3)
        STORE_LOG("Appended a log, term = %d", logEntry.term);
    mtx.unlock();
}

#endif // raft_storage_h