#ifndef persister_h
#define persister_h

#include <fcntl.h>
#include <mutex>
#include <iostream>
#include <fstream>
#include "rpc.h"

#define DEBUG_LAB2 0
#define MAX_LOG_SZ 131072
#define LOG_THRESHOLD 996
#define TRANS_DEBUG 0
/*
 * Your code here for Lab2A:
 * Implement class chfs_command, you may need to add command types such as
 * 'create', 'put' here to represent different commands a transaction requires. 
 * 
 * Here are some tips:
 * 1. each transaction in ChFS consists of several chfs_commands.
 * 2. each transaction in ChFS MUST contain a BEGIN command and a COMMIT command.
 * 3. each chfs_commands contains transaction ID, command type, and other information.
 * 4. you can treat a chfs_command as a log entry.
 */
enum cmd_type {
    CMD_BEGIN = 0,
    CMD_COMMIT,
    CMD_CREATE,
    CMD_PUT,
    CMD_REMOVE,
    CMD_GET,
    CMD_GETATTR,
};
class chfs_command {
public:
    typedef unsigned long long txid_t;
  
    cmd_type type = CMD_BEGIN;
    txid_t id = 0;
    uint32_t inum, length = 0, file_type;
    std::string data;

    // constructor
    chfs_command() {}

    chfs_command(enum cmd_type _type, txid_t _id) : type(_type), id(_id) {}

    chfs_command(enum cmd_type _type, txid_t _id, uint32_t _inum) : type(_type), id(_id), inum(_inum) {}

    chfs_command(enum cmd_type _type, txid_t _id, uint32_t _inum, uint32_t _file_type) : type(_type), id(_id), inum(_inum), file_type(_file_type){}

    chfs_command(enum cmd_type _type, txid_t _id, uint32_t _inum, uint32_t _length, std::string _data) :
        type(_type), id(_id), inum(_inum), length(_length), data(_data) {}

    uint32_t getsize() const { // return byte
        uint32_t s = sizeof(cmd_type) + sizeof(txid_t);
        if (type != CMD_BEGIN && type != CMD_COMMIT)
            s += sizeof(uint32_t); //inum
        if (type == CMD_CREATE) // file_type
            s += sizeof(uint32_t);
        if (type == CMD_PUT)
            s += 2 * sizeof(uint32_t) + length;
        return s;
    }
    void clear() {
        type = CMD_BEGIN; id = 0;
        inum = length = 0; 
        data = "";
    }
    void toString(FILE *stream) {
        fwrite(&type, 1, 1, stream);
        fwrite(&id, 8, 1, stream);
        if (type != CMD_BEGIN && type != CMD_COMMIT)
          fwrite(&inum, 4, 1, stream);
        if (type == CMD_CREATE)
          fwrite(&file_type, 4, 1, stream);
        if (type == CMD_PUT) {
          fwrite(&length, 4, 1, stream);
          if (length) {
            uint32_t tmp = fwrite(data.c_str(), 1, length, stream);
            if (tmp != length)
                printf("Fatal: write string not equal to it's length\n");
          }
        }
        fflush(stream);
    }
    bool fromString(FILE* stream) {
        char buf[512*205];
        int type_num;
        uint32_t tmp = fread(&type_num, 1, 1, stream); 
        if (tmp != 1) return false;
        tmp = fread(&id, 8, 1, stream);
        switch(type_num) {
           case 0: type = CMD_BEGIN; break;
           case 1: type = CMD_COMMIT; break;
           case 2: type = CMD_CREATE; break;
           case 3: type = CMD_PUT; break;
           case 4: type = CMD_REMOVE; break;
           case 5: type = CMD_GET; break;
           case 6: type = CMD_GETATTR; break;
        }
        if (type != CMD_BEGIN && type != CMD_COMMIT) {
          fread(&inum, 4, 1, stream);
        }
        if (type == CMD_CREATE)
          fread(&file_type, 4, 1, stream);
        if (type == CMD_PUT) {
          fread(&length, 4, 1, stream);
          if (length) {
            tmp = fread(buf, 1, length, stream);
            if (tmp != length)
                printf("Fatal: read string not equal to it's length\n");
            //buf[length] = '\0';
            data = std::string(buf, length);  // this is very important, std::string(buf) is wrong, because the \0 hole
          }
          else data = "";
        }
        return true;
    }
};

/*
 * Your code here for Lab2A:
 * Implement class persister. A persister directly interacts with log files.
 * Remember it should not contain any transaction logic, its only job is to 
 * persist and recover data.
 * 
 * P.S. When and how to do checkpoint is up to you. Just keep your logfile size
 *      under MAX_LOG_SZ and checkpoint file size under DISK_SIZE.
 */
template<typename command>
class persister {

public:
    persister(const std::string& file_dir);
    ~persister();

    // persist data into solid binary file
    // You may modify parameters in these functions
    void append_log(command* log);
    void checkpoint();

    // restore data from solid binary file
    // You may modify parameters in these functions
    void restore_logdata();
    void restore_checkpoint();
    // restored log data
    std::vector<command> log_entries;
    int cur_logsize;
private:
    std::mutex mtx;
    std::string file_dir;
    std::string file_path_checkpoint;
    std::string file_path_logfile;
    FILE* log_ptr;
    FILE* checkpoint_ptr;
    FILE* trans_ptr;
};

template<typename command>
persister<command>::persister(const std::string& dir){
    // DO NOT change the file names here
    cur_logsize = 0;
    file_dir = dir;
    file_path_checkpoint = file_dir + "/checkpoint.bin";
    file_path_logfile = file_dir + "/logdata.bin";
    if (TRANS_DEBUG)
        trans_ptr = fopen("trans.log", "ab+");
}

template<typename command>
persister<command>::~persister() {
    // Your code here for lab2A
    if (TRANS_DEBUG)
        fclose(trans_ptr);
}

template<typename command>
void persister<command>::append_log(command* log) {
    // Your code here for lab2A
    //log_ptr = fopen(file_path_logfile.data(), "ab+");
    log_ptr = fopen(file_path_checkpoint.data(), "ab+");
    if (log_ptr < 0) return ;
    cur_logsize += log->getsize();
    log->toString(log_ptr);
    //fflush(log_ptr);
    fclose(log_ptr);
    //if (cur_logsize > MAX_LOG_SZ) checkpoint(); // do the checkpoint
}

template<typename command>
void persister<command>::checkpoint() {
    // Your code here for lab2A
    if (TRANS_DEBUG) {
        fprintf(trans_ptr, "Trans: Start a transformation logsize=%d\n", cur_logsize);
        fflush(trans_ptr);
    }
    log_ptr = fopen(file_path_logfile.data(), "rb");
    fseek(log_ptr, 0, SEEK_SET);
    checkpoint_ptr = fopen(file_path_checkpoint.data(), "ab+"); // append
    if (!log_ptr) {
        fclose(checkpoint_ptr);
        if (TRANS_DEBUG) {
            fprintf(trans_ptr, "Trans: End a transformation, no log\n");
            fflush(trans_ptr);
        }
        return;
    }
    std::map<unsigned long long, int> commited; commited.clear();
    std::vector<command> tmp; tmp.clear();
    chfs_command cmd;
    while (cmd.fromString(log_ptr)){
        tmp.push_back(cmd);
        cmd.clear();
    }
    fclose(log_ptr);
    if (TRANS_DEBUG)
        fprintf(trans_ptr, "Trans: all entries count = %d\n", (int)tmp.size());
    for (command cur : tmp) { // compress
        cur.toString(checkpoint_ptr);
        if (TRANS_DEBUG) {
            switch(cur.type) {
                case CMD_CREATE:
                    fprintf(trans_ptr, "Trans: CREATE inum=%d file_type=%d\n", cur.inum, cmd.file_type);
                    break;
                case CMD_GET:
                    fprintf(trans_ptr, "Trans: GET inum=%d\n", cur.inum);
                    break;
                case CMD_GETATTR:
                    fprintf(trans_ptr, "Trans: GETATTR inum=%d\n", cur.inum);
                    break;
                case CMD_PUT:
                    fprintf(trans_ptr, "Trans: PUT inum=%d length=%d\n", cur.inum, cur.length);
                    if (TRANS_DEBUG > 1)
                        fprintf(trans_ptr, "Trans: PUT data=%s\n", cur.data.c_str());
                    break;
                case CMD_REMOVE:
                    fprintf(trans_ptr, "Trans: REMOVE inum=%d\n", cur.inum);
                    break;
                case CMD_BEGIN:
                    fprintf(trans_ptr, "Trans: BEGIN\n");
                    break;
                case CMD_COMMIT:
                    fprintf(trans_ptr, "Trans: COMMIT\n");
                    break;
                default: break;
            }
            fflush(trans_ptr);
        }
    }
    fclose(checkpoint_ptr);

    log_ptr = fopen(file_path_logfile.data(), "wb"); // clean the file
    cur_logsize = 0;
    fclose(log_ptr);
    if (TRANS_DEBUG) {
        fprintf(trans_ptr, "Trans: End a transformation logsize=%d\n", cur_logsize);
        fflush(trans_ptr);
    }
}

template<typename command>
void persister<command>::restore_logdata() {
    // Your code here for lab2A
    //log_ptr = fopen(file_path_logfile.data(), "rb");
    log_ptr = fopen(file_path_checkpoint.data(), "rb");
    if (!log_ptr) return; // log not exist.
    chfs_command cmd;
    while (cmd.fromString(log_ptr)) { // keep reading
        log_entries.push_back(cmd);
        cur_logsize += cmd.getsize();
        cmd.clear();
    }
    fclose(log_ptr);
};

template<typename command>
void persister<command>::restore_checkpoint() {
    // Your code here for lab2A
    checkpoint_ptr = fopen(file_path_checkpoint.data(), "rb");
    if (!checkpoint_ptr) return ;
    chfs_command cmd;
    while (cmd.fromString(checkpoint_ptr)) {
        log_entries.push_back(cmd);
        cmd.clear();
    }
    fclose(checkpoint_ptr);
};

using chfs_persister = persister<chfs_command>;

#endif // persister_h