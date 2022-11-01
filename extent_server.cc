// the extent server implementation

#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>
#include <map>

#include "extent_server.h"
#include "persister.h"
#define EXTENT_DEBUG 0 

extent_server::extent_server() 
{
  im = new inode_manager();
  _persister = new chfs_persister("log"); // DO NOT change the dir name here
  
  if (DEBUG_LAB2) {
    debugfp = fopen("readable-log.txt", "a+");
  }
  // Your code here for Lab2A: recover data on startup
  //_persister->restore_checkpoint();
  _persister->restore_logdata();
  std::map<unsigned long long, int> commited; commited.clear();
  for (chfs_command cmd : _persister->log_entries) {
    if (cmd.type == CMD_COMMIT) commited[cmd.id] = 1;
  }
  for (chfs_command cmd : _persister->log_entries) { // for each command
      char *buf = NULL; int fo;
      extent_protocol::attr attr;
      if (commited[cmd.id])
      switch(cmd.type) {
        case CMD_CREATE:
          if (DEBUG_LAB2) {
            fprintf(debugfp, "Restore: CREATE inum=%d file_type=%d\n", cmd.inum, cmd.type);
            if (im->lookup_inode() != cmd.inum)
              fprintf(debugfp, "Fatal: CREATE use different inumber, pre=%d cur=%d\n", cmd.inum, im->lookup_inode());
            fflush(debugfp);
          }
          im->assign_inode(cmd.inum, cmd.file_type);
          break;
        case CMD_PUT:
          if (DEBUG_LAB2) {
            fprintf(debugfp, "Restore: PUT inum=%d length=%d\n", cmd.inum, cmd.length);
            if (DEBUG_LAB2 > 1) 
              fprintf(debugfp, "Restore: PUT data=%s\n", cmd.data.c_str());
            fflush(debugfp);
          }
          im->write_file(cmd.inum, cmd.data.c_str(), cmd.length);
          break;
        case CMD_REMOVE:
          if (DEBUG_LAB2) {
            fprintf(debugfp, "Restore: REMOVE inum=%d\n", cmd.inum);
            fflush(debugfp);
          }
          im->remove_file(cmd.inum);
          break;
        case CMD_GET:
          if (DEBUG_LAB2) {
            fprintf(debugfp, "Restore: GET inum=%d\n", cmd.inum);
            fflush(debugfp);
          }
          buf = NULL;
          im->read_file(cmd.inum, &buf, &fo);
          if (fo != 0)
            free(buf);
          break;
        case CMD_GETATTR:
          if (DEBUG_LAB2) {
            fprintf(debugfp, "Restore: GETATTR inum=%d\n", cmd.inum);
            fflush(debugfp);
          }
          im->get_attr(cmd.inum, attr);
          break;
        default: break;
      }
  }
}

void extent_server::begin_transaction(unsigned long long &tid) {
  tid = time(NULL);
  _persister->append_log(new chfs_command(CMD_BEGIN, tid));
}

void extent_server::end_transaction(unsigned long long tid) {
  _persister->append_log(new chfs_command(CMD_COMMIT, tid));
}

int extent_server::create(uint32_t type, unsigned long long tid, extent_protocol::extentid_t &id)
{
  // alloc a new inode and return inum
  if (EXTENT_DEBUG)
    printf("extent_server: create inode\n");
  id = im->lookup_inode();
  _persister->append_log(new chfs_command(CMD_CREATE, tid, id, type));
  if (DEBUG_LAB2) {
    fprintf(debugfp, "LOG: CREATE inum=%lld file_type=%d\n", id, type);
    fflush(debugfp);
  }
  im->assign_inode(id, type);

  return extent_protocol::OK;
}

int extent_server::put(extent_protocol::extentid_t id, unsigned long long tid, std::string buf, int &)
{
  id &= 0x7fffffff;
  
  const char * cbuf = buf.c_str();
  int size = buf.size();
  _persister->append_log(new chfs_command(CMD_PUT, tid, id, size, buf));
  if (DEBUG_LAB2) {
    fprintf(debugfp, "LOG: PUT inum=%lld length=%d\n", id, size);
    if (DEBUG_LAB2 > 1) 
      fprintf(debugfp, "LOG: PUT data=%s\n", buf.data());
    fflush(debugfp);
  }
  im->write_file(id, cbuf, size);
  
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, unsigned long long tid, std::string &buf)
{
  if (EXTENT_DEBUG)
    printf("extent_server: get %lld\n", id);

  id &= 0x7fffffff;

  int size = 0;
  char *cbuf = NULL;

  _persister->append_log(new chfs_command(CMD_GET, tid, id));
  if (DEBUG_LAB2) {
    fprintf(debugfp, "LOG: GET inum=%lld\n", id);
    fflush(debugfp);
  }
  im->read_file(id, &cbuf, &size);
  if (size == 0)
    buf = "";
  else {
    buf.assign(cbuf, size);
    free(cbuf);
  }

  return extent_protocol::OK;
}

int extent_server::getattr(extent_protocol::extentid_t id, unsigned long long tid, extent_protocol::attr &a)
{
  if (EXTENT_DEBUG)
    printf("extent_server: getattr %lld\n", id);

  id &= 0x7fffffff;
  
  extent_protocol::attr attr;
  memset(&attr, 0, sizeof(attr));
  _persister->append_log(new chfs_command(CMD_GETATTR, tid, id));
  if (DEBUG_LAB2) {
    fprintf(debugfp, "LOG: GETATTR inum=%lld\n", id);
    fflush(debugfp);
  }
  im->get_attr(id, attr);
  a = attr;

  return extent_protocol::OK;
}

int extent_server::remove(extent_protocol::extentid_t id, unsigned long long tid, int &)
{
  if (EXTENT_DEBUG)
    printf("extent_server: write %lld\n", id);

  id &= 0x7fffffff;
  _persister->append_log(new chfs_command(CMD_REMOVE, tid, id));
  if (DEBUG_LAB2) {
    fprintf(debugfp, "LOG: REMOVE inum=%lld\n", id);
    fflush(debugfp);
  }
  im->remove_file(id);
 
  return extent_protocol::OK;
}

