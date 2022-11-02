// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include "extent_protocol.h"

#include "inode_manager.h"
#include "persister.h"

class extent_server {
 protected:
#if 0
  typedef struct extent {
    std::string data;
    struct extent_protocol::attr attr;
  } extent_t;
  std::map <extent_protocol::extentid_t, extent_t> extents;
#endif
  inode_manager *im;
  chfs_persister *_persister;

  FILE* debugfp;
 public:
  extent_server();
  int create(int clt, uint32_t type, unsigned long long tid, extent_protocol::extentid_t &id);
  int put(int clt, extent_protocol::extentid_t id, unsigned long long tid, std::string, int &);
  int get(int clt, extent_protocol::extentid_t id, unsigned long long tid, std::string &);
  int getattr(int clt, extent_protocol::extentid_t id, unsigned long long tid, extent_protocol::attr &);
  int remove(int clt, extent_protocol::extentid_t id, unsigned long long tid, int &);
  void begin_transaction(unsigned long long &tid);
  void end_transaction(unsigned long long tid);

  // Your code here for lab2A: add logging APIs
};

#endif 







