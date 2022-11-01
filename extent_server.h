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

  int create(uint32_t type, extent_protocol::extentid_t &id, unsigned long long tid);
  int put(extent_protocol::extentid_t id, std::string, int &, unsigned long long tid);
  int get(extent_protocol::extentid_t id, std::string &, unsigned long long tid);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &, unsigned long long tid);
  int remove(extent_protocol::extentid_t id, int &, unsigned long long tid);
  void begin_transaction(unsigned long long &tid);
  void end_transaction(unsigned long long tid);

  // Your code here for lab2A: add logging APIs
};

#endif 







