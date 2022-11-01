// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <string>
#include "extent_protocol.h"
#include "extent_server.h"

class extent_client {
 private:
  rpcc *cl;

 public:
  extent_client(std::string dst);

  extent_protocol::status create(uint32_t type, extent_protocol::extentid_t &eid, unsigned long long tid);
  extent_protocol::status get(extent_protocol::extentid_t eid, 
			                        std::string &buf, unsigned long long tid);
  extent_protocol::status getattr(extent_protocol::extentid_t eid, 
				                          extent_protocol::attr &a, unsigned long long tid);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf, unsigned long long tid);
  extent_protocol::status remove(extent_protocol::extentid_t eid, unsigned long long tid);
  void begin_transaction(unsigned long long &tid);
  void end_transaction(unsigned long long tid);
};

#endif 

