// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

extent_client::extent_client(std::string dst)
{
  sockaddr_in dstsock;
  make_sockaddr(dst.c_str(), &dstsock);
  cl = new rpcc(dstsock);
  if (cl->bind() != 0) {
    printf("extent_client: bind failed\n");
  }
}

extent_protocol::status
extent_client::create(uint32_t type, extent_protocol::extentid_t &id, unsigned long long tid)
{
  extent_protocol::status ret = cl->call(extent_protocol::create, cl->id(), type, tid, id);
  //VERIFY (ret == extent_protocol::OK);
  // Your lab2B part1 code goes here
  //ret = es->create(type, id, tid);
  return ret;
}

extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf, unsigned long long tid)
{
  extent_protocol::status ret = cl->call(extent_protocol::get, cl->id(), eid, tid, buf);
  //VERIFY (ret == extent_protocol::OK);
  // Your lab2B part1 code goes here
  //ret = es->get(eid, buf, tid);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr, unsigned long long tid)
{
  extent_protocol::status ret = cl->call(extent_protocol::getattr, cl->id(), eid, tid, attr);
  //VERIFY (ret == extent_protocol::OK);
  // Your lab2B part1 code goes here
  //ret = es->getattr(eid, attr, tid);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf, unsigned long long tid)
{
  int r;
  extent_protocol::status ret = cl->call(extent_protocol::put, cl->id(), eid, tid, buf, r);
  //VERIFY (ret == extent_protocol::OK);
  // Your lab2B part1 code goes here
  //ret = es->put(eid, buf, r, tid);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid, unsigned long long tid)
{
  int r;
  extent_protocol::status ret = cl->call(extent_protocol::remove, cl->id(), eid, tid, r);
  //VERIFY (ret == extent_protocol::OK);
  // Your lab2B part1 code goes here
  //ret = es->remove(eid, r, tid);
  return ret;
}

void
extent_client::begin_transaction(unsigned long long &tid) {
  //es->begin_transaction(tid);
}

void 
extent_client::end_transaction(unsigned long long tid) {
  //es->end_transaction(tid);
}
