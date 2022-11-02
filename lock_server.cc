// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

lock_server::lock_server():
  nacquire (0)
{
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  VERIFY (clt != 0);
  pthread_mutex_lock(&mutex);
  while (usedLock.count(lid))
    pthread_cond_wait(&cond, &mutex);
  usedLock[lid] = clt;
  std::cout << "acquire lock from clt=" << clt << " lid=" << lid << std::endl;
  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
	// Your lab2B part2 code goes here
  VERIFY (clt != 0);
  pthread_mutex_lock(&mutex);
  if (usedLock[lid] == clt) {
    usedLock.erase(lid);
    std::cout << "release lock from clt=" << clt << " lid=" << lid << std::endl;
  }
  pthread_mutex_unlock(&mutex);
  pthread_cond_broadcast(&cond);
  return ret;
}