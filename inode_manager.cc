#include "inode_manager.h"
#include <ctime>
#include <cmath>
#include <fstream>
#include <string>
#include "map"
// disk layer -----------------------------------------

#define MIN(a,b) ((a)<(b) ? (a) : (b))
#define BITMAP_BIOS(block) (((block) % (BPB)) >> 3) // the block's byte postition of that block
#define BIT_SHIFT(block) (((block) % (BPB)) % 8)  // the bit postion of that's block's Byte.
#define SHIFTED(block) ((1 << (BIT_SHIFT(block))) & 0xff) // 1 Byte, only the block's bit is 1
#define SHIFTED_0(block) ((~ (1 << BIT_SHIFT(block))) & 0xff) // 1 Byte, only the block's bit is 0
#define CHECK_USED(block, ch) (((ch) >> (BIT_SHIFT(block))) & 1) // return 1, block is used;
#define DEBUG_MODE 0
// mode = 0, no output to part1_log, mode = 1, print i-node layer message, mode = 2, print block layer message.
std::ofstream outfile;
const std::string log_dst = "part1_log.out";

disk::disk()
{
  bzero(blocks, sizeof(blocks));
}

void
disk::read_block(blockid_t id, char *buf) // buf contains all bits from block[id], buf must > a block
{
  for (int i = 0; i < BLOCK_SIZE; ++i) buf[i] = blocks[id][i];
}

void
disk::write_block(blockid_t id, const char *buf) // write first BLOCK_SIZE'S char to the block
{
  //*(blocks[id]) = *buf;
  int len = BLOCK_SIZE;
  for (int i = 0; i < len; ++i) blocks[id][i] = buf[i];
}

int
disk::read_bit(blockid_t id, int byte_bios, int bit_bios) // Or can use using_blocks..
{
   return (blocks[id][byte_bios] >> bit_bios) & 0x1;
}

void
disk::set_bit_one(blockid_t id, int byte_bios, int bit_bios) // cause the system actually in mem, so i can use this trick..
{
  blocks[id][byte_bios] |= ((1 << bit_bios) & 0xff);
}

void
disk::set_bit_zero(blockid_t id, int byte_bios, int bit_bios)
{
  blocks[id][byte_bios] &= ((~(1 << bit_bios)) & 0xff);
}
// block layer -----------------------------------------

std::string
inode_manager::snapshot()
{
  std::string res;
  for (int i = 0; i < BLOCK_NUM; ++i) {
    res = res + std::string((const char *)bm->d->blocks[i], BLOCK_SIZE);
  }
  return res;
}

void
inode_manager::restore_snapshot(std::string s)
{
  for (int i = 0; i < BLOCK_NUM; ++i) {
    for (int j = 0; j < BLOCK_SIZE; ++j) {
      bm->d->blocks[i][j] = (unsigned char)s[i * BLOCK_NUM + j];
    }
  }
}

// Allocate a free disk block.
blockid_t
block_manager::alloc_block()
{
  /*
   * your code goes here.
   * note: you should mark the corresponding bit in block bitmap when alloc.
   * you need to think about which block you can start to be allocated.
   */
  // char curBlock[BLOCK_SIZE]; // buffer
  blockid_t startBlock = IBLOCK(INODE_NUM, sb.nblocks) + 1; // the first block can be used
  while (startBlock < BLOCK_NUM) {
    // if (using_blocks.find(startBlock) != using_blocks.end()) { // double check
    //     startBlock++;
    //     continue;
    // }
    // d->read_block(BBLOCK(startBlock), curBlock);
    // if (CHECK_USED(startBlock, curBlock[BITMAP_BIOS(startBlock)])) {
    //   startBlock++;
    //   continue;
    // }
    if (d->read_bit(BBLOCK(startBlock), BITMAP_BIOS(startBlock), BIT_SHIFT(startBlock))) {
      startBlock++;
      continue;
    }
    break;
  }
  if (startBlock == BLOCK_NUM) {
      printf("fatal error: block used up");
      fflush(stdout);
      if (DEBUG_MODE > 1) {
        outfile << "fatal error: block used up\n";
        outfile.flush();
      }
      return 0;
  }
  //using_blocks[startBlock] = 1; // set using_blocks to 1
  
  // d->read_block(BBLOCK(startBlock), curBlock);
  // curBlock[BITMAP_BIOS(startBlock)] |= SHIFTED(startBlock); // set bitmap to 1
  // d->write_block(BBLOCK(startBlock), curBlock);
  d->set_bit_one(BBLOCK(startBlock), BITMAP_BIOS(startBlock), BIT_SHIFT(startBlock));
  if (DEBUG_MODE > 1) {
    outfile << "alloc_block id=" << startBlock << "\n";
    outfile.flush();
  }
  return startBlock;
}

void
block_manager::free_block(uint32_t id)
{
  /* 
   * your code goes here.
   * note: you should unmark the corresponding bit in the block bitmap when free.
   */
  if (id >= BLOCK_NUM) return; // out of range
  // if (using_blocks.find(id) == using_blocks.end()) return ; // the block is not used
  // using_blocks.erase(id);
  if (!d->read_bit(BBLOCK(id), BITMAP_BIOS(id), BIT_SHIFT(id))) return ; // not used
  d->set_bit_zero(BBLOCK(id), BITMAP_BIOS(id), BIT_SHIFT(id));
  // char curBlock[BLOCK_SIZE];
  // d->read_block(BBLOCK(id), curBlock);
  // curBlock[BITMAP_BIOS(id)] &= SHIFTED_0(id); // set bitmap to 0
  // d->write_block(BBLOCK(id), curBlock);
  return;
}

// The layout of disk should be like this:
// |<-sb->|<-free block bitmap->|<-inode table->|<-data->|
block_manager::block_manager()
{
  d = new disk();

  // format the disk
  sb.size = BLOCK_SIZE * BLOCK_NUM;
  sb.nblocks = BLOCK_NUM;
  sb.ninodes = INODE_NUM;

}

void
block_manager::read_block(uint32_t id, char *buf)
{
  d->read_block(id, buf);
}

void
block_manager::write_block(uint32_t id, const char *buf)
{
  d->write_block(id, buf);
}

// inode layer -----------------------------------------

inode_manager::inode_manager()
{
  bm = new block_manager();
  uint32_t root_dir = alloc_inode(extent_protocol::T_DIR);
  if (root_dir != 1) {
    printf("\tim: error! alloc first inode %d, should be 1\n", root_dir);
    fflush(stdout);
    if (DEBUG_MODE) {
      outfile << "im: error! alloc first inode " << root_dir << ", should be 1\n" << std::endl;
      outfile.flush();
    }
    exit(0);
  }
  if (DEBUG_MODE) {
    outfile.open(log_dst.c_str(), std::ios::out);
  }
}

inode_manager::~inode_manager() {
    outfile.close();
}

long getTimeNs() {
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec * 1000000000 + ts.tv_nsec;
}
/* Create a new file.
 * Return its inum. */
uint32_t
inode_manager::alloc_inode(uint32_t type)
{
  /* 
   * your code goes here.
   * note: the normal inode block should begin from the 2nd inode block.
   * the 1st is used for root_dir, see inode_manager::inode_manager().
   */
  uint32_t inum = 1; // start from 1
  struct inode* ino;
  while (inum < INODE_NUM) {
    ino = get_inode(inum);
    if (ino->type != extent_protocol::T_DIR 
            && ino->type != extent_protocol::T_FILE 
            && ino->type != extent_protocol::T_SYM) break;
    inum++;
  }
  if (inum == INODE_NUM) return 0; // alloc fail
  ino->type = type;
  ino->size = 0;
  ino->atime = ino->ctime = ino->mtime = getTimeNs();
  put_inode(inum, ino);
  printf("alloc_inode: inum=%d\n", inum);
  fflush(stdout);
  if (DEBUG_MODE) {
    outfile << "alloc_inode: " << inum << std::endl;
    outfile.flush();
  }
  return inum;
}

/*寻找一个可用的inode*/
uint32_t 
inode_manager::lookup_inode() 
{
  uint32_t inum = 1; // start from 1
  struct inode* ino;
  while (inum < INODE_NUM) {
    ino = get_inode(inum);
    if (ino->type != extent_protocol::T_DIR 
            && ino->type != extent_protocol::T_FILE 
            && ino->type != extent_protocol::T_SYM) break;
    inum++;
  }
  if (inum == INODE_NUM) return 0; // alloc fail
  return inum;
}

void
inode_manager::assign_inode(uint32_t inum, uint32_t type) 
{
    struct inode* ino = get_inode(inum);
    ino->type = type;
    ino->size = 0;
    ino->atime = ino->ctime = ino->mtime = getTimeNs();
    put_inode(inum, ino);
}

void
inode_manager::free_inode(uint32_t inum)
{
  /* 
   * your code goes here.
   * note: you need to check if the inode is already a freed one;
   * if not, clear it, and remember to write back to disk.
   */
  struct inode* ino = get_inode(inum);
  if (ino->type != extent_protocol::T_DIR 
        && ino->type != extent_protocol::T_FILE 
        && ino->type != extent_protocol::T_SYM) return ; // already deleted
  ino->type = 0; // type设置为0表示删除了
  ino->size = 0;
  ino->atime = ino->ctime = ino->mtime = getTimeNs();
  put_inode(inum, ino);
  printf("free_inode: inum=%d\n", inum);
  fflush(stdout);
  if (DEBUG_MODE) {
    outfile << "free_inode: " << inum << std::endl;
    outfile.flush();
  }
  return;
}


/* Return an inode structure by inum, NULL otherwise.
 * Caller should release the memory. */
/*
  从disk上读取到inode，深copy
*/
struct inode* 
inode_manager::get_inode(uint32_t inum)
{
  struct inode *ino = (struct inode *)malloc(sizeof(inode));
  struct inode *ino_disk;
  /* 
   * your code goes here.
   */
  char buf[BLOCK_SIZE];
  if (inum >= INODE_NUM) return NULL; // out of range
  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  ino_disk->atime = ino_disk->ctime = ino_disk->mtime = getTimeNs();
  //printf("buf=%s len=%d\n", buf, (int)strlen(buf));
  *ino = *ino_disk;
  //bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
  return ino;
}

/*
  将ino持久化到inum对应的disk上
*/
void
inode_manager::put_inode(uint32_t inum, struct inode *ino)
{
  char buf[BLOCK_SIZE];
  struct inode *ino_disk;

  printf("\tim: put_inode %d\n", inum);
  fflush(stdout);
  //printf("\tim: put_inode inum=%d size=%d\n", inum, ino->size);
  if (ino == NULL)
    return;

  bm->read_block(IBLOCK(inum, bm->sb.nblocks), buf);
  ino_disk = (struct inode*)buf + inum%IPB;
  ino->ctime = ino->mtime = ino->atime = getTimeNs();
  *ino_disk = *ino;
  bm->write_block(IBLOCK(inum, bm->sb.nblocks), buf);
}

/*
  将val写到字符串buf中，是buf中存的第id个 (从0开始)
*/
int Bit_Bios;

void write_int_to_buf(unsigned char *buf, int id, int val) {
  *(int *) (buf + id * 4) = val;
}
/*
  从buf中读第id个int，存在val中
*/
void read_int_from_buf(unsigned char *buf, int id, blockid_t &val) {
    val = * (int *)(buf + id * 4);
}

/* Get all the data of a file by inum. 
 * Return alloced data, should be freed by caller. */
void
inode_manager::read_file(uint32_t inum, char **buf_out, int *size)
{
  /*
   * your code goes here.
   * note: read blocks related to inode number inum,
   * and copy them to buf_out
   */
  struct inode *ino = get_inode(inum);
  if (DEBUG_MODE) {
    outfile << "read file inum=" << inum << " size=" << ino->size << "\n";
    outfile.flush();
  }
  int block_needed = ino->size / BLOCK_SIZE;
  if ((unsigned int)block_needed * BLOCK_SIZE < ino->size) block_needed++;
  char *res = (char *)malloc(sizeof(char) * block_needed * BLOCK_SIZE);
  *buf_out = res;
  *size = ino->size;
  if (block_needed <= NDIRECT) {
     for (int i = 0; i < block_needed; ++i) {
        blockid_t id = ino->blocks[i]; // 第id个block
        if (DEBUG_MODE > 1) {
          outfile << "read block id=" << id << "\n";
          outfile.flush();
        }
        bm->read_block(id, &res[i * BLOCK_SIZE]);
     }
  }
  else {
    for (int i = 0; i < NDIRECT; ++i) {
        blockid_t id = ino->blocks[i]; // 第id个block
        if (DEBUG_MODE > 1) {
          outfile << "read block id=" << id << "\n";
          outfile.flush();
        }
        bm->read_block(id, &res[i * BLOCK_SIZE]);
    }
    char buf[BLOCK_SIZE];
    uint32_t indirectBlockID = ino->blocks[NDIRECT];
    unsigned char bufx[BLOCK_SIZE];
    bm->read_block(indirectBlockID, buf);
    memcpy(bufx, buf, BLOCK_SIZE);
    for (int i = NDIRECT; i < block_needed; ++i) {
        blockid_t id;
        read_int_from_buf(bufx, i - NDIRECT, id);
        if (DEBUG_MODE > 1) {
          outfile << "read block id=" << id << "\n";
          outfile.flush();
        }
        bm->read_block(id, &res[i * BLOCK_SIZE]);
    }
  }
  return;
}

/*
  debug output
*/
void D() {
  puts("qwq");
}

/* alloc/free blocks if needed */
void
inode_manager::write_file(uint32_t inum, const char *buf, int size)
{
  /*
   * your code goes here.
   * note: write buf to blocks of inode inum.
   * you need to consider the situation when the size of buf 
   * is larger or smaller than the size of original inode
   */
  printf("im write file: inum=%d size=%d \n", inum, size);
  fflush(stdout);
  if (DEBUG_MODE) {
    outfile << "im write file: inum=" << inum << " size=" << size << "\n";
    outfile.flush();
  }
  struct inode *ino = get_inode(inum);
  /*  free history blocks */
  if (ino->size != 0) { 
    int last_block = ino->size / BLOCK_SIZE;
    if ((unsigned int)last_block * BLOCK_SIZE < ino->size) last_block++;
    if (last_block <= NDIRECT) {
      for (int i = 0; i < last_block; ++i) {
        uint32_t block_id = ino->blocks[i];
        if (DEBUG_MODE > 1) {
          outfile << "free block id=" << block_id << "\n";
          outfile.flush();
        }
        bm->free_block(block_id);
      }
      memset(ino->blocks, 0, sizeof(ino->blocks));
    }
    else {
      for (int i = 0; i < NDIRECT; ++i) {
        uint32_t block_id = ino->blocks[i];
        if (DEBUG_MODE > 1) {
          outfile << "free block id=" << block_id << "\n";
          outfile.flush();
        }
        bm->free_block(block_id);
      }
      char buf[BLOCK_SIZE];
      uint32_t indirectBlockID = ino->blocks[NDIRECT];
      unsigned char bufx[BLOCK_SIZE];
      bm->read_block(indirectBlockID, buf);
      memcpy(bufx, buf, BLOCK_SIZE);
      for (int i = NDIRECT; i < last_block; ++i) {
          blockid_t id;
          read_int_from_buf(bufx, i - NDIRECT, id);
          if (DEBUG_MODE > 1) {
            outfile << "free block id=" << id << "\n";
            outfile.flush();
          }
          bm->free_block(id);
      }
      if (DEBUG_MODE > 1) {
          outfile << "free indirect block id=" << indirectBlockID << "\n";
          outfile.flush();
      }
      bm->free_block(indirectBlockID);
      memset(ino->blocks, 0, sizeof(ino->blocks));
    }
  }
  /*calculate new blocks number*/
  int block_needed = size / BLOCK_SIZE, cntBlock = 0, lastlen = 0;
  if (block_needed * BLOCK_SIZE < size) {
     lastlen = size - block_needed * BLOCK_SIZE;
     block_needed++;
  }
  else lastlen = BLOCK_SIZE;
  /*direct*/
  if (block_needed <= NDIRECT) { // 直接映射
    for (int i = 0; i < block_needed - 1; ++i) {
      uint32_t block_id = bm->alloc_block();
      if (DEBUG_MODE > 1) {
          outfile << "write filled block id=" << block_id << "\n";
          outfile.flush();
      }
      bm->write_block(block_id, &buf[i * BLOCK_SIZE]);
      ino->blocks[cntBlock++] = block_id;
    }
    if (lastlen > 0) { // 最后余一小块
        uint32_t block_id = bm->alloc_block();
        char tmp[BLOCK_SIZE];
        memcpy(tmp, &buf[(block_needed - 1) * BLOCK_SIZE], lastlen);
        if (DEBUG_MODE > 1) {
          outfile << "write unfilled block id=" << block_id << " len=" << lastlen << "\n";
          outfile.flush();
        }
        bm->write_block(block_id, tmp);
        ino->blocks[cntBlock++] = block_id;
    }
  }
  else {
    for (int i = 0; i < NDIRECT; ++i) { 
      uint32_t block_id = bm->alloc_block();
      if (DEBUG_MODE > 1) {
          outfile << "write filled block id=" << block_id << "\n";
          outfile.flush();
      }
      bm->write_block(block_id, &buf[i * BLOCK_SIZE]);
      ino->blocks[cntBlock++] = block_id;
    }
    uint32_t indirectBlockID = bm->alloc_block();
    unsigned char bufx[BLOCK_SIZE]; // 存间接映射的块
    ino->blocks[cntBlock++] = indirectBlockID;
    for (int i = NDIRECT; i < block_needed - 1; ++i) { // 使用一层间接映射
      uint32_t block_id = bm->alloc_block();
      bm->write_block(block_id, &buf[i * BLOCK_SIZE]);
      write_int_to_buf(bufx, i - NDIRECT, block_id);
      if (DEBUG_MODE > 1) {
          outfile << "write filled block id=" << block_id << "\n";
          outfile.flush();
      }
    }
    if (lastlen > 0) { // 最后余一小块
        uint32_t block_id = bm->alloc_block();
        char tmp[BLOCK_SIZE];
        memcpy(tmp, &buf[(block_needed - 1) * BLOCK_SIZE], lastlen);
        bm->write_block(block_id, tmp);
        write_int_to_buf(bufx, block_needed - 1 - NDIRECT, block_id);
        if (DEBUG_MODE > 1) {
          outfile << "write unfilled block id=" << block_id << " len=" << lastlen << "\n";
          outfile.flush();
        }
    }
    char tmpbuf[BLOCK_SIZE];
    memcpy(tmpbuf, bufx, BLOCK_SIZE);
    bm->write_block(indirectBlockID, tmpbuf);
    if (DEBUG_MODE > 1) {
        outfile << "write indirect block id=" << indirectBlockID << "\n";
        outfile.flush();
    }
  }
  ino->size = size;
  ino->atime = ino->ctime = ino->mtime = getTimeNs();
  put_inode(inum, ino);
  return;
}

void
inode_manager::get_attr(uint32_t inum, extent_protocol::attr &a)
{
  /*
   * your code goes here.
   * note: get the attributes of inode inum.
   * you can refer to "struct attr" in extent_protocol.h
   */
  struct inode* ino = get_inode(inum);
  a.atime = ino->atime; 
  a.ctime = ino->ctime;
  a.mtime = ino->mtime;
  a.type = ino->type;
  a.size = ino->size;
  return;
}

void
inode_manager::remove_file(uint32_t inum)
{
  /*
   * your code goes here
   * note: you need to consider about both the data block and inode of the file
   */
  struct inode *ino = get_inode(inum);
  int block_needed = ino->size / BLOCK_SIZE;
  if ((unsigned int)block_needed * BLOCK_SIZE < ino->size) block_needed++;
  if (block_needed <= NDIRECT) {
      for (int i = 0; i < block_needed; ++i) {
        blockid_t id = ino->blocks[i];
        bm->free_block(id);
      } 
  }
  else {
      for (int i = 0; i < NDIRECT; ++i) {
        blockid_t id = ino->blocks[i];
        bm->free_block(id);
      } 
      uint32_t indirectBlockID = ino->blocks[NDIRECT];
      char buf[BLOCK_SIZE];
      unsigned char bufx[BLOCK_SIZE];
      bm->read_block(indirectBlockID, buf);
      memcpy(bufx, buf, BLOCK_SIZE);
      for (int i = NDIRECT; i < block_needed; ++i) {
          blockid_t id;
          read_int_from_buf(bufx, i - NDIRECT, id);
          bm->free_block(id);
      }
      bm->free_block(indirectBlockID);
  }
  free_inode(inum);
  return;
}
