// chfs client.  implements FS operations using extent and lock server
#include <sstream>
#include <iostream>
#include <vector>
#include <cstring>
#include <string>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "chfs_client.h"
#include "extent_client.h"

#define CHFS_DEBUG 0
/* 
 * Your code here for Lab2A:
 * Here we treat each ChFS operation(especially write operation such as 'create', 
 * 'write' and 'symlink') as a transaction, your job is to use write ahead log 
 * to achive all-or-nothing for these transactions.
 */


/*macro for just if a function is success*/
#define EXT_RPC(xx) do { \
    if ((xx) != extent_protocol::OK) { \
        printf("EXT_RPC Error: %s:%d \n", __FILE__, __LINE__); \
        r = IOERR; \
        goto release; \
    } \
} while (0)

chfs_client::chfs_client(std::string extent_dst, std::string lock_dst)
{
    ec = new extent_client(extent_dst);
    lc = new lock_client(lock_dst);
    unsigned long long tid;
    ec->begin_transaction(tid);
    if (ec->put(1, "", tid) != extent_protocol::OK)
        printf("Fatal:error init root dir\n"); // XYB: init root dir
    ec->end_transaction(tid);
}

/* trans string to ull*/
chfs_client::inum
chfs_client::n2i(std::string n)
{
    std::istringstream ist(n);
    unsigned long long finum;
    ist >> finum;
    return finum;
}

/*trans inum to string*/
std::string
chfs_client::filename(inum inum)
{
    std::ostringstream ost;
    ost << inum;
    return ost.str();
}

/* judge if inum's corresponding file is a file, based on getattr*/
bool
chfs_client::isfile(inum inum)
{
    extent_protocol::attr a;

    unsigned long long tid;
    ec->begin_transaction(tid);
    if (ec->getattr(inum, a, tid) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    ec->end_transaction(tid);
    if (a.type == extent_protocol::T_FILE) {
        if (CHFS_DEBUG)
            printf("isfile: %lld is a file\n", inum);
        return true;
    } 
    if (CHFS_DEBUG)
        printf("isfile: %lld is a dir or symlink\n", inum);
    return false;
}
/** Your code here for Lab...
 * You may need to add routines such as
 * readlink, issymlink here to implement symbolic link.
 * 
 * */
bool
chfs_client::issymlink(inum inum) {
    extent_protocol::attr a;
    
    unsigned long long tid;
    ec->begin_transaction(tid);

    if (ec->getattr(inum, a, tid) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    ec->end_transaction(tid);
    if (a.type == extent_protocol::T_SYM) {
        if (CHFS_DEBUG)
            printf("issymlink: %lld is a symlink\n", inum);
        return true;
    }
    return false;
}

bool
chfs_client::isdir(inum inum)
{
    extent_protocol::attr a;
    unsigned long long tid;
    ec->begin_transaction(tid);

    if (ec->getattr(inum, a, tid) != extent_protocol::OK) {
        printf("error getting attr\n");
        return false;
    }
    ec->end_transaction(tid);
    if (a.type == extent_protocol::T_DIR) {
        if (CHFS_DEBUG)
            printf("isdir: %lld is a dir\n", inum);
        return true;
    }
    return false;
}

int 
chfs_client::symlink(inum parent, const char* link, const char* name, inum &ino_out) 
{
    int r = OK;
    std::string buf;
    
    unsigned long long tid; 
    ec->begin_transaction(tid); // start tX

    EXT_RPC(ec->create(extent_protocol::T_SYM, ino_out, tid));
    buf = std::string(link);
    EXT_RPC(ec->put(ino_out, buf, tid));

    /*add a file to parent*/
    EXT_RPC(ec->get(parent, buf, tid));
    buf = buf + "&" + std::string(name) + "&" + filename(ino_out);
    EXT_RPC(ec->put(parent, buf, tid));

    ec->end_transaction(tid); // end tX
release:
    return r;
}

int
chfs_client::readlink(inum ino, std::string &data)
{
    int r = OK;
    //std::vector<std::string> res;
    std::string buf;
    unsigned long long tid;
    ec->begin_transaction(tid);
    EXT_RPC(ec->get(ino, buf, tid));
    ec->end_transaction(tid);
    //res = split(buf, '\0');
    data = buf;
    if (CHFS_DEBUG)
        printf("&&&&&&& readlink: ino=%lld link=%s\n", ino, buf.c_str());
release:
    return r;
}

/*get the metadata of a file, store in fin, but no type*/
int
chfs_client::getfile(inum inum, fileinfo &fin)
{
    int r = OK;
    unsigned long long tid;
    ec->begin_transaction(tid);
    //printf("getfile inum=%lld\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a, tid) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    ec->end_transaction(tid);
    fin.atime = a.atime;
    fin.mtime = a.mtime;
    fin.ctime = a.ctime;
    fin.size = a.size;
    if (CHFS_DEBUG)
        printf("getfile inum=%lld -> sz %llu\n", inum, fin.size);

release:
    return r;
}

/*get the metadata of a dir*/
int
chfs_client::getdir(inum inum, dirinfo &din)
{
    int r = OK;
    unsigned long long tid;
    ec->begin_transaction(tid);
    if (CHFS_DEBUG)
        printf("getdir inum=%lld\n", inum);
    extent_protocol::attr a;
    if (ec->getattr(inum, a, tid) != extent_protocol::OK) {
        r = IOERR;
        goto release;
    }
    ec->end_transaction(tid);
    din.atime = a.atime;
    din.mtime = a.mtime;
    din.ctime = a.ctime;
    
release:
    return r;
}

// Only support set size of attr
// Your code here for Lab2A: add logging to ensure atomicity
//TODO
int
chfs_client::setattr(inum ino, size_t size)
{
    int r = OK;
    extent_protocol::attr attr;
    std::string buf;
    /*
     * your code goes here.
     * note: get the content of inode ino, and modify its content
     * according to the size (<, =, or >) content length.
     */
    unsigned long long tid;
    ec->begin_transaction(tid);

    EXT_RPC(ec->getattr(ino, attr, tid));
    EXT_RPC(ec->get(ino, buf, tid));
    if (attr.size < size)
        buf += std::string(size - attr.size, '\0');
    else if (attr.size > size)
        buf = buf.substr(0, size);
    EXT_RPC(ec->put(ino, buf, tid));

    ec->end_transaction(tid);
release: 
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::create(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    bool found = false;
    //extent_protocol::attr attr;
    std::string buf;
    unsigned long long tid;
    std::string tmp_buf = "";
    /*
     * your code goes here.
     * note: lookup is what you need to check if file exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */ 
    r = lookup(parent, name, found, ino_out);
    if (r != OK) goto release;
    if (found) {
        r = EXIST;
        goto release;
    }
  
    ec->begin_transaction(tid);   

    EXT_RPC(ec->create(extent_protocol::T_FILE, ino_out, tid));
    //setattr(ino_out, 0); 
    /*is the expansion of setattr*/
 
    // EXT_RPC(ec->getattr(ino_out, attr, tid));
    // EXT_RPC(ec->get(ino_out, tmp_buf, tid));
    // if (attr.size < 0)
    //     tmp_buf += std::string(0 - attr.size, '\0');
    // else if (attr.size > 0)
    //     tmp_buf = tmp_buf.substr(0, 0); 
    EXT_RPC(ec->put(ino_out, tmp_buf, tid));   // actually clean the file

    /*add a dirent to parent's information*/
    EXT_RPC(ec->get(parent, buf, tid));
    buf = buf + "&" + std::string(name) + "&" + filename(ino_out);
    EXT_RPC(ec->put(parent, buf, tid));

    ec->end_transaction(tid);
    if (CHFS_DEBUG)
        printf("create file parent=%lld name=%s inum=%lld\n", parent, name, ino_out);
release: return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::mkdir(inum parent, const char *name, mode_t mode, inum &ino_out)
{
    int r = OK;
    bool found = false;
    std::string buf;
    unsigned long long tid;
    std::string tmp_buf = "";
    /*
     * your code goes here.
     * note: lookup is what you need to check if directory exist;
     * after create file or dir, you must remember to modify the parent infomation.
     */
    r = lookupdir(parent, name, found, ino_out);
    if (r != OK) goto release;
    if (found) {
        r = EXIST;
        goto release;
    }
    r = lookup(parent, name, found, ino_out);
    if (found) {
        r = EXIST;
        goto release;
    }
    
    ec->begin_transaction(tid);   

    EXT_RPC(ec->create(extent_protocol::T_DIR, ino_out, tid));
    //setattr(ino_out, 0); // make the new dir empty

    EXT_RPC(ec->put(ino_out, tmp_buf, tid));   // actually clean the file

    /*add a dirent to parent's information*/
    EXT_RPC(ec->get(parent, buf, tid));
    buf = buf + "&" + std::string(name) + "&" + filename(ino_out);
    EXT_RPC(ec->put(parent, buf, tid));

    ec->end_transaction(tid);
    if (CHFS_DEBUG)
        printf("create dir parent=%lld name=%s inum=%lld\n", parent, name, ino_out);
release:
    return r;
}

/*lookup a file or symlink, rather than a dir*/
int
chfs_client::lookup(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    std::list<dirent> list;
    /*
     * your code goes here.
     * note: lookup file from parent dir according to name;
     * you should design the format of directory content.
     */
    unsigned long long tid;
    ec->begin_transaction(tid);   
    extent_protocol::attr attr;
    ec->getattr(parent, attr, tid);
    ec->end_transaction(tid);
    if (attr.type != extent_protocol::T_DIR)
    {
        return EXIST;
    }

    r = readdir(parent, list);
    if (r != OK) goto release;
    found = false; ino_out = 0;
    for (std::list<dirent>::iterator it = list.begin(); it != list.end(); ++it) {
        if (!strcmp(name, (*it).name.c_str())) {
            if (isfile((*it).inum) || issymlink((*it).inum)) {
                ino_out = (*it).inum;
                found = true;
                goto release;
            }
        }
    }
    if (CHFS_DEBUG)
        printf("lookup file/symlink in parent=%lld name=%s ino=%lld found=%d\n", parent, name, ino_out, found);
release:
    return r;
}

/*lookup a dir*/
int
chfs_client::lookupdir(inum parent, const char *name, bool &found, inum &ino_out)
{
    int r = OK;
    std::list<dirent> list;
    std::list<dirent>::iterator it;
    /*
     * your code goes here.
     * note: lookup dir from parent dir according to name;
     * you should design the format of directory content.
     */

    extent_protocol::attr attr;
    unsigned long long tid;
    ec->begin_transaction(tid);   
    ec->getattr(parent, attr, tid);
    ec->end_transaction(tid);
    if (attr.type != extent_protocol::T_DIR)
    {
        return EXIST;
    }

    r = readdir(parent, list);
    if (r != OK) goto release;
    found = false; ino_out = 0;
    for (it = list.begin(); it != list.end(); ++it) {
        if (!strcmp(name, (*it).name.c_str())) {
            if (CHFS_DEBUG)
                printf("matched name=%s isdir=%d\n", name, isdir((*it).inum));
            if (isdir((*it).inum)) {
                ino_out = (*it).inum;
                found = true;
                break;
            }
        }
    }
    if (CHFS_DEBUG)
        printf("lookupdir in parent=%lld name=%s ino=%lld found=%d\n", parent, name, ino_out, found);
release:
    return r;
}

std::vector<std::string> split(const std::string& str, const std::string& delim) {
	std::vector<std::string> res;
	if(!str.length()) return res;
	//先将要切割的字符串从string类型转换为char*类型
	char * strs = new char[str.length() + 1] ; //不要忘了
	strcpy(strs, str.c_str()); 
	char * d = new char[delim.length() + 1];
	strcpy(d, delim.c_str());
	char *p = strtok(strs, d);
	while(p) {
		std::string s = p; //分割得到的字符串转换为string类型
		res.push_back(s); //存入结果数组
		p = strtok(NULL, d);
	}
    free(d);
    free(strs);
    //printf("success to split %s\n", str.c_str());
	return res;
}

int
chfs_client::readdir(inum dir, std::list<dirent> &list)
{
    int r = OK;
    std::string buf;
    int size;
    std::vector<std::string> res;
    dirent cur;
    /*
     * your code goes here.
     * note: you should parse the dirctory content using your defined format,
     * and push the dirents to the list.
     */
    if (CHFS_DEBUG)
        printf("try to read dir inum=%lld\n", dir);
    if (!isdir(dir)) return chfs_client::NOENT;

    unsigned long long tid;
    ec->begin_transaction(tid);   
    EXT_RPC(ec->get(dir, buf, tid));
    ec->end_transaction(tid);
    res = split(buf, "&");
    size = res.size();
    for (int i = 0; i < size; i += 2) {
        cur.name = res[i];
        cur.inum = n2i(res[i + 1]);
        list.push_back(cur);
    }

release:
    return r;
}

int
chfs_client::read(inum ino, size_t size, off_t off, std::string &data)
{
    int r = OK;
    int actual_size = 0;
    std::string buf;
    /*
     * your code goes here.
     * note: read using ec->get().
     */
    unsigned long long tid;
    ec->begin_transaction(tid);       
    EXT_RPC(ec->get(ino, buf, tid));
    if (CHFS_DEBUG)
        printf("chfs_layer: read inum=%lld buflen=%ld aholefile=%s\n", ino, buf.length(), buf.data());
    if (off + size > buf.length()) {
        actual_size = buf.length() - off;
    }
    else actual_size = size;
    if (actual_size <= 0) {
        data = "";
        if (CHFS_DEBUG)
            printf("chfs_layer: inum=%lld off > filesize, read nothing", ino);
        goto release;
    }
    data = buf.substr(off, actual_size);
    if (CHFS_DEBUG)
        printf("finish read inum=%lld size=%lu actualSize=%d off=%ld data=%s\n", ino, size, actual_size, off, data.c_str());
    ec->end_transaction(tid);
release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int
chfs_client::write(inum ino, size_t size, off_t off, const char *data,
        size_t &bytes_written)
{
    int r = OK;
    std::string buf;
    std::string data_buf = std::string(data, size);
    unsigned long long tid;
    ec->begin_transaction(tid);       
    EXT_RPC(ec->get(ino, buf, tid));
    if (size + off > buf.length())
        buf.resize(off + size, '\0');
    bytes_written = size;
    buf.replace(off, size, data_buf);
    EXT_RPC(ec->put(ino, buf, tid));
    ec->end_transaction(tid);
    if (CHFS_DEBUG) {
        printf("finish write inum=%lld size=%lu off=%ld \n", ino, size, off);
        fflush(stdout);
    }
    /*
     * your code goes here.
     * note: write using ec->put().
     * when off > length of original file, fill the holes with '\0'.
     */
    // EXT_RPC(ec->get(ino, buf));
    // ori_size = buf.length();
    // if ((size_t)off > ori_size) { // fix the hole
    //     for (size_t i = 0; i < off - ori_size; ++i) {
    //         buf += '\0';
    //     }
    //     ori_size = off;
    // }
    // if (off + size > ori_size) { // append
    //     for (size_t i = 0; i < off + size - ori_size; ++i)
    //         buf += 'z';
    // }
    // for (size_t i = 0; i < size; ++i) {
    //     buf[off + i] = data[i];
    //     bytes_written++;
    // }
    // EXT_RPC(ec->put(ino, buf));
    
release:
    return r;
}

// Your code here for Lab2A: add logging to ensure atomicity
int chfs_client::unlink(inum parent,const char *name)
{
    int r = OK;
    int flag = false;
    std::string buf;
    std::list<dirent> list;
    std::list<dirent>::iterator it;
    /*
     * your code goes here.
     * note: you should remove the file using ec->remove,
     * and update the parent directory content.
     */
    if (CHFS_DEBUG)
        printf("start unlink parent=%lld name=%s\n", parent, name);
    r = readdir(parent, list);
    uint32_t target_inum;
    for (it = list.begin(); it != list.end(); it++) {
        if (!strcmp(name, (*it).name.c_str())) {
            if (isfile((*it).inum) || issymlink((*it).inum)) {
                //EXT_RPC(ec->remove((*it).inum));
                target_inum = (*it).inum;
                flag = true;
                list.erase(it);
                break;
            }
        }
    }
    if (!flag) { // not erase
        return chfs_client::NOENT;
    }
    unsigned long long tid;
    ec->begin_transaction(tid);       
    EXT_RPC(ec->remove(target_inum, tid));
    for (it = list.begin(); it != list.end(); it++) {
        buf = buf + "&" + (*it).name + "&" +  filename((*it).inum);
    }
    EXT_RPC(ec->put(parent, buf, tid)); //edit parent file
    ec->end_transaction(tid);
    if (CHFS_DEBUG)
        printf("success unlink: parent=%lld name=%s\n", parent, name);
release:
    return r;
}

