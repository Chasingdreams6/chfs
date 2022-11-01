#include <iostream>
using namespace std;

void write_int_to_buf(char *buf, int id, int val) {
  *(int *) (buf + id * 4) = val;
}
/*
  从buf中读第id个int，存在val中
*/
void read_int_from_buf(char *buf, int id, int &val) {
    val = * (int *)(buf + id * 4);
}

enum cmd_type {
    CMD_BEGIN = 0,
    CMD_COMMIT,
    CMD_CREATE,
    CMD_PUT,
    CMD_REMOVE,
};

class chfs_command {
public:
    typedef unsigned long long txid_t;


    cmd_type type = CMD_BEGIN;
    txid_t id = 0;
    uint32_t inum, length = 0;
    std::string data;

    // constructor
    chfs_command() {}

    uint64_t size() const { // return byte
        uint64_t s = sizeof(cmd_type) + sizeof(txid_t);
        if (type != CMD_BEGIN && type != CMD_COMMIT) // inum
            s += sizeof(uint32_t);
        if (type == CMD_PUT)
            s += sizeof(uint32_t) + length;
        return s;
    }
    void toString(FILE *stream) {
        fwrite(&type, 1, 1, stream);
        fwrite(&id, 8, 1, stream);
        if (type != CMD_BEGIN && type != CMD_COMMIT)
          fwrite(&inum, 4, 1, stream);
        if (type == CMD_PUT) {
          fwrite(&length, 4, 1, stream);
          fwrite(data.data(), 1, length, stream);
        }
    }
    void fromString(FILE* stream) {
        char buf[512*205];
        int type_num;
        int tmp = fread(&type_num, 1, 1, stream); 
        tmp = fread(&id, 8, 1, stream);
        switch(type_num) {
           case 0: type = CMD_BEGIN; break;
           case 1: type = CMD_COMMIT; break;
           case 2: type = CMD_CREATE; break;
           case 3: type = CMD_PUT; break;
           case 4: type = CMD_REMOVE; break;
        }
        if (type != CMD_BEGIN && type != CMD_COMMIT) {
          fread(&inum, 4, 1, stream);
        }
        if (type == CMD_PUT) {
          fread(&length, 4, 1, stream);
          fread(buf, 1, length, stream);
          data = string(buf);
        }
    }
};

int main() {
   FILE* outstream = fopen("tmp.bin", "wb");
   chfs_command cmd1; cmd1.type = CMD_PUT; cmd1.id = 237464655683; cmd1.inum = 55234; cmd1.length = 7; cmd1.data = "22 \0 44";
   cout << cmd1.type << " " << cmd1.id << " " << cmd1.inum <<  " " << cmd1.length << " " << cmd1.data << endl;
   cmd1.toString(outstream);
   fclose(outstream);

   FILE* instream = fopen("tmp.bin", "rb");
   chfs_command cmd2;
   cmd2.fromString(instream);
   cout << cmd2.type << " " << cmd2.id << " "  << cmd2.inum << " " << cmd2.length << " " << cmd2.data << endl;
   fclose(instream);
   return 0;
}
