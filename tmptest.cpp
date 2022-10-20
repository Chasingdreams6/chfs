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

int main() {
   string d = "xxx";
   d = d.substr(0, 0);
   cout << d.length() << " " << d << endl;
   return 0;
}