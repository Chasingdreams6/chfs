#include <iostream>
#include <fstream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>

#include <mutex>
#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include "rpc.h"
#include "mr_protocol.h"

using namespace std;

struct KeyVal {
    string key;
    string val;
    KeyVal(string key1, string val1) : key(key1), val(val1) {}
};

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
bool isLetter(char ch) {
    return ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z';
}
vector<KeyVal> Map(const string &filename, const string &content)
{
    unordered_map<string, int> map;
    vector<KeyVal> res;
    string word;
    for (auto ch : content) {
        if (isLetter(ch)) word += ch;
        else if (word.size()) {
            map[word]++;
            word.clear();
        }
    }
    for (auto kv : map ) {
        res.emplace_back(KeyVal(kv.first, to_string(kv.second)));
    }
    return res;
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
string strPlus(string str1, string str2) {
    return to_string(stol(str1) + stol(str2));
};
string Reduce(const string &key, const vector < string > &values)
{
    // Copy your code from mr_sequential.cc here.
    string ret = "0";
    for (auto str : values)
        ret = strPlus(ret, str);
    return ret;
}


typedef vector<KeyVal> (*MAPF)(const string &key, const string &value);
typedef string (*REDUCEF)(const string &key, const vector<string> &values);

class Worker {
public:
	Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf);

	void doWork();

private:
	void doMap(int index, const string &filename);
	void doReduce(int index, int nfiles);
	void doSubmit(mr_tasktype taskType, int index);
    void askTask(mr_protocol::AskTaskResponse &);

    bool working = false;
	mutex mtx;
	int id;

	rpcc *cl;
	std::string basedir;
    std::string hackdir = "../";
	MAPF mapf;
	REDUCEF reducef;
};


Worker::Worker(const string &dst, const string &dir, MAPF mf, REDUCEF rf)
{
	this->basedir = dir;
	this->mapf = mf;
	this->reducef = rf;

	sockaddr_in dstsock;
	make_sockaddr(dst.c_str(), &dstsock);
	this->cl = new rpcc(dstsock);
	if (this->cl->bind() < 0) {
		printf("mr worker: call bind error\n");
	}
}
int strHash(const string &str) {
    unsigned int hashVal = 0;
    for (char ch : str) {
        hashVal = hashVal * 79 + (int) ch;
    }
    return hashVal % REDUCER_COUNT;
}
void Worker::doMap(int index, const string &filename)
{
	// Lab4: Your code goes here.
    if (!filename.size()) return;
    working = true;
    cout << "baseDir:" << basedir << endl;
    cout << "doMap File:" << filename << endl;
    string prefix = hackdir + "tmr-" + to_string(index) + "-";
    string content;
    ifstream file(filename);
    ostringstream tmp;
    tmp << file.rdbuf();
    content = tmp.str();

    vector <KeyVal> keyVals = Map(filename, content);

    vector <string> contents(REDUCER_COUNT);
    for (const KeyVal &keyVal : keyVals) {
        int reducerId = strHash(keyVal.key);
        contents[reducerId] += keyVal.key + ' ' + keyVal.val + '\n';
    }

    for (int i = 0; i < REDUCER_COUNT; ++i) {
        const string &content = contents[i];
        if (!content.empty()) {
            string intermediateFilepath = prefix + to_string(i);
            ofstream file(intermediateFilepath, ios::out);
            file << content;
            file.close();
        }
    }
    file.close();
}

void Worker::doReduce(int index, int nfiles)
{
	// Lab4: Your code goes here.
    working = true;
    string filepath;
    unordered_map<string, unsigned long long> map;
    cout << "id = " << index << " Start do reduce" << endl;
    for (int i = 0; i < nfiles; ++i) {
        filepath = hackdir + "tmr-" + to_string(i) + '-' + to_string(index);
        ifstream file(filepath, ios::in);
        if (!file.is_open()) {
            continue;
        }
        string key, value;
        while (file >> key >> value)
            map[key] += atoll(value.c_str());
        file.close();
    }

    string content;
    for (auto keyVal : map)
        content += keyVal.first + ' ' + to_string(keyVal.second) + '\n';

    ofstream mrOut(basedir + "mr-out", ios::out | ios::app);
    cout << "After Reduce to ->" + basedir + "mr-out" << endl;
    mrOut << content << endl;
    mrOut.close();
}

void Worker::doSubmit(mr_tasktype taskType, int index)
{
	bool b;
	mr_protocol::status ret = this->cl->call(mr_protocol::submittask, taskType, index, b);
	if (ret != mr_protocol::OK) {
		fprintf(stderr, "submit task failed\n");
		exit(-1);
	}
    working = false;
}

void Worker::askTask(mr_protocol::AskTaskResponse &res) {
    cl->call(mr_protocol::asktask, id, res);
}

void Worker::doWork()
{
	for (;;) {
        mr_protocol::AskTaskResponse res;
        if (!working) askTask(res);
        switch (res.tasktype) {
            case MAP:
                cout << "Work on " << res.index << " " << res.filename << endl;
                doMap(res.index, res.filename);
                doSubmit(MAP, res.index);
                break;
            case REDUCE:
                cout << "Reduce on" << res.index << " " << endl;
                doReduce(res.index, res.nfiles);
                doSubmit(REDUCE, res.index);
                break;
            case NONE:
                sleep(1);
                break;
        }
	}
}

int main(int argc, char **argv)
{
	if (argc != 3) {
		fprintf(stderr, "Usage: %s <coordinator_listen_port> <intermediate_file_dir> \n", argv[0]);
		exit(1);
	}

	MAPF mf = Map;
	REDUCEF rf = Reduce;
	
	Worker w(argv[1], argv[2], mf, rf);
	w.doWork();

	return 0;
}

