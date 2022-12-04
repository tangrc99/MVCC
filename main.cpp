//
// Created by 唐仁初 on 2022/10/13.
//

#include "ValueTable.h"
#include "SkipList.h"
#include <iostream>
#include <map>
#include <thread>


using namespace mvcc;
using namespace std;


int ValueConcurrentInsertTest() {
    std::atomic<int> version = 0;
    Value value;

    auto start = std::chrono::system_clock::now();

    std::thread th([&version, &value] {
        for (int i = 0; i < 100000; i++) {
            auto v = version.fetch_add(1);
            value.write("1", v);
        }
    });

    for (int i = 0; i < 100000; i++) {
        auto v = version.fetch_add(1);
        value.write("1", v);
    }

    th.join();
    auto end = std::chrono::system_clock::now();

    std::cout << "Insert time ms : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << std::endl;
    return 0;
}


int singleInsertTest() {

    ValueTable value;
    //Value value;

    auto start = std::chrono::system_clock::now();


    std::thread th([&value] {
        for (int i = 0; i < 100000; i++) {
            value.emplace("1", "1");
        }
    });

    for (int i = 0; i < 100000; i++) {
        value.emplace("1", "1");
    }

    th.join();
    auto end = std::chrono::system_clock::now();

    std::cout << "Insert time ms : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << std::endl;

    return 0;
}

void ops() {

    ValueTable table;
    // 插入一个键值对
    table.emplace("key", "value");
    // 查找键值对是否存在
    table.exist("key");
    // 读取键值对
    table.read("key");
    // 修改键值对
    table.update("key", "new_value");
    //删除键值对
    table.erase("key");


    for(auto it = table.begin();it != table.end();++it){
        auto value = *it;
    }


    for(auto it = table.find("start");it != table.end();++it){
        auto value = *it;
    }


    for(auto it = table.find("start"),end = table.find("end");it != end;++it){
        auto value = *it;
    }

    std::vector<std::pair<std::string, std::string>> kvs = {{"k1","v1"},{"k2","v2"}};
    bool finished = table.bulkWrite(kvs);


    bool committed = table.transaction(kvs);

}


int garbageCleanTest(){
    ValueTable table(3);

    table.emplace("1","1");
    table.emplace("2","2");

    table.erase("2");

    table.compact();

    //table.emplace("3","1");


    //std::cout << table.size();


    this_thread::sleep_for(std::chrono::seconds(2));


    return 0;
}


int main() {


    return garbageCleanTest();

    std::vector<std::string> keys(100000);
    for (int i = 0; i < 100000; i++) {
        keys[i] = std::to_string(i);
    }

    ValueTable table;
    mvcc::SkipList<Value> skipList(18);
    std::map<std::string, mvcc::Value *> map;

    std::mutex mtx;

    auto start = std::chrono::system_clock::now();

//    std::thread th([&skipList,&keys,&mtx,&map](){
//        for(int i = 0;i < 50000;i++){
////            mtx.lock();
////            map.emplace(keys[i],"1");
//            skipList.insert(keys[i],"1");
//      //      mtx.unlock();
//        }
//    });

    for (int i = 0; i < 100000; i++) {
        //   mtx.lock();

        // map.emplace(keys[i],new Value("1",0));
      //  skipList.insert(keys[i]);
        // mtx.unlock();
          table.emplace(keys[i],"1");
    }

    auto end = std::chrono::system_clock::now();
    // th.join();

    std::cout << "Insert time ms : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << std::endl;


    return 0;
}