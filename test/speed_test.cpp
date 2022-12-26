//
// Created by 唐仁初 on 2022/12/4.
//

#include "../SkipList.h"
#include "../Value.h"
#include "../OpCoordinator.h"
#include "../Operation.h"
#include "../ValueTable.h"

#include <gtest/gtest.h>
#include <map>

using namespace mvcc;


TEST(SPEED_TEST,INSERT_TETS){


    size_t size = 1000000;

    std::vector<std::string> keys(size);
    for (int i = 0; i < size; i++) {
        keys[i] = std::to_string(i);
    }

    std::map<std::string,std::string> map;
    auto start = std::chrono::system_clock::now();

    for (int i = 0; i < size; i++) {
        map.emplace( keys[i], "1");
    }

    auto end = std::chrono::system_clock::now();

    std::cout << "Map insert time ms : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << std::endl;


    SkipList<std::string> skip_list(18);
    start = std::chrono::system_clock::now();

    for (int i = 0; i < size; i++) {
        skip_list.insert( keys[i], "1");
    }

    end = std::chrono::system_clock::now();

    std::cout << "SkipList insert time ms : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << std::endl;
}

TEST(SPEED_TEST,MULTI_INSERT_TEST){

    size_t size = 100000;

    std::vector<std::string> keys(size);
    for (int i = 0; i < size; i++) {
        keys[i] = std::to_string(i);
    }

    std::map<std::string,std::string> map;
    std::mutex mtx;

    ValueTable table;

    // 提前准备数据模拟数据表
    for (int i = 0; i < 100000; i++) {
//        std::lock_guard<std::mutex> lg(mtx);
        map["1"] = "1";
        table.emplace(keys[i],"1");
    }
    auto start = std::chrono::system_clock::now();

    // 双线程读取
    std::thread th([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            table.read(keys[i]);
        }
    });
    std::thread th1([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            table.read(keys[i]);
        }
    });
    std::thread th2([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            table.read(keys[i]);
        }
    });
    std::thread th3([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            table.read(keys[i]);
        }
    });
    // 双线程写入
    std::thread th4([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            table.emplace(keys[i],"2");

        }
    });
    std::thread th5([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            table.emplace(keys[i],"2");

        }
    });

    th.join();
    th1.join();
    th2.join();
    th3.join();
    th4.join();
    th5.join();
    auto end = std::chrono::system_clock::now();

    std::cout << "ValueTable time ms : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << std::endl;


    // std::map 测试

    start = std::chrono::system_clock::now();

    // 多线程读取
    th = std::thread([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);
            map[keys[i]];
        }
    });
    th1 = std::thread([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);
            map[keys[i]];
        }
    });
    th2 = std::thread([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);
            map[keys[i]];
        }
    });
    th3 = std::thread([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);
            map[keys[i]];
        }
    });

    // 双线程写入
    th4 = std::thread([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);
            map[keys[i]];
        }
    });
    th5 = std::thread([&table,&map,&mtx,&keys] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);
            map[keys[i]];
        }
    });

    th.join();
    th1.join();
    th2.join();
    th3.join();
    th4.join();
    th5.join();
    end = std::chrono::system_clock::now();

    std::cout << "Map time ms : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << std::endl;
}

TEST(SPEED_TEST,SINGLE_KEY_TEST) {
    ValueTable table;
    std::map<std::string,std::string> map;
    std::mutex mtx;

    auto start = std::chrono::system_clock::now();

    // 双线程读取
    std::thread th([&table,&map,&mtx] {
        for (int i = 0; i < 100000; i++) {
            table.read("1");
        }
    });
    std::thread th1([&table,&map,&mtx] {
        for (int i = 0; i < 100000; i++) {
            table.read("1");
        }
    });
    // 双线程写入
//    std::thread th2([&table,&map,&mtx] {
//        for (int i = 0; i < 100000; i++) {
//            table.emplace("1","1");
//        }
//    });
//    std::thread th3([&table,&map,&mtx] {
//        for (int i = 0; i < 100000; i++) {
//            table.emplace("1","1");
//        }
//    });
    std::thread th4([&table,&map,&mtx] {
        for (int i = 0; i < 100000; i++) {
            table.emplace("1","1");
        }
    });
    std::thread th5([&table,&map,&mtx] {
        for (int i = 0; i < 100000; i++) {
            table.emplace("1","1");
        }
    });
    th.join();
    th1.join();
//    th2.join();
//    th3.join();
    th4.join();
    th5.join();
    auto end = std::chrono::system_clock::now();

    std::cout << "ValueTable time ms : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << std::endl;

    start = std::chrono::system_clock::now();

    // 双线程读取
     th = std::thread([&table,&map,&mtx] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);

            map["1"];
        }
    });
    th1= std::thread([&table,&map,&mtx] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);

            map["1"];
        }
    });
//    th2 = std::thread([&table,&map,&mtx] {
//        for (int i = 0; i < 100000; i++) {
//            std::lock_guard<std::mutex> lg(mtx);
//
//            map["1"] = "1";
//        }
//    });
//    th3= std::thread([&table,&map,&mtx] {
//        for (int i = 0; i < 100000; i++) {
//            std::lock_guard<std::mutex> lg(mtx);
//
//            map["1"] = "1";
//        }
//    });
    th4 = std::thread([&table,&map,&mtx] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);

            map["1"] = "1";
        }
    });
    th5= std::thread([&table,&map,&mtx] {
        for (int i = 0; i < 100000; i++) {
            std::lock_guard<std::mutex> lg(mtx);

            map["1"] = "1";
        }
    });

    th.join();
    th1.join();
//    th2.join();
//    th3.join();
    th4.join();
    th5.join();
    end = std::chrono::system_clock::now();

    std::cout << "Map time ms : " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count()
              << std::endl;
}