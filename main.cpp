//
// Created by 唐仁初 on 2022/10/13.
//

#include "ValueTable.h"
#include "SkipList.h"
#include <iostream>


int skipListTest(){

    SkipList<int> skipList;

    skipList.insert("1",1);
    skipList.insert("2",2);
    skipList.insert("3",3);
    skipList.insert("1",3);
    skipList.insert("1",6);
    skipList.insert("1",2);
    skipList.insert("2",1);
    skipList.insert("12",1);

    auto it = skipList.find("1");


    std::cout << it ;

    return 0;
}




int main() {



    return skipListTest();

    ValueTable table;
    table.emplace("1", "111");

    std::cout << table.read("1") << std::endl;

    table.update("1", "1");

    std::cout << table.read("1") << std::endl;

    table.erase("1");

    std::cout << table.read("1") << std::endl;

    std::cout << table.memoryUse() << std::endl;

    return 0;
}