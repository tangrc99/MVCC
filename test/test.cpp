//
// Created by 唐仁初 on 2022/11/2.
//

#include "../SkipList.h"
#include "../Value.h"
#include "../OpCoordinator.h"
#include "../Operation.h"
#include "../ValueTable.h"

#include <gtest/gtest.h>

using namespace mvcc;

TEST(MVCC_TEST,SKIP_LIST_TEST){
    SkipList<int> skipList;

    skipList.insert("1", 1);
    skipList.insert("2", 2);
    skipList.insert("3", 3);
    skipList.insert("1", 3);
    skipList.insert("1", 6);
    skipList.insert("1", 2);

    auto it = skipList.find("1");
    EXPECT_EQ(*it, 2);


    skipList.insert("2", 1);

    EXPECT_TRUE(skipList.erase(it));
    EXPECT_FALSE(skipList.erase("1"));
    skipList.insert("12", 1);

    EXPECT_EQ(skipList.size(),3);

    EXPECT_ANY_THROW(*skipList.end() = 1);

    auto [start,end] = skipList.findBetween("1","100");

    EXPECT_GE(start.key(),"1");
    EXPECT_LE(end.key(),"100");

    skipList.update("3",5);

    EXPECT_EQ(*skipList.find("3"),5);
}

TEST(MVCC_TEST,VALUE_TEST){
    Value value;

    auto node = value.write("1",1);
    node->commit();
    node = value.write("21",2);

    EXPECT_EQ(value.read(3),"1");
    node->commit();
    EXPECT_EQ(value.read(3),"21");

    node = value.remove(4);
    node->commit();

    EXPECT_EQ(value.read(4),"");
}

TEST(MVCC_TEST,COORDINATOR_TEST){
    auto value = new Value;
    auto wri = Coordinator.startWriteOperation(value,"1");
    wri.write();
    auto rd = Coordinator.startReadOperation(value);
    EXPECT_EQ(rd.read(),"1");

    Coordinator.startWriteOperation(value,"234423");
    Coordinator.startWriteOperation(value,"2323");
    Coordinator.startWriteOperation(value,"2423");
    wri = Coordinator.startWriteOperation(value,"23");


    rd = Coordinator.startReadOperation(value);
    EXPECT_EQ(rd.read(),"1");
    wri.write();
    EXPECT_EQ(rd.read(),"23");

    auto stream = Coordinator.startStreamReadOperation(value);
    stream.next(value);
}

TEST(MVCC_TEST,TRANSATION_TEST){

    Value node1,node2,node3;
    auto transaction = Coordinator.startTransaction();
    transaction.appendOperation(&node1, "1");
    transaction.appendOperation(&node2, "2");
    transaction.tryCommit();

    auto stream = Coordinator.startStreamReadOperation(&node1);
    EXPECT_EQ(stream.read(),"1");
    stream.next(&node2);
    EXPECT_EQ(stream.read(),"2");

    transaction = Coordinator.startTransaction();
    transaction.appendOperation(&node1, "11");
    transaction.appendOperation(&node2, "22");

    EXPECT_TRUE(node1.getLock());
    EXPECT_FALSE(transaction.tryCommit());
    node1.unlock();

    transaction = Coordinator.startTransaction();
    transaction.appendOperation(&node1, "11");
    transaction.appendOperation(&node2, "22");
    EXPECT_TRUE(transaction.tryCommit());

    stream = Coordinator.startStreamReadOperation(&node1);
    EXPECT_EQ(stream.read(),"11");
    stream.next(&node2);
    EXPECT_EQ(stream.read(),"22");
}

TEST(MVCC_TEST,VALUE_TABLE_TEST){
    ValueTable table;

    EXPECT_EQ(table.read("1"),"");
    EXPECT_FALSE(table.exist("1"));
    table.emplace("1","1");
    EXPECT_EQ(table.read("1"),"1");
    table.erase("1");
    EXPECT_FALSE(table.exist("1"));

    table.emplace("2","2");
    table.emplace("2","100");
    table.emplace("33","33");
    table.emplace("22","22");
}

int main(int argc,char *argv[]){
    testing::InitGoogleTest(&argc,argv);

    return RUN_ALL_TESTS();
}