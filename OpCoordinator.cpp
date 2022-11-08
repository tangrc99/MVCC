//
// Created by 唐仁初 on 2022/10/10.
//

#include "OpCoordinator.h"
#include "Value.h"
#include "Operation.h"

Transaction OpCoordinator::startTransaction() {
    return Transaction(updateVersion());
}

BulkWrite OpCoordinator::startBulkWriteOperation(){
    return BulkWrite(updateVersion());
}

ReadOperation OpCoordinator::startReadOperation(Value *node) {

    return ReadOperation(node,*version_);
}

StreamReadOperation OpCoordinator::startStreamReadOperation(Value *node) {
    return StreamReadOperation(node,*version_);
}

WriteOperation OpCoordinator::startWriteOperation(Value *node,const std::string &value) {

    return WriteOperation(node,value,updateVersion());
}

DeleteOperation OpCoordinator::startDeleteOperation(Value *node) {

    return DeleteOperation(node,updateVersion());
}


Version OpCoordinator::updateVersion() {
    auto version = sequence_.fetch_add(1) + 1;
    versions_.emplace_back(version);
    auto v = new Version(version);
    std::swap(v,version_);
    delete v;
    return *version_;
}

bool OpCoordinator::isTransaction(long version) {
    return false;
}
