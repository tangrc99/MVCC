//
// Created by 唐仁初 on 2022/10/10.
//

#include "OpCoordinator.h"
#include "Value.h"
#include "Operation.h"

namespace mvcc {

    op::Transaction OpCoordinator::startTransaction() {
        return op::Transaction(updateVersion());
    }

    op::BulkWriteOperation OpCoordinator::startBulkWriteOperation() {
        return op::BulkWriteOperation(updateVersion());
    }

    op::ReadOperation OpCoordinator::startReadOperation(Value *node) {

        return op::ReadOperation(node, *version_);
    }

    op::StreamReadOperation OpCoordinator::startStreamReadOperation(Value *node) {
        return op::StreamReadOperation(node, *version_);
    }

    op::WriteOperation OpCoordinator::startWriteOperation(Value *node, const std::string &value) {

        return op::WriteOperation(node, value, updateVersion());
    }

    op::DeleteOperation OpCoordinator::startDeleteOperation(Value *node) {

        return op::DeleteOperation(node, updateVersion());
    }


    Version OpCoordinator::updateVersion() {
        long version = sequence_.fetch_add(1) + 1;

        mtx->mtx.lock();
        versions_.emplace(version);
        mtx->mtx.unlock();

        auto v = new Version(version);

        auto ex = version_.load();
        while( !version_.compare_exchange_weak(ex,v)){
            ex = version_.load();
        }

        delete ex;

        return *version_;
    }

    bool OpCoordinator::isTransaction(long version) {
        return false;
    }
}