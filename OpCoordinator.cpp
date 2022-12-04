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

        version_->versionUpdate(1);

        return *version_;
    }

    bool OpCoordinator::isTransaction(long version) {
        return false;
    }

    OpCoordinator &OpCoordinator::getInstance() {
        static OpCoordinator coordinator;
        return coordinator;
    }

    long OpCoordinator::getNewestVersion() {
        return sequence_.load();
    }

    long OpCoordinator::getLowestVersion() {

        if (versions_.empty())
            return sequence_.load();

        std::lock_guard<std::mutex> lg(mtx->mtx);
        return *versions_.begin();
    }

    void OpCoordinator::versionReleaseNotify(long version) {

        std::lock_guard<std::mutex> lg(mtx->mtx);
        versions_.erase(version);
    }

    size_t OpCoordinator::aliveOperationNum() {
        if (version_ == nullptr)
            return 0;

        if (versions_.size() == 1) {  // 如果只有一个版本存活，他还可能会被协调者持有
            return version_->count() - 1;
        }

        return versions_.size();
    }

    OpCoordinator::OpCoordinator() : version_(new Version(0)),mtx(new VersionMutex) {}

    OpCoordinator::OpCoordinator(const OpCoordinator &other) : sequence_(other.sequence_.load()), version_(other.version_),
                                                               versions_(other.versions_),mtx(other.mtx) {
    }

    OpCoordinator &OpCoordinator::operator=(const OpCoordinator &other) {
        if (this == &other) {
            return *this;
        }
        sequence_ = other.sequence_.load();
        version_ = other.version_;
        versions_ = other.versions_;
        return *this;
    }
}