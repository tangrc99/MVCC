//
// Created by 唐仁初 on 2022/10/10.
//

#ifndef ALGYOLO_OPCOORDINATOR_H
#define ALGYOLO_OPCOORDINATOR_H


#include "Version.h"
#include <atomic>
#include <list>


#define MAX_LOCK_TIME 1

class ReadOperation;
class StreamReadOperation;
class WriteOperation;
class DeleteOperation;
class Transaction;
class BulkWrite;
class Value;


/// Class OpCoordinator is the core of MVCC. Every Operation should be generated from this class with assigned Version.
/// The Read and Write Operations will be concurrent.
class OpCoordinator {
public:

    /// Singleton call of OpCoordinator.
    /// \return Singleton impl
    static OpCoordinator &getInstance() {
        static OpCoordinator coordinator;
        return coordinator;
    }

    /// Start a transaction operation. Max version of this impl will update.
    /// \return Transaction impl
    Transaction startTransaction();

    /// Start a bulk write operation. Max version of this impl will update.
    /// \return BulkWriteOperation impl
    BulkWrite startBulkWriteOperation();

    /// Start a read operation on given node. Operation will use latest version.
    /// \param node Node to read
    /// \return ReadOperation impl
    ReadOperation startReadOperation(Value *node);

    /// Start a stream read operation on given node. Operation will use latest version.
    /// \param node Node to start read
    /// \return ReadOperation impl
    StreamReadOperation startStreamReadOperation(Value *node);

    /// Start a write operation on given node. Max version of this impl will update.
    /// \param node Node to write
    /// \param value Value to write
    /// \return WriteOperation impl
    WriteOperation startWriteOperation(Value *node, const std::string &value);

    /// Start a delete operation on given node. Max version of this impl will update.
    /// \param node Node to delete
    /// \return DeleteOperation impl
    DeleteOperation startDeleteOperation(Value *node);

    /// Get lowest alive version. Used by ValueNode to release out of date value record.
    /// \return Lowest alive version
    inline long getLowestVersion() {
        if (versions_.empty())
            return sequence_.load();

        return versions_.front();
    }

    /// Callback used by Version. Release version record in this impl.
    /// \param version Version to release
    void versionReleaseNotify(long version) {
        for (auto v = versions_.begin(); v != versions_.end(); v++) {
            if (*v == version) {
                versions_.erase(v);
                break;
            }
        }
    }

    /// Get current alive version num.
    /// \return Alive version num
    size_t aliveOperationNum() {
        if (version_ == nullptr)
            return 0;

        if (versions_.size() == 1) {  // 如果只有一个版本存活，他还可能会被协调者持有
            return version_->count() - 1;
        }

        return versions_.size();
    }


    bool isTransaction(long version);

private:

    /// Fetch add the current version of this impl. Thread safe.
    /// \return Updated version
    Version updateVersion();

    OpCoordinator() : version_(new Version(0)) {}

    /// Copy constructor.
    /// \param other Other impl
    OpCoordinator(const OpCoordinator &other) : sequence_(other.sequence_.load()), version_(other.version_),
                                                versions_(other.versions_) {

    }

    OpCoordinator &operator=(const OpCoordinator &other) {
        if (this == &other) {
            return *this;
        }
        sequence_ = other.sequence_.load();
        version_ = other.version_;
        versions_ = other.versions_;
        return *this;
    }

private:

    std::list<long> versions_;  // 当前存活的 version
    std::atomic<long> sequence_ = 0;
    Version *version_;
};

#define Coordinator OpCoordinator::getInstance()

#endif //ALGYOLO_OPCOORDINATOR_H
