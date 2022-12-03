//
// Created by 唐仁初 on 2022/10/10.
//

#ifndef ALGYOLO_OPCOORDINATOR_H
#define ALGYOLO_OPCOORDINATOR_H


#include "Version.h"
#include <atomic>
#include <set>
#include <mutex>


#define MAX_LOCK_TIME 1

namespace mvcc {

    namespace op{
        class ReadOperation;
        class StreamReadOperation;
        class WriteOperation;
        class DeleteOperation;
        class Transaction;
        class BulkWriteOperation;
    }

    class Value;



    /// @brief Global transaction coordinator.
    /// @details Class OpCoordinator is the core of MVCC. Every Operation should be generated from this class with assigned Version.
    /// The Read and Write Operations will be concurrent. And pessimistic concurrency control is adopted between writes.
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
        op::Transaction startTransaction();

        /// Start a bulk write operation. Max version of this impl will update.
        /// \return BulkWriteOperation impl
        op::BulkWriteOperation startBulkWriteOperation();

        /// Start a read operation on given node. Operation will use latest version.
        /// \param node Node to read
        /// \return ReadOperation impl
        op::ReadOperation startReadOperation(Value *node);

        /// Start a stream read operation on given node. Operation will use latest version.
        /// \param node Node to start read
        /// \return ReadOperation impl
        op::StreamReadOperation startStreamReadOperation(Value *node);

        /// Start a write operation on given node. Max version of this impl will update.
        /// \param node Node to write
        /// \param value Value to write
        /// \return WriteOperation impl
        op::WriteOperation startWriteOperation(Value *node, const std::string &value);

        /// Start a delete operation on given node. Max version of this impl will update.
        /// \param node Node to delete
        /// \return DeleteOperation impl
        op::DeleteOperation startDeleteOperation(Value *node);

        /// Get lowest alive version. Used by ValueNode to release out of date value record.
        /// \return Lowest alive version
        inline long getLowestVersion() {

            if (versions_.empty())
                return sequence_.load();

            std::lock_guard<std::mutex> lg(mtx->mtx);
            return *versions_.begin();
        }

        /// Callback used by Version. Release version record in this impl.
        /// \param version Version to release
        void versionReleaseNotify(long version) {

            std::lock_guard<std::mutex> lg(mtx->mtx);
            versions_.erase(version);
        }

        /// Get current alive version num.
        /// \return Alive version num
        size_t aliveOperationNum() {
            if (version_ == nullptr)
                return 0;

            if (versions_.size() == 1) {  // 如果只有一个版本存活，他还可能会被协调者持有
                return version_.load()->count() - 1;
            }

            return versions_.size();
        }

        /// Not Used.
        /// \param version
        /// \return
        /// @warning This interface is not used
        bool isTransaction(long version);

    private:

        /// Fetch add the current version of this impl. Thread safe.
        /// \return Updated version
        Version updateVersion();

        /// Default Constructor. Used to impl singleton.
        OpCoordinator() : version_(new Version(0)),mtx(new VersionMutex) {}

        /// Copy constructor.
        /// \param other Other impl
        OpCoordinator(const OpCoordinator &other) : sequence_(other.sequence_.load()), version_(other.version_.load()),
                                                    versions_(other.versions_),mtx(other.mtx) {
        }

        /// Copy Operator. Used to impl singleton.
        /// \param other Other OpCoordinator impl
        /// \return Copied impl
        OpCoordinator &operator=(const OpCoordinator &other) {
            if (this == &other) {
                return *this;
            }
            sequence_ = other.sequence_.load();
            version_ = other.version_.load();
            versions_ = other.versions_;
            return *this;
        }

    private:

        struct VersionMutex{
            std::mutex mtx;
        };

        VersionMutex* mtx;  // 锁解决并发分配事务号
        std::set<long> versions_;  // 当前存活的 version
        std::atomic<long> sequence_ = 0;
        std::atomic<Version *> version_;
    };
}
#define Coordinator OpCoordinator::getInstance()

#endif //ALGYOLO_OPCOORDINATOR_H
