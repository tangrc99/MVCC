//
// Created by 唐仁初 on 2022/10/11.
//

#ifndef ALGYOLO_OPERATION_H
#define ALGYOLO_OPERATION_H


#include "Value.h"
#include "Version.h"
#include <utility>


class Transaction;


/// Operation is an abstract class of sequenced operation. Each operation class will put revised ValueNode in
/// assigned Version and commit or undo when operation finishes.
class Operation {
protected:

    friend class Transaction;

    /// Construct an Operation impl with assigned Version
    /// \param version Assigned version
    explicit Operation(const Version &version) : version_(version) {}

    /// Transaction interface. Do operation without commit automatically.
    /// \return Is operation succeeded
    virtual bool doWithoutCommit() = 0;

    /// Transaction interface. Commit operation.
    /// \return Is committed
    virtual bool commit() = 0;

    /// Transaction interface. Undo operation.
    virtual void undo() = 0;

    virtual ~Operation() = default;

protected:

    Version version_;
};


/// Class WriteOperation is an abstract of write process. User can use write() function to start write process.
/// Transaction and use derived interface to control transaction process.
class WriteOperation : public Operation {
public:

    friend class Transaction;

    /// Construct a WriteOperation impl.
    /// \param node Node to write
    /// \param value Value to write
    /// \param version Assigned Version
    explicit WriteOperation(Value *node, std::string value, const Version &version) : Operation(version), node_(node),
                                                                                      value_(std::move(value)) {

    }

    ~WriteOperation() override = default;

    /// Start write process.
    /// \return Is operation succeeded
    bool write() {
        if (node_ == nullptr)
            return false;

        auto operated = node_->write(value_, version_.version());
        if (!operated)
            return false;

        version_.recordOperation(operated);

        return version_.commit();
    }

    bool operator<(const WriteOperation &other) {
        return node_ < other.node_;
    }

    bool operator==(const WriteOperation &other) {
        return node_ == other.node_;
    }

    bool operator>(const WriteOperation &other) {
        return node_ > other.node_;
    }

private:

    /// Transaction interface. Do operation without commit automatically.
    /// \return Is operation succeeded
    bool doWithoutCommit() override {
        if (node_ == nullptr)
            return false;


        auto operated = ValueNodeOperation::updateValue(node_,value_, version_.version());
        if (!operated)
            return false;

        version_.recordOperation(operated);

        return true;
    }

    bool commit() override {
        return version_.commit();
    }

    void undo() override {
        version_.undo();
    }

private:
    Value *node_ = nullptr;
    std::string value_;
    bool acquire_newest = false;    //FIXME : 如果用户要求插入的是最新值
};

class DeleteOperation final : public WriteOperation {
public:

    explicit DeleteOperation(Value *node, const Version &version) : WriteOperation(node, "", version) {

    }

    ~DeleteOperation() override = default;

};

class ReadOperation final : public Operation {
public:

    explicit ReadOperation(Value *node, const Version &version) : Operation(version), node_(node) {

    }

    ~ReadOperation() override = default;

    /// Start read process and get value.
    /// \return Expected value
    std::string read() {
        return node_->read(version_.version());
    }

private:

    /// Transaction interface. No use.
    /// \return Is operation succeeded
    bool doWithoutCommit() override {
        return true;
    }

    bool commit() override {
        return true;
    }

    void undo() override {

    }

private:
    Value *node_ = nullptr;     // FIXME : 目前实现的是快照读，而不是当前读
};


class StreamReadOperation final : public Operation {

public:

    explicit StreamReadOperation(Value *node, const Version &version) : Operation(version), node_(node) {

    }

    ~StreamReadOperation() override = default;

    /// Start read process and get value.
    /// \return Expected value
    std::string read() {
        return node_->read(version_.version());
    }

    /// Change read node to next.
    /// \param node Node to move to
    void next(Value *node) {
        node_ = node;
    }

private:

    /// Transaction interface. No use.
    /// \return Is operation succeeded
    bool doWithoutCommit() override {
        return true;
    }

    bool commit() override {
        return true;
    }

    void undo() override {

    }

private:
    Value *node_ = nullptr;
};

using UpdateOperation = WriteOperation;


class BulkWrite final : public Operation {
public:

    explicit BulkWrite(const Version &version) : Operation(version) {}

    ~BulkWrite() override = default;

    /// Append write operation to this operation stream.
    /// \param node Node to write
    /// \param value Value to write
    void appendOperation(Value *node, const std::string &value) {
        ops_.emplace_back(WriteOperation(node, value, version_));
    }

    /// Execute bulk write operation. If one write operation is failed, bulk write operation will stop without undo.
    /// \return Is all write operation succeeded
    bool run() {
        std::list<WriteOperation> ops{};    // 确保只运行一次
        std::swap(ops, ops_);
        for (auto &op: ops) {
            if (!op.write())
                return false;
        }
        return true;
    }

private:

    /// Transaction interface. No use.
    /// \return Is operation succeeded
    bool doWithoutCommit() override {
        return !ops_.empty();
    };

    bool commit() override {
        return run();
    };

    void undo() override {

    }

    std::list<WriteOperation> ops_;
};


class Transaction {
public:
    explicit Transaction(const Version &version) : version_(version) {

    }

    ~Transaction() = default;

    void appendWrite(Value *node, const std::string &value) {
        ops_.emplace_back(WriteOperation(node, value, version_));
    }

    bool tryCommit() try {

        // 这里需要先进行排序，根据指针地址的大小进行排序
        // std::sort(ops_.begin(), ops_.end(),[](WriteOperation &a,WriteOperation &b){return a<b;});

        for (auto &op: ops_) {
            // 对每一个需要用到的对象尝试进行加锁，如果不能全部获得锁，则失去所有锁
            bool locked = op.node_->getLock();
            if (!locked) {
                throw std::runtime_error("LOCK ERROR");
            }
        }

        for (auto &op: ops_) {
            // 进行写操作
            bool done = op.doWithoutCommit();
            if (!done) {
                throw std::runtime_error("EXEC ERROR");
            }
        }

        for (auto &op: ops_) {
            // 进行提交操作，可能会有一部分值在提交过程中就被读
            if (!op.commit())
                throw std::runtime_error("COMMIT ERROR");
        }


        for (auto &op: ops_) {
            op.node_->unlock();
        }

        ops_ = {};
        return true;

    } catch (std::exception &e) {

        for (auto &op: ops_) {
            op.undo();
            op.node_->unlock();
        }

        return false;
    }


private:
    Version version_;
    std::list<WriteOperation> ops_;
};


#endif //ALGYOLO_OPERATION_H
