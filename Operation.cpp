//
// Created by ๅไปๅ on 2022/10/11.
//

#include "Operation.h"

namespace mvcc::op {


    WriteOperation::WriteOperation(Value *node, std::string value, const Version &version) : Operation(version),
                                                                                             node_(node),
                                                                                             value_(std::move(value)) {

    }

    bool WriteOperation::write() {
        if (node_ == nullptr)
            return false;

        auto operated = node_->write(value_, version_.version());
        if (!operated)
            return false;

        version_.recordOperation(operated);

        return version_.commit();
    }

    bool WriteOperation::operator<(const WriteOperation &other) {
        return node_ < other.node_;
    }

    bool WriteOperation::operator==(const WriteOperation &other) {
        return node_ == other.node_;
    }

    bool WriteOperation::operator>(const WriteOperation &other) {
        return node_ > other.node_;
    }

    bool WriteOperation::doWithoutCommit() {
        if (node_ == nullptr)
            return false;


        auto operated = ValueNodeOperation::updateValue(node_, value_, version_.version());
        if (!operated)
            return false;

        version_.recordOperation(operated);

        return true;
    }

    bool WriteOperation::commit() {
        return version_.commit();
    }

    void WriteOperation::undo() {
        version_.undo();
    }


    DeleteOperation::DeleteOperation(Value *node, const Version &version) : WriteOperation(node, "", version) {

    }

    ReadOperation::ReadOperation(Value *node, const Version &version) : Operation(version), node_(node) {

    }

    std::string ReadOperation::read() {
        return node_->read(version_.version());
    }

    bool ReadOperation::doWithoutCommit() {
        return true;
    }

    bool ReadOperation::commit() {
        return true;
    }

    void ReadOperation::undo() {

    }

    StreamReadOperation::StreamReadOperation(Value *node, const Version &version) : Operation(version), node_(node) {

    }

    std::string StreamReadOperation::read() {
        return node_->read(version_.version());
    }

    void StreamReadOperation::next(Value *node) {
        node_ = node;
    }

    bool StreamReadOperation::doWithoutCommit() {
        return true;
    }

    bool StreamReadOperation::commit() {
        return true;
    }

    void StreamReadOperation::undo() {

    }

    BulkWriteOperation::BulkWriteOperation(const Version &version) : Operation(version) {}

    void BulkWriteOperation::appendOperation(Value *node, const std::string &value) {
        ops_.emplace_back(WriteOperation(node, value, version_));
    }

    bool BulkWriteOperation::run() {
        std::list<WriteOperation> ops{};    // ็กฎไฟๅช่ฟ่กไธๆฌก
        std::swap(ops, ops_);
        for (auto &op: ops) {
            if (!op.write())
                return false;
        }
        return true;
    }

    bool BulkWriteOperation::doWithoutCommit() {
        return !ops_.empty();
    }

    bool BulkWriteOperation::commit() {
        return run();
    }

    void BulkWriteOperation::undo() {

    }

    Transaction::Transaction(const Version &version) : version_(version) {

    }

    void Transaction::appendOperation(Value *node, const std::string &value) {
        ops_.emplace_back(WriteOperation(node, value, version_));
    }

    bool Transaction::tryCommit() try {

        // ่ฟ้้่ฆๅ่ฟ่กๆๅบ๏ผๆ?นๆฎๆ้ๅฐๅ็ๅคงๅฐ่ฟ่กๆๅบ
        // std::sort(ops_.begin(), ops_.end(),[](WriteOperation &a,WriteOperation &b){return a<b;});

        for (auto &op: ops_) {
            // ๅฏนๆฏไธไธช้่ฆ็จๅฐ็ๅฏน่ฑกๅฐ่ฏ่ฟ่กๅ?้๏ผๅฆๆไธ่ฝๅจ้จ่ทๅพ้๏ผๅๅคฑๅปๆๆ้
            bool locked = op.node_->getLock();
            if (!locked) {
                throw std::runtime_error("LOCK ERROR");
            }
        }

        for (auto &op: ops_) {
            // ่ฟ่กๅๆไฝ
            bool done = op.doWithoutCommit();
            if (!done) {
                throw std::runtime_error("EXEC ERROR");
            }
        }

        for (auto &op: ops_) {
            // ่ฟ่กๆไบคๆไฝ๏ผๅฏ่ฝไผๆไธ้จๅๅผๅจๆไบค่ฟ็จไธญๅฐฑ่ขซ่ฏป
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
}