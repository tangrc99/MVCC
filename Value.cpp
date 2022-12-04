//
// Created by 唐仁初 on 2022/10/7.
//

#include "Value.h"
#include "OpCoordinator.h"

namespace mvcc{

    ValueNode::ValueNode(std::string value, long version, ValueNode *prev, ValueNode *nxt, ValueNode::Status status) : value_(
            std::move(value)), prev_(prev), nxt_(nxt), version_(version), status_(status) {
    }

    bool ValueNode::commit() {

        if (status_ != ValueNode::Uncommitted)
            return false;

        // 得到当前活跃的最低版本号
        long lowest_version = Coordinator.getLowestVersion();

        auto prev = prev_.load();

        // 只有最旧的事务可以进行移出
        while (prev != nullptr && prev->version_ < lowest_version && prev->status_ != ValueNode::Uncommitted) {
            prev_.store(nullptr);
            auto nxt = prev->prev_.load();
            delete prev;
            prev = nxt;
        }

        status_ = value_.empty() ? ValueNode::Deleted : ValueNode::Committed;

        return true;
    }

    bool ValueNode::undo() {
        status_ = ValueNode::Undo;
        return true;
    }



    Value::~Value() {
        auto cur = latest.load();
        while(cur!= nullptr){
            auto nxt = cur->prev_.load();
            delete cur;
            cur = nxt;
        }
    }

    Value::Value(const std::string &value, long version)
            : latest(new ValueNode(value, version, nullptr, nullptr, ValueNode::Committed)) {
        mem_use_.fetch_add(value.size() + sizeof(long) + sizeof(ValueNode::status_));
    }

    Value::Value(const Value &other) : mtx(){
        auto node = other.latest.load();

        if(node->status_ != ValueNode::Committed){
            throw std::runtime_error("Copy a value with status uncommitted");
        }

        latest = new ValueNode(node->value_,node->version_,nullptr, nullptr,node->status_);
        mem_use_ = other.mem_use_.load();
        mtx.unlock();
    }

    Value &Value::operator=(const Value &other) {
        if (this == &other) {
            return *this;
        }
        auto node = other.latest.load();
        if(node->status_ != ValueNode::Committed){
            throw std::runtime_error("Copy a value with status uncommitted");
        }

        latest = new ValueNode(node->value_,node->version_,nullptr, nullptr,node->status_);
        mem_use_ = other.mem_use_.load();
        mtx.unlock();
        return *this;
    }

    ValueNode *Value::write(const std::string &value, long version, int wait_ms) {

        std::unique_lock<std::timed_mutex> lg(mtx, std::defer_lock);
        auto locked = lg.try_lock_for(std::chrono::milliseconds(wait_ms));

        if (!locked) {
            return {};
        }

        auto node = new ValueNode(value, version, latest, nullptr);

        latest.exchange(node);
        return node;
    }

    ValueNode *Value::remove(long version, int wait_ms) {
        return write("", version, wait_ms);
    }

    std::string Value::read(long version, bool read_latest) {

        auto node = latest.load();

        if (read_latest) {
            // 当前读
            while (node != nullptr) {

                if (node->status_ == ValueNode::Committed)
                    return node->value_;

                if (node->status_ == ValueNode::Deleted)
                    return {};

                node = node->prev_.load();
            }

        } else {
            // 快照读
            while (node != nullptr) {

                if (node->status_ == ValueNode::Committed && node->version_ <= version)
                    return node->value_;

                if (node->status_ == ValueNode::Deleted)
                    return {};

                node = node->prev_.load();
            }
        }

        return {};
    }

    bool Value::getLock(int wait_ms) {
        auto locked = mtx.try_lock_for(std::chrono::milliseconds(wait_ms));
        if (!locked)
            return false;
        in_transaction = true;
        return in_transaction;
    }

    void Value::unlock() {
        if (in_transaction)
            mtx.unlock();
    }

    size_t Value::memoryUse() const {
        return mem_use_.load();
    }

    ValueNode *Value::updateValue(const std::string &value, long version) {

        // 这里要检查有没有被锁
        mtx.try_lock();

        auto node = new ValueNode(value, version, latest, nullptr);

        latest.exchange(node);

        return node;
    }






}


