//
// Created by 唐仁初 on 2022/10/11.
//

#ifndef ALGYOLO_VALUETABLE_H
#define ALGYOLO_VALUETABLE_H

#include "Value.h"
#include "OpCoordinator.h"
#include "Operation.h"
#include "SkipList.h"
#include <unordered_map>

class ValueTable {
private:

    class Iterator {
    public:

        explicit Iterator(const SkipList<Value>::Iterator &it)
                : it_(it), stream_(Coordinator.startStreamReadOperation(&*it)) {

        }

        std::string operator*() {
            return stream_.read();
        }

        Iterator &operator++() {
            ++it_;
            stream_.next(&*it_);
            return *this;
        }

        bool operator==(const Iterator &other) const {
            return it_ == other.it_;
        }

        bool operator!=(const Iterator &other) const {
            return it_ != other.it_;
        }

    private:
        StreamReadOperation stream_;
        SkipList<Value>::Iterator it_;
    };

public:

    ValueTable() = default;

    Iterator begin() {
        return Iterator(skipList_.begin());
    }

    Iterator end() {
        return Iterator(skipList_.end());
    }

    bool bulkWrite(const std::vector<std::pair<std::string, std::string>> &kvs) {

        auto bulk = Coordinator.startBulkWriteOperation();

        for (auto &kv: kvs) {
            // 这里需要进行优化，因为填表内部是排序的，所以经过排序可以一次性搜索出来全部的位置
            auto it = skipList_.find(kv.first);
            if (it == skipList_.end())
                return false;

            Value *value_node = &(*it);
            bulk.appendOperation(value_node, kv.second);

        }
        return bulk.run();
    }

    bool erase(const std::string &key) {
        return update(key, "");
    }

    bool emplace(const std::string &key, const std::string &value) {
        return update(key, value);
    }

    bool update(const std::string &key, const std::string &value) {
        if (key.empty())
            return false;

        Value *value_node = &skipList_[key];

        auto write = Coordinator.startWriteOperation(value_node, value);

        bool write_res = write.write();

        if (write_res) {
            mem_use_.fetch_add(key.size() + value.size());
        }

        return write_res;
    }

    std::string read(const std::string &key) {
        auto it = skipList_.find(key);
        if (it == skipList_.end())
            return {};

        Value *value_node = &(*it);

        auto read = Coordinator.startReadOperation(value_node);

        return read.read();
    }

    bool exist(const std::string &key) {
        return skipList_.find(key) != skipList_.end();
    }

    [[nodiscard]] size_t size() const {
        return skipList_.size();
    }

    [[nodiscard]] size_t memoryUse() const {
        return mem_use_.load();
    }

    void merge(const ValueTable &other){
        skipList_.merge(other.skipList_);
        mem_use_.store(other.mem_use_);
    }

private:
    SkipList<Value> skipList_;  // 这里可以替换为其他线程安全的容器
    std::atomic<size_t> mem_use_ = 0;
};

#endif //ALGYOLO_VALUETABLE_H
