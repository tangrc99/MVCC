//
// Created by 唐仁初 on 2022/10/11.
//

#include "ValueTable.h"

namespace mvcc {

    bool ValueTable::transaction(const std::vector<std::pair<std::string, std::string>> &kvs) {
        auto transaction = Coordinator.startTransaction();
        for (auto &kv: kvs) {
            auto it = skipList_.find(kv.first);
            if (it == skipList_.end())
                return false;
            Value *value_node = &(*it);
            transaction.appendOperation(value_node, kv.second);
        }
        return transaction.tryCommit();
    }

    bool ValueTable::bulkWrite(const std::vector<std::pair<std::string, std::string>> &kvs) {

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

    bool ValueTable::emplace(const std::string &key, const std::string &value) {
        return update(key, value);
    }

    bool ValueTable::update(const std::string &key, const std::string &value) {
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

    std::string ValueTable::read(const std::string &key) {
        auto it = skipList_.find(key);
        if (it == skipList_.end())
            return {};

        Value *value_node = &(*it);

        auto read = Coordinator.startReadOperation(value_node);

        return read.read();
    }



}