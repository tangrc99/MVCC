//
// Created by 唐仁初 on 2022/10/11.
//

#include "ValueTable.h"

namespace mvcc {

    bool ValueTable::transaction(const std::vector<std::pair<std::string, std::string>> &kvs) {
        auto transaction = Coordinator.startTransaction();

        // 如果正在压缩则写缓冲区
        if (status_ == 1) {
            for (auto &kv: kvs) {
                Value *value_node = &buffer_[kv.first];
                transaction.appendOperation(value_node, kv.second);
            }
            return transaction.tryCommit();
        }

        for (auto &kv: kvs) {
            Value *value_node = &skipList_[kv.first];
            transaction.appendOperation(value_node, kv.second);
        }

        tryCompact();

        return transaction.tryCommit();
    }

    bool ValueTable::bulkWrite(const std::vector<std::pair<std::string, std::string>> &kvs) {

        auto bulk = Coordinator.startBulkWriteOperation();

        // 如果正在压缩则写缓冲区
        if (status_ == 1) {
            for (auto &kv: kvs) {
                Value *value_node = &buffer_[kv.first];
                bulk.appendOperation(value_node, kv.second);
            }
            return bulk.run();
        }
        for (auto &kv: kvs) {
            Value *value_node = &skipList_[kv.first];
            bulk.appendOperation(value_node, kv.second);
        }

        tryCompact();

        return bulk.run();
    }

    bool ValueTable::emplace(const std::string &key, const std::string &value) {
        return update(key, value);
    }

    bool ValueTable::update(const std::string &key, const std::string &value) {
        if (key.empty())
            return false;

        // 如果正在开启清理，则写入缓冲区中
        if (status_.load() == 1) {
            Value *value_node = &buffer_[key];

            auto write = Coordinator.startWriteOperation(value_node, value);

            bool write_res = write.write();

            if (write_res) {
                mem_use_.fetch_add(key.size() + value.size());
            }
            return write_res;
        }


        Value *value_node = &skipList_[key];

        auto write = Coordinator.startWriteOperation(value_node, value);

        bool write_res = write.write();

        if (write_res) {
            mem_use_.fetch_add(key.size() + value.size());
        }

        tryCompact();   // 写操作时会进行清理检查

        return write_res;
    }

    std::string ValueTable::read(const std::string &key) {
        auto it = skipList_.find(key);
        if (it == skipList_.end()) {
            // 缓冲区无内容，直接返回
            if (status_.load() == 0)
                return {};

            // 如果缓冲区有内容，还需要查找缓冲区
            it = buffer_.find(key);
            if (it == buffer_.end()) {
                return {};
            }
        }

        Value *value_node = &(*it);

        auto read = Coordinator.startReadOperation(value_node);

        return read.read();
    }

    void ValueTable::tryCompact() {

        if(threshold_ == never)
            return ;

        auto hl = static_cast<double >(skipList_.size()) * percent;
        if(hl < static_cast<double >(deleted_nums))
            compact();
    }

    void ValueTable::clearBuffer() {
        std::thread th([this] {

            status_.store(2);   // 当前事务都重新写入到表中

            // 获取最新事务号，等待所有写入事务执行完毕，确保buffer不会更新
            auto newest = Coordinator.getNewestVersion();
            while (newest > Coordinator.getLowestVersion()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            // 等待当前事务提交完毕后，融合缓冲区
            skipList_.merge(buffer_);

            // 所有数据已经放入主表，不需要再进行读取
            status_.store(0);

            // 确保所有的读事务都已经结束，然后释放缓冲区
            newest = Coordinator.getNewestVersion();
            while (newest > Coordinator.getLowestVersion()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            buffer_.clear();
        });
        th.detach();
    }

    void ValueTable::compact() {

        if (status_.load() != 0)
            return;

        // 确保缓冲区已经被清空了
        if (buffer_.size() != 0) {
            return;
        }

        // 判断是否无活跃事务
        if (Coordinator.aliveOperationNum() != 0) {
            return;
        }

        // 锁住表
        int ex = 0;
        int dt = 1;
        // 只能有一个线程锁住表

        if (!status_.compare_exchange_weak(ex, dt))
            return;

        // 判断是否无活跃事务，若无则开始压缩表
        if (Coordinator.aliveOperationNum() != 0) {

            // 合并缓冲区内容
            clearBuffer();
            return;
        }


        // 先压缩表，然后开始合并缓冲区内容
        std::thread th([this] {

            try{
                // 获取表中已删除的个数（不太准确）
                size_t deleted = deleted_nums.load();

                // 安全清理所有删除掉的节点
                skipList_.compact();

                status_.store(2);   // 当前事务都重新写入到表中

                // 获取最新事务号，等待所有写入事务执行完毕，确保buffer不会更新
                auto newest = Coordinator.getNewestVersion();
                while (newest > Coordinator.getLowestVersion()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }

                // 等待当前事务提交完毕后，融合缓冲区
                skipList_.merge(buffer_);

                // 更新已删除计数
                deleted_nums.fetch_sub(deleted);

                // 所有数据已经放入主表，不需要再进行读取
                status_.store(0);

                // 确保所有的读事务都已经结束，然后释放缓冲区
                newest = Coordinator.getNewestVersion();
                while (newest > Coordinator.getLowestVersion()) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }

                buffer_.clear();


            } catch (...) {

            }


        });

        th.join();
    }


}