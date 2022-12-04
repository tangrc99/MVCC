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
#include <thread>


namespace mvcc {

    /// @brief ValueTable is a read-write concurrent table.
    /// @details ValueTable provides a container to user. The operations of this container is thread safe. The read strategy is read
    /// latest. Write operation on same node is not allowed.
    class ValueTable {
    public:

        /// @brief Iterator provides a snapshot-read interface to ValueTable.
        /// @details Iterator warps a StreamReadOperation to provide a snapshot-read. Iterator only uses SkipList's bottom
        /// level to implement traversal.
        class Iterator {
        public:

            /// Constructs an Iterator impl with given SkipList<Value>::Iterator.
            /// \param it SkipList<Value>::Iterator to warp
            explicit Iterator(const SkipList<Value>::Iterator &it)
                    : it_(it), stream_(Coordinator.startStreamReadOperation(&*it)) {

            }

            /// Read the value of current position.
            /// \return
            std::string operator*() {
                return stream_.read();
            }

            /// Change this StreamReadOperation position to next node.
            /// \return Changed impl.
            Iterator &operator++() {
                ++it_;
                stream_.next(&*it_);
                return *this;
            }

            /// Compares the contents of the current two iterators to be equal.
            /// \param other Another Iterator impl
            /// \return Results of comparison
            bool operator==(const Iterator &other) const {
                return it_ == other.it_;
            }

            /// Compares the contents of the current two iterators to be not equal.
            /// \param other Another Iterator impl
            /// \return Results of comparison
            bool operator!=(const Iterator &other) const {
                return it_ != other.it_;
            }

        private:
            op::StreamReadOperation stream_;
            SkipList<Value>::Iterator it_;
        };

    public:

        /// Describes garbage cleanup level
        enum CleanThreshold {
            /// Cleanup starts at 50%
            high,
            /// Cleanup starts at 30%
            medium,
            /// Cleanup starts at 15%
            low,
            /// Cleanup never starts
            never
        };


        /// Constructs a ValueTable impl using given skip list level.
        /// \param max_level skip list's max level
        /// \param threshold Garbage cleanup threshold. If it is set to 1, no cleanup is performed
        explicit ValueTable(int max_level = 18, CleanThreshold threshold = never) : skipList_(max_level),
                                                                                    buffer_(max_level),
                                                                                    deleted_nums(0),
                                                                                    threshold_(threshold),
                                                                                    status_(0) {
            if (threshold == high) {
                percent = 0.5;
            } else if (threshold == medium) {
                percent = 0.3;
            } else if (threshold == low) {
                percent = 0.15;
            } else {
                percent = 1;
            }
        };

        ~ValueTable() = default;

        /// Get the begin of all values.
        /// \return Iterator of first value
        Iterator begin() {
            return Iterator(skipList_.begin());
        }

        /// Get the end of all values. End means an iterator points to nullptr.
        /// \return Iterator of end
        Iterator end() {
            return Iterator(skipList_.end());
        }

        /// Start a transaction. If there is any error while processing, all operations will roll back.
        /// \param kvs vector of key-value pair to write
        /// \return Is transaction suceeded
        bool transaction(const std::vector<std::pair<std::string, std::string>> &kvs);

        /// Start a bulk write. Operation will pause after error occurs.
        /// \param kvs vector of key-value pair to write
        /// \return Is all operation finished
        bool bulkWrite(const std::vector<std::pair<std::string, std::string>> &kvs);

        /// Erase a record with given key.
        /// \param key The key of record
        /// \return Is ok
        bool erase(const std::string &key) {
            deleted_nums.fetch_add(1);  // 增加删除计数
            return skipList_.erase(key);
        }

        /// Emplace a record with given key and value.
        /// \param key The key of record
        /// \param value The value of record
        /// \return Is ok
        bool emplace(const std::string &key, const std::string &value);

        /// Update a record with given key and value.
        /// \param key The key of record
        /// \param value The value of record
        /// \return Is ok
        bool update(const std::string &key, const std::string &value);

        /// Read a record with given key. If not exists, returns "" means empty.
        /// \param key The key of record
        /// \return value or ""
        std::string read(const std::string &key);

        /// Check is record with given key exists.
        /// \param key The key of record
        /// \return Is record exists
        bool exist(const std::string &key) {
            return skipList_.find(key) != skipList_.end();
        }


        /// Find a record with given key and get it's iterator. This function is used to traverse table.
        /// \param key The key of record
        /// \return Iterator of record.
        Iterator find(const std::string &key) {
            auto pos = skipList_.find(key);
            if (pos == skipList_.end()){
                if (status_.load() == 0)
                    return end();
                pos = buffer_.find(key);
                if (pos == buffer_.end()) {
                    return end();
                }
            }
            return Iterator(pos);
        }


        /// The num of records, typically key nums in this table.
        /// \return The num of records
        [[nodiscard]] size_t size() const {
            return skipList_.size();
        }

        /// Not used.
        /// \return
        [[nodiscard]] size_t memoryUse() const {
            return mem_use_.load();
        }


        /// Force start compact process. Make sure that no operation is now on this table. A daemon thread will be start
        /// to do compact task.
        /// @note Costly action.
        void compact();


    private:
        void clearBuffer();

        void tryCompact();

    private:

        std::atomic<int> status_;   // 1 : compact,写缓冲区 2 : clean,写主表

        SkipList<Value> skipList_;  //  内存表区域
        SkipList<Value> buffer_;    // 备用插入缓冲区

        std::atomic<size_t> mem_use_ = 0;   // 内存占用估算

        CleanThreshold threshold_;  // 清理阈值
        double percent; // 具体百分比

        std::atomic<size_t> deleted_nums; // 删除操作的个数

        std::thread th_;
    };
}
#endif //ALGYOLO_VALUETABLE_H
