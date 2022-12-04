//
// Created by 唐仁初 on 2022/10/7.
//

#ifndef ALGYOLO_VALUE_H
#define ALGYOLO_VALUE_H


#include <atomic>
#include <mutex>


namespace mvcc {

    namespace op {

        class Operation;

        class Transaction;

        class WriteOperation;
    }


    /// @brief A atomic linked list node with version control.
    /// @details ValueNode is a typically linked list node. It's previous and next ptr are atomic. Every ValueNode owns
    /// a version to provide different view.
    class ValueNode {
    public:

        friend class Value;

        /// The status of value
        enum Status {
            /// Operation is committed.
            Committed,
            /// Value is deleted.
            Deleted,
            /// Operation is uncommitted.
            Uncommitted,
            /// Operation rollback.
            Undo
        };

        /// Constructor of ValueNode.
        /// \param value The value of this node
        /// \param version The version of this node
        /// \param prev The previous node of this node
        /// \param nxt The next node of this node
        /// \param status The status of this node
        explicit ValueNode(std::string value, long version, ValueNode *prev = nullptr, ValueNode *nxt = nullptr,
                           Status status = Uncommitted);

        ~ValueNode() = default;

        /// Commit the value revision. Thread safe.
        /// \return Is operation OK
        bool commit();

        /// Discard the value revision and undo. Thread safe.
        /// \return Is operation OK
        bool undo();

    private:

        long version_;
        std::string value_;
        Status status_;
        std::atomic<ValueNode *> prev_, nxt_;
    };


    /// @brief Provide a unified view of a ValueNode list.
    /// @details Class Value maintains a ValueNode list with different version to achieve MVCC. The nodes are sorted by its version.
    /// Only nodes with committed value are visible to users.
    class Value {
    public:

        friend class op::Transaction;

        /// Default Construction. Construct an empty value.
        Value() = default;

        /// Destruction to release all ValueNode in this row.
        ~Value();

        /// Construct a Value impl with assigned value and latest version.
        /// \param value Latest value
        /// \param version Latest version
        explicit Value(const std::string &value, long version);

        /// Copy Constructor. Only copy latest value of other impl.
        /// \param other Other Value impl
        /// @warning This succeeds only if all operations on the value are committed, otherwise an exception will be thrown.
        /// @throw std::runtime_error("Copy a value with status uncommitted")
        Value(const Value &other);

        /// Copy Operator. Only copy latest value of other impl.
        /// \param other Other Value impl
        /// @warning This succeeds only if all operations on the value are committed, otherwise an exception will be thrown.
        /// @throw std::runtime_error("Copy a value with status uncommitted")
        Value &operator=(const Value &other);


        /// Write operation. Decorated interface of updateValue.
        /// This operation will insert a new uncommitted ValueNode with assigned value, and returns its value.
        /// Operation will wait for given time to get mutex, if failed, function will return nullptr.
        /// \param value Value to write
        /// \param version Operation version
        /// \param wait_ms Max wait time
        /// \return Ptr of operated ValueNode.
        ValueNode *write(const std::string &value, long version, int wait_ms = 50);

        /// Lazy free this node. This operation will insert a ValueNode with empty value.Operation will wait for given time
        /// to get mutex, if failed, function will return nullptr.
        /// \param version Operation version
        /// \param wait_ms Max wait time
        /// \return Ptr of operated ValueNode.
        ValueNode *remove(long version, int wait_ms = 50);

        /// Read operation. Get the value older than given version(default) or get latest version.
        /// \param version Operation version
        /// \param read_latest Read type
        /// \return Read value
        std::string read(long version, bool read_latest = false);

        /// Lock this value. Only used in transaction operations.Operation will wait for given time to get mutex.
        /// \param wait_ms Max wait time
        /// \return Is operation succeeded
        bool getLock(int wait_ms = 50);

        /// Release the lock.
        void unlock();

        /// Get approximately memory use of this node.
        /// \return Memory use
        size_t memoryUse() const;

    private:

        friend class ValueNodeOperation;

        /// Write operation. Only used by transaction.
        /// \param value Value to write
        /// \param version Operation version
        /// \return Ptr of operated ValueNode.
        ValueNode *updateValue(const std::string &value, long version);

    private:
        std::timed_mutex mtx;
        std::atomic<ValueNode *> latest = nullptr;
        std::atomic<size_t> mem_use_ = 0;

        bool in_transaction = false;    // 用于区分节点是被单个写操作锁住，还是事务锁住
    };



    /// @brief To expose some private functions of Value.
    /// @details Class ValueNodeOperation is used to partly expose private interface of ValueNode.
    /// ValueNodeOperation is friend class of ValueNode. Private interface to expose is decorated in this class.
    /// And Other classes that need these interfaces are friend class of ValueNodeOperation.
    class ValueNodeOperation {
    private:

        friend class op::WriteOperation;

        /// Decorate interface of ValueNode::updateValue.
        /// \param node ValueNode to run
        /// \param value Value to write
        /// \param version Operation version
        /// \return Ptr of operated ValueNode
        static ValueNode *updateValue(Value *node, const std::string &value, long version) {
            return node->updateValue(value, version);
        }

    };
}
#endif //ALGYOLO_VALUE_H
