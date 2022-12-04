//
// Created by 唐仁初 on 2022/4/27.
//

#ifndef SKIPLIST_SKIPLIST_H
#define SKIPLIST_SKIPLIST_H

#include <vector>
#include <atomic>
#include <random>
#include <iostream>

namespace mvcc {

    /// @brief A typically SkipList provides concurrent write and read.
    /// @details Class SkipList is a partly thread safe container, whose underlying data structure is skip list.
    /// The value type should has default constructor or copyable.
    /// \tparam V Value type of skip list node
    /// @note The operations on different skip list nodes are thread safe, but the update operations on same node is not thread safe.
    /// User can use a MVCC strategy to make update of value thread safe.
    template<typename V>
    class SkipList {
    private:

        /// Class SkipListNode is the atomic composition in SkipList. The operation on value is not thread safe.
        class SkipListNode {
        public:

            friend class SkipList;

            /// Default Constructor. If type V has default constructor, user can construct a node and given its value later.
            /// \param key The key of this node
            /// \param level The level of this node
            explicit SkipListNode(std::string key, int level): key_(std::move(key)), backward(level) {
            }

            /// If type V is copyable, construct a node with given value
            /// \param key The key of this node
            /// \param value The given value
            /// \param level The level of this node
            explicit SkipListNode(std::string key, V value, int level)  : key_(std::move(key)), value_(value), backward(level) {
            }

            ~SkipListNode() = default;

            /// Set next node of given level. This function will compare and swap, so it's thread safe. If old node is not
            /// the same as expected, function will return false and do nothing.
            /// \param new_node Desired new node
            /// \param old_node Expected old node
            /// \param level
            /// \return Is operation succeeded
            bool setNextNode(SkipListNode *new_node, SkipListNode *old_node, int level){
                return backward[level - 1].compare_exchange_weak(old_node, new_node);
            }


            /// Get next node of given level. This function is thread safe. If given level is larger than this node's
            /// max level, function will return nullptr.
            /// \param level Expected level
            /// \return Next node on given level
            /// @note level starts at 1 rather than 0
            SkipListNode *getNextNode(int level){
                return level > backward.size() ? nullptr : backward[level - 1].load();
            }

            /// Get the max level of this node.
            /// \return Max level
            [[nodiscard]] int level()  const {
                return static_cast<int>(backward.size());
            }

            /// Lazy free this node. Thread safe.
            void markDelete(){
                deleted.store(false);
            }

            /// Check if node is lazy freed. Thread safe.
            /// \return Is node lazy freed
            bool isDeleted() {
                return deleted.load();
            }

        private:
            using SkipListNodePtr = std::atomic<SkipListNode *>;

            std::string key_;
            V value_;

            std::atomic<bool> deleted = false;
            std::vector<SkipListNodePtr> backward;
        };

    public:

        /// @brief Iterator of SkipList
        /// @details class Iterator is used to warp SkipListNode. The CRUD operation will return Iterator to hide underlying data
        /// structure details. User can also use iterator to traverse SkipList on the bottom level thread safely.
        class Iterator {
        public:

            friend class SkipList;

            /// Constructor a iterator points to given node.
            /// \param node Node pointed to
            explicit Iterator(SkipListNode *node): node_(node) {

            }

            /// Iterator::end is a special iterator witch points to nullptr.
            /// \return Iterator::end
            static Iterator end() {
                return Iterator(nullptr);
            }

            /// Get reference of warp value.
            /// \return Refer of type V
            V &operator*() const {
                if (node_ == nullptr)
                    throw std::runtime_error("Using operator * on invalid Iterator");
                return node_->value_;
            }

            ///
            /// \return
            Iterator &operator++() {
                node_ = node_->getNextNode(1);
                return *this;
            }

            Iterator operator++() const {
                auto it = *this;
                node_ = node_->getNextNode(1);
                return it;
            }

            bool operator==(const Iterator &other) const {
                return node_ == other.node_;
            }

            bool operator!=(const Iterator &other) const {
                return node_ != other.node_;
            }
            /// Check validity. If iterator points to nullptr or a lazy freed node, it is invalid.
            /// \return Is iterator valid
            explicit operator bool() const {
                return node_ != nullptr && !node_->isDeleted();
            }

            [[nodiscard]] std::string key()const {
                if (node_ == nullptr)
                    throw std::runtime_error("Request key on invalid Iterator");

                return node_->key_;
            }
        private:
            SkipListNode *node_;
        };

    public:

        /// Construct an empty SkipList with given max level.
        /// \param max_level Max level of SkipList, default 7
        explicit SkipList(int max_level = 7)  : root_(new SkipListNode("", max_level)), MAX_L(max_level) {
            std::random_device rd;
            e = std::default_random_engine(rd());
            root_->markDelete();
        }

        /// Release all nodes, not thread safe.
        ~SkipList(){
            auto cur = root_;
            while (cur != nullptr) {
                auto nxt = cur->getNextNode(1);

                delete cur;
                cur = nxt;
            }
        }

        /// Insert a node without value in SkipList. Thread safe. If key already exists, function will return old node's iterator.
        /// Otherwise, a new node will be construct and return.
        /// \param key The key of node.
        /// \return Iterator of node with given key
        /// @note Thread safe
        Iterator insert(const std::string &key) {

            int level = randomLevel();


            auto start = root_;

            // 从高层到底层，找到应该插入的位置
            for (int i = MAX_L; i > level; i--) {
                auto prev = findPrevByKey(i, key, start);
                start = prev;
            }

            // 检查是否有重复的值
            std::vector<SkipListNode *> prev_nodes(level, nullptr);

            for (int i = level; i > 0; i--) {
                auto prev = findPrevByKey(i, key, start);
                if (prev->key_ == key) {
                    return Iterator(prev);
                }
                prev_nodes[i - 1] = prev;
                start = prev;
            }

            auto node = new SkipListNode(key, level);

            // 在检查的基础上，重新进行搜索，然后将值更替掉
            for (int i = level; i > 0; i--) {

                bool succeed = false;
                while (!succeed) {

                    auto prev = findPrevByKey(i, key, prev_nodes[i - 1]); // 重新寻找，防止有更改发生
                    auto nxt = prev->getNextNode(i);
                    node->setNextNode(nxt, nullptr, i);      // 注意排序，这样修改后不会存在链表中断问题

                    succeed = prev->setNextNode(node, nxt, i);
                }
            }

            size_.fetch_add(1);

            return Iterator(node);
        }

        /// Insert a node with given value. Not thread safe, because this copy of value is not atomic.
        /// If key already exists, function will revise old node's value.
        /// \param key The key of node
        /// \param value The value of node
        /// \return Iterator of node with given key
        /// @note Thread safe
        Iterator insert(const std::string &key, const V &value) {

            int level = randomLevel();

            auto start = root_;

            for (int i = MAX_L; i > level; i--) {
                auto prev = findPrevByKey(i, key, start);
                start = prev;
            }

            // 检查是否有重复的值
            std::vector<SkipListNode *> prev_nodes(level, nullptr);

            for (int i = level; i > 0; i--) {
                auto prev = findPrevByKey(i, key, start);
                if (prev->key_ == key) {
                    prev->value_ = value;
                    return Iterator(prev);
                }
                prev_nodes[i - 1] = prev;
                start = prev;
            }

            auto node = new SkipListNode(key, value, level);

            // 在检查的基础上，重新进行搜索，然后将值更替掉
            for (int i = level; i > 0; i--) {

                bool succeed = false;
                while (!succeed) {

                    auto prev = findPrevByKey(i, key, prev_nodes[i - 1]); // 重新寻找，防止有更改发生
                    auto nxt = prev->getNextNode(i);
                    node->setNextNode(nxt, nullptr, i);      // 注意排序，这样修改后不会存在链表中断问题

                    succeed = prev->setNextNode(node, nxt, i);
                }
            }

            size_.fetch_add(1);

            return Iterator(node);
        }

        /// Thread safe version of insert(key,value). If key already exists, function will returns an
        /// end iterator.
        /// \param key The key of node
        /// \param value The value of node
        /// \return Iterator of node with given key
        /// @note Thread safe
        Iterator insertIfNotExist(const std::string &key, const V &value) {
            int level = randomLevel();

            auto node = new SkipListNode(key, value, level);

            auto start = root_;

            for (int i = MAX_L; i > level; i--) {
                auto prev = findPrevByKey(i, key, start);
                start = prev;
            }

            // 检查是否有重复的值
            std::vector<SkipListNode *> prev_nodes(level, nullptr);

            for (int i = level; i > 0; i--) {
                auto prev = findPrevByKey(i, key, start);
                if (prev->key_ == key) {
                    delete node;
                    return Iterator(nullptr);
                }
                prev_nodes[i - 1] = prev;
                start = prev;
            }

            // 在检查的基础上，重新进行搜索，然后将值更替掉
            for (int i = level; i > 0; i--) {

                bool succeed = false;
                while (!succeed) {

                    auto prev = findPrevByKey(i, key, prev_nodes[i - 1]); // 重新寻找，防止有更改发生
                    auto nxt = prev->getNextNode(i);
                    node->setNextNode(nxt, nullptr, i);      // 注意排序，这样修改后不会存在链表中断问题

                    succeed = prev->setNextNode(node, nxt, i);
                }
            }

            size_.fetch_add(1);

            return Iterator(node);
        }

        /// Find node with given key and return it's iterator. If not found, returns iterator end.
        /// \param key The key of node
        /// \return Iterator of found node
        /// @note Thread safe
        Iterator find(const std::string &key) {

            int i = MAX_L;

            SkipListNode *node = root_;

            while (i > 0) {

                if (node->key_ == key) {
                    return node->isDeleted() ? Iterator(nullptr) : Iterator(node);
                }

                if (node->key_ > key)
                    break;

                auto nxt = node->getNextNode(i);
                if (nxt == nullptr || nxt->key_ > key) {
                    i--;
                } else {
                    node = nxt;
                }
            }

            return Iterator(nullptr);
        }

        /// Find nodes whose key between min and max, [min,max]. Function returns the startup iterator and concluding iterator。
        /// User must check validity of iterator when traversing.
        /// \param min The lower bound of key, default MIN
        /// \param max The upper bound of key, default MAX
        /// \return The begin and end.
        /// @note Thread safe
        std::pair<Iterator, Iterator> findBetween(const std::string &min = "", const std::string &max = "") {
            Iterator start(nullptr), end(nullptr);

            SkipListNode *find_pos = root_;

            if (min.empty()) {
                start = this->begin();
            } else {

                for (int i = MAX_L; i > 0; i--) {
                    find_pos = findPrevByKey(i, min, find_pos);
                }
                if (find_pos->key_ != min) {
                    find_pos = find_pos->getNextNode(1);
                }
                start = Iterator(find_pos);
            }

            if (max.empty()) {
                end = Iterator::end();
            } else {
                for (int i = find_pos->level(); i > 0; i--) {
                    find_pos = findPrevByKey(i, max, find_pos);
                }
                end = Iterator(find_pos);
            }

            return {std::move(start), std::move(end)};
        }

        /// Update value with given key. Not thread safe, because value copy is not atomic. If not found, function will return false.
        /// \param key The key of node
        /// \param value Expected new value of node
        /// \return Is operation succeeded.
        /// @note Thread safe
        bool update(const std::string &key, const V &value) {
            auto node = find(key);
            if (node == Iterator::end())
                return false;
            *node = value;
            return true;
        }

        /// Lazy free a node with given iterator. Thread Safe. If not found, function will return false.
        /// \param iterator Node to lazy free
        /// \return Is operation succeeded
        /// @note Thread safe
        bool erase(const Iterator &iterator) {
            auto node = iterator.node_;
            if (node == nullptr) {
                return false;
            }
            node->deleted.store(true);
            size_.fetch_sub(1);
            return true;
        }

        /// Lazy free a node with given key. Thread Safe. If not found, function will return false.
        /// \param key Node to lazy free
        /// \return Is operation succeeded
        /// @note Thread safe
        bool erase(const std::string &key) {
            return erase(find(key));
        }


        /// Get num of nodes in SkipList.
        /// \return num
        /// @note Thread safe
        size_t size() const {
            return size_.load();
        }

        /// Get the iterator of first element in SkipList.
        /// \return The iterator of first node
        /// @note Thread safe
        Iterator begin() const {
            return Iterator(root_->getNextNode(1));
        }

        /// Get the iterator of end. Iterator::end is a iterator points to null.
        /// \return The iterator of end
        Iterator end() const {
            return Iterator::end();
        }

        /// Get the reference of node's value with given key. If node not exists, a new node will be constructed.
        /// \param key The key of node
        /// \return The reference of value
        /// @note Thread safe
        V &operator[](const std::string &key) {
            return *insert(key);
        }

        /// Get a view from other SkipList. Internal data will be shared but not copied. Thread Safe.
        /// \param other Other SkipList
        /// @warning Advised to be used when read-only
        void getViewFrom(const SkipList<V> &other) {
            clear();
            root_ = other.root_;
            MAX_L = other.MAX_L;
            size_.store(other.size_.load());
        }

        /// Free all nodes in SkipList.
        /// @warning Not thread safe.
        void clear() {
            auto new_root = new SkipListNode("", MAX_L);
            auto cur = root_->getNextNode(1);
            std::swap(root_,new_root);

            delete new_root;

            while (cur != nullptr) {
                auto nxt = cur->getNextNode(1);
                delete cur;
                cur = nxt;
            }
            size_.store(0); // 大小重置
        }

        /// Compact this SkipList and release all deleted nodes. This function is not thread safe.
        /// @warning Not Thread Safe.
        void compact(){

            // 需要遍历每一个层级
            for(int level = MAX_L; level > 0;level--){

                auto slow = root_;
                auto fast = root_->getNextNode(level);

                while (fast != nullptr) {

                    if(fast->isDeleted()){
                        auto nxt = fast->getNextNode(level);
                        slow->setNextNode(nxt,fast,level);  // new,old,level

                        if(level == 1)
                            delete fast;    // 防止重复删除
                        fast = nxt;
                        continue;
                    }

                    auto nxt = fast->getNextNode(level);
                    slow = fast;
                    fast = nxt;
                }

            }

        }

        /// Merge this SkipList and other one. It will use copy rather than view. Make sure that other SkipList will not
        /// be revised. Otherwise, it is unsure that all values are copied.
        /// \param other Other SkipList
        /// @note Thread Safe
        /// @warning Make sure no revise in other SkipList.
        void merge(SkipList<V> &other){

            auto cur = other.root_->getNextNode(1);

            while (cur != nullptr) {
                auto nxt = cur->getNextNode(1);
                this->insert(cur->key_,cur->value_);
                cur = nxt;
            }

        }

        /// Analyze the node nums of each level. This function is only used for test.
        /// \return Vector of node nums of each level.
        std::vector<int> countLevels() {

            std::vector<int> levels(MAX_L, 0);

            for (int i = MAX_L; i > 0; i--) {
                int sum = 0;
                auto cur = root_->getNextNode(i);
                while (cur) {
                    cur = cur->getNextNode(i);
                    sum++;
                }

                levels[i - 1] = sum;
            }
            return levels;
        }

        /// Analyze the random output of SkipList, only used for test.
        /// \param times Generate times
        /// \return Generated random result
        std::vector<int> randomTest(int times) {
            std::vector<int> levels(times, 0);
            for (int i = 0; i < times; i++) {
                levels[i] = randomLevel();
            }
            return levels;
        }

    private:

        /// Internal interface. Find a node whose key is less than or equal to given key. The search will start from
        /// start node in given level. If not found, function will return nullptr.
        /// \param level Level to start search
        /// \param key Key to search
        /// \param start Node to start with
        /// \return Search result.
        SkipListNode *findPrevByKey(int level, const std::string &key, SkipListNode *start) {
            if (start->level() < level)
                return nullptr;

            auto prev = start;
            auto node = start->getNextNode(level);

            while (node != nullptr && node->key_ <= key) {
                prev = prev->getNextNode(level);
                node = node->getNextNode(level);
            }
            return prev;
        }


        /// Internal interface. Generate a random level used to construct new node.
        /// \return Generated level
        int randomLevel() {

            int level = 1;
            while (e() % 2) {
                level++;
            }
            return (level < MAX_L) ? level : MAX_L;
        }

    private:

        std::default_random_engine e;

        SkipListNode *root_;
        int MAX_L;
        std::atomic<size_t> size_ = 0;
    };




}


#endif //SKIPLIST_SKIPLIST_H
