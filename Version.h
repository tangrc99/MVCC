//
// Created by ๅไปๅ on 2022/10/10.
//

#ifndef ALGYOLO_VERSION_H
#define ALGYOLO_VERSION_H

#include <atomic>
#include <list>


namespace mvcc {

    class ValueNode;

    /// @brief Global transaction version
    /// @details Class Version is a bridge between class OpCoordinator and class Operation. Every Version impl is constructed and
    /// recorded by OpCoordinator. Each Copy of Version impl will increase use count and each destruction will decrease.
    /// When use count is zero, OpCoordinator will release record.\n
    /// Every Operation impl owns a Version impl assigned by OpCoordinator. Operation should record its operation on Value impls.
    /// And when operation ends, use Version::commit or Version::undo to finish.
    class Version {
    public:

        /// Construct a typical version with given version.
        /// \param version Given version sequence
        /// \param refer Is version a refer
        explicit Version(long version, bool refer = false);

        /// Copy constructor. The operation will increase use_count.
        /// \param other Source object
        Version(const Version &other);

        /// Every destruction will decrease use_count. If use count is zero, function will call OpCoordinator::versionReleaseNotify
        ~Version();

        /// Compares the version id of two Version impl to be equal.
        /// \param other Another Version impl
        /// \return Result of comparison
        Version &operator=(const Version &other);

        /// Get the version sequence.
        /// \return Version value
        [[nodiscard]] long version() const;

        /// Commit all operations of this Object impl.
        /// \return Is committed
        bool commit();

        /// Undo all operations of this Object impl.
        void undo();

        /// Record an operation in this object. This function is used to collect all operations and delayed commit together
        /// \param operated Operation to record
        void recordOperation(ValueNode *operated);

        /// Get use count of this impl.
        /// \return Use count
        [[nodiscard]] int count() const;

        /// Atomic update warp version.
        /// \param n Self adding quantity
        void versionUpdate(int n);

    private:

        std::atomic<long> version_ = 0;            // ไบๅกๅท

        std::atomic<int> *use_count_ = nullptr;   // ๅผ็จ่ฎกๆฐ๏ผ็จไบไบๅก่งๅพๆงๅถ

        bool refer_ = false;
        bool running_default = true;    // ๅฆๆ็จๆทๆฒกๆcommitๆundo๏ผๆๆ็ๆถๅไผ่ชๅจ undo
        std::list<ValueNode *> operations_;     // ๅฝๅ็ๆฌๆไฝ่ฟ็ๆๆvalue่็น
    };
}

#endif //ALGYOLO_VERSION_H
