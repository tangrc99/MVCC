//
// Created by 唐仁初 on 2022/10/10.
//

#include "Version.h"
#include "Value.h"
#include "OpCoordinator.h"
namespace mvcc {

    Version::Version(long version, bool refer) : version_(version), use_count_(new std::atomic<int>(1)),
                                                 refer_(refer) {}

    Version::Version(const Version &other) : version_(other.version_.load()), use_count_(other.use_count_), refer_(other.refer_) {
        use_count_->fetch_add(1);
    }

    Version::~Version() {

        if (running_default) {
            for (auto &op: operations_) {
                if (op != nullptr)
                    op->undo();
            }
        }

        if (use_count_->fetch_sub(1) == 1) {
            Coordinator.versionReleaseNotify(version_);
        }
    }


    Version &Version::operator=(const Version &other) {

        if (this == &other) {
            return *this;
        }

        version_ = other.version_.load();
        use_count_ = other.use_count_;
        operations_ = {};
        use_count_->fetch_add(1);
        return *this;
    }

    long Version::version() const {
        return version_.load();
    }

    void Version::undo() {
        if (refer_)
            return;

        running_default = false;
        for (auto &op: operations_) {
            op->undo();
        }
    }

    bool Version::commit() {
        if (refer_)
            return false;

        running_default = false;
        for (auto &op: operations_) {
            op->commit();
        }
        return true;
    }

    void Version::recordOperation(ValueNode *operated) {
        operations_.emplace_back(operated);
    }

    int Version::count() const {
        return use_count_->load();
    }

    void Version::versionUpdate(int n) {
        version_.fetch_add(n);
    }


}