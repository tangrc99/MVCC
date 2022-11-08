//
// Created by 唐仁初 on 2022/10/7.
//

#include "Value.h"
#include "OpCoordinator.h"

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
