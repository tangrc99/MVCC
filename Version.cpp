//
// Created by 唐仁初 on 2022/10/10.
//

#include "Version.h"
#include "Value.h"
#include "OpCoordinator.h"

Version::~Version() {

    if (running_default) {
        for (auto &op: operations_) {
            if (op != nullptr)
                op->undo();
        }
    }

    if(use_count_->fetch_sub(1) == 1){
        Coordinator.versionReleaseNotify(version_);
    }
}

void Version::undo() {
    if (refer_)
        return ;

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
