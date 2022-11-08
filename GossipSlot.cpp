//
// Created by 唐仁初 on 2022/9/14.
//

#include "GossipSlot.h"


namespace gossip {

    SlotVersion GossipSlot::insertOrUpdate(const std::string &key, const std::string &value) {

        map_.emplace(key, value);
        version_ = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        return version_;
    }

    SlotVersion GossipSlot::insertOrUpdate(const std::string &key, const std::string &value, SlotVersion version) {
        std::lock_guard<std::mutex> lg(mtx);
        if (version < version_)
            return -1;
        map_.emplace(key, value);
        version_ = version;
        return version_;
    }

    SlotVersion GossipSlot::remove(const std::string &key) {
        std::lock_guard<std::mutex> lg(mtx);

        auto res = map_.erase(key);

        if (!res)
            return -1;

        version_ = std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        return version_;
    }

    SlotVersion GossipSlot::remove(const std::string &key, SlotVersion version) {
        std::lock_guard<std::mutex> lg(mtx);

        if (version < version_)
            return -1;

        auto res = map_.erase(key);

        if (!res)
            return -1;

        version_ = version;
        return version_;
    }

    SlotVersion GossipSlot::compareAndMergeSlot(SlotValues values, SlotVersion version) {
        std::lock_guard<std::mutex> lg(mtx);
        if (version < version_)
            return -1;

        map_.merge(values);
        return version_;
    }

    std::pair<std::string, SlotVersion> GossipSlot::find(const std::string &key) {
        std::lock_guard<std::mutex> lg(mtx);

        auto value = map_.read(key);
        if (value.empty())
            return {"", 0};

        return {value, version_};
    }


}


