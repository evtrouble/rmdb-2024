#include "clock_replacer.h"

ClockReplacer::ClockReplacer(size_t num_pages) : shards_(SHARD_COUNT) {
    size_t per_shard = (num_pages + SHARD_COUNT - 1) / SHARD_COUNT;
    for (auto& shard : shards_) {
        shard.entries.resize(per_shard);
    }
}

ClockReplacer::Shard& ClockReplacer::get_shard(frame_id_t frame_id) {
    return shards_[frame_id & (SHARD_COUNT - 1)];
}

bool ClockReplacer::victim(frame_id_t* frame_id) {
    for (auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mtx);
        if (shard.num_entries == 0) continue;

        size_t start = shard.clock_hand;
        do {
            size_t idx = shard.clock_hand % shard.entries.size();
            if (shard.entries[idx].in_replacer) {
                if (shard.entries[idx].ref) {
                    shard.entries[idx].ref = false; // 引用位清零
                } else {
                    // 找到可替换页面
                    *frame_id = idx + (&shard - &shards_[0]) * shard.entries.size();
                    shard.entries[idx].in_replacer = false;
                    shard.num_entries--;
                    shard.clock_hand = (idx + 1) % shard.entries.size();
                    return true;
                }
            }
            shard.clock_hand = (shard.clock_hand + 1) % shard.entries.size();
        } while (shard.clock_hand != start);
    }
    return false;
}

void ClockReplacer::pin(frame_id_t frame_id) {
    auto& shard = get_shard(frame_id);
    std::lock_guard<std::mutex> guard(shard.mtx);
    size_t idx = frame_id / SHARD_COUNT % shard.entries.size();
    if (shard.entries[idx].in_replacer) {
        shard.entries[idx].in_replacer = false;
        shard.num_entries--;
    }
}

void ClockReplacer::unpin(frame_id_t frame_id) {
    auto& shard = get_shard(frame_id);
    std::lock_guard<std::mutex> guard(shard.mtx);
    size_t idx = frame_id / SHARD_COUNT % shard.entries.size();
    if (!shard.entries[idx].in_replacer) {
        shard.entries[idx].in_replacer = true;
        shard.entries[idx].ref = true; // 初始设置引用位为1
        shard.num_entries++;
    }
}

size_t ClockReplacer::Size() {
    size_t total = 0;
    for (auto& shard : shards_) {
        std::lock_guard<std::mutex> guard(shard.mtx);
        total += shard.num_entries;
    }
    return total;
}