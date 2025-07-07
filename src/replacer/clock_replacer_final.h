#pragma once

#include <vector>
#include <mutex>
#include <atomic>
#include "common/config.h"
#include "replacer/replacer.h"

class ClockReplacer_Final : public Replacer
{
public:
    explicit ClockReplacer_Final(size_t num_pages);
    ~ClockReplacer_Final() = default;

    bool victim(frame_id_t *frame_id) override;
    void pin(frame_id_t frame_id) override;
    void unpin(frame_id_t frame_id) override;
    size_t Size() override;

private:
    struct ClockEntry
    {
        bool in_replacer{false}; // 是否在可替换队列中
        bool ref{false};         // 引用位
    };

    struct Shard
    {
        std::mutex mtx;                  // 分片锁
        std::vector<ClockEntry> entries; // 页面状态数组
        size_t clock_hand{0};            // 时钟指针
        size_t num_entries{0};           // 当前可替换的页面数
    };

    static constexpr int SHARD_COUNT = 16; // 分片数，减少锁竞争
    std::vector<Shard> shards_;            // 分片数组

    inline Shard &get_shard(frame_id_t frame_id);
};