#include "lru_replacer.h"
#include <iostream>

LRUReplacer::LRUReplacer(size_t num_pages) : shards_(SHARD_COUNT) {}

bool LRUReplacer::victim(frame_id_t *frame_id)
{
    for (auto &shard : shards_)
    {
        std::lock_guard guard(shard.mtx);
        if (!shard.lru_list.empty())
        {
            *frame_id = shard.lru_list.back();
            shard.lru_list.pop_back();
            shard.lru_map.erase(*frame_id);
            return true;
        }
    }
    return false;
}

void LRUReplacer::pin(frame_id_t frame_id)
{
    auto &shard = get_shard(frame_id);
    std::lock_guard guard(shard.mtx);
    auto iter = shard.lru_map.find(frame_id);
    if (iter != shard.lru_map.end())
    {
        shard.lru_list.erase(iter->second);
        shard.lru_map.erase(iter);
    }
}

void LRUReplacer::unpin(frame_id_t frame_id)
{
    auto &shard = get_shard(frame_id);
    std::lock_guard guard(shard.mtx);
    auto iter = shard.lru_map.find(frame_id);
    if (iter != shard.lru_map.end())
    {
        // 如果已存在，先从列表中删除
        shard.lru_list.erase(iter->second);
        shard.lru_map.erase(iter);
    }
    // 添加到头部
    shard.lru_list.push_front(frame_id);
    shard.lru_map[frame_id] = shard.lru_list.begin();
}

size_t LRUReplacer::Size()
{
    size_t size = 0;
    for (auto &shard : shards_)
    {
        std::lock_guard guard(shard.mtx);
        size += shard.lru_list.size();
    }
    return size;
}