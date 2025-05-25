#include "lru_replacer.h"

LRUReplacer::LRUReplacer(size_t num_pages) : shards_(SHARD_COUNT) {}

bool LRUReplacer::victim(frame_id_t *frame_id)
{
    for (auto &shard : shards_)
    {
        std::lock_guard<std::mutex> guard(shard.mtx);
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
    std::lock_guard<std::mutex> guard(shard.mtx);
    if (shard.lru_map.count(frame_id))
    {
        shard.lru_list.erase(shard.lru_map[frame_id]);
        shard.lru_map.erase(frame_id);
    }
}

void LRUReplacer::unpin(frame_id_t frame_id)
{
    auto &shard = get_shard(frame_id);
    std::lock_guard<std::mutex> guard(shard.mtx);
    if (!shard.lru_map.count(frame_id))
    {
        shard.lru_list.push_front(frame_id);
        shard.lru_map[frame_id] = shard.lru_list.begin();
    }
}

size_t LRUReplacer::Size()
{
    size_t size = 0;
    for (auto &shard : shards_)
    {
        std::lock_guard<std::mutex> guard(shard.mtx);
        size += shard.lru_list.size();
    }
    return size;
}