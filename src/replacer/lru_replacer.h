#pragma once

#include <list>
#include <mutex>
#include <vector>
#include <unordered_map>
#include "common/config.h"
#include "replacer/replacer.h"

class LRUReplacer : public Replacer {
public:
    explicit LRUReplacer(size_t num_pages);
    ~LRUReplacer() = default;

    bool victim(frame_id_t* frame_id) override;
    void pin(frame_id_t frame_id) override;
    void unpin(frame_id_t frame_id) override;
    size_t Size() override;

private:
    struct Shard {
        std::mutex mtx;
        std::list<frame_id_t> lru_list;
        std::unordered_map<frame_id_t, std::list<frame_id_t>::iterator> lru_map;
    };
    
    static constexpr int SHARD_COUNT = 16;  // 根据CPU核心数调整
    std::vector<Shard> shards_;
    
    inline Shard& get_shard(frame_id_t frame_id) {
        return shards_[frame_id & (SHARD_COUNT - 1)];
    }
};