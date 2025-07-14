#include <atomic>
#include <vector>
#include <cstddef>
#include "common/config.h"
#include "replacer/replacer.h"

class ClockReplacer : public Replacer {
public:
    explicit ClockReplacer(size_t num_pages) 
        : num_frames_(num_pages), 
          states_(num_pages),
          clock_hand_(0) {
        // 初始化所有帧：在替换器中（present=1），引用位为0（referenced=0）
        for (size_t i = 0; i < num_frames_; ++i) {
            states_[i] = 1; // 二进制: 0b01 (present=1, referenced=0)
        }
    }

    ~ClockReplacer() override = default;

    bool victim(frame_id_t *frame_id) override;

    void pin(frame_id_t frame_id) override;

    void unpin(frame_id_t frame_id) override;

    size_t Size() override;

private:
    const size_t num_frames_;                     // 帧总数
    std::vector<std::atomic<uint32_t>> states_;   // 每个帧的状态（原子变量）
    std::atomic<size_t> clock_hand_;              // 时钟指针（原子变量）
};