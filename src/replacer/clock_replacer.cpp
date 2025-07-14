#include "clock_replacer.h"

bool ClockReplacer::victim(frame_id_t *frame_id) {
    for (size_t i = 0; i < num_frames_; ++i) {
        // 原子获取当前时钟指针位置，并移动到下一个位置
        size_t index = clock_hand_.fetch_add(1, std::memory_order_relaxed) % num_frames_;
        uint32_t old_state = states_[index].load(std::memory_order_acquire);
        
        while (true) {
            // 检查帧是否在替换器中（present=1）
            if ((old_state & 1) == 0) {
                break; // 不在替换器中，跳过
            }

            if (old_state & 2) { // referenced=1
                // 尝试将 referenced 位置 0 (保留 present=1)
                uint32_t new_state = old_state & ~2; // 清除 referenced 位
                if (states_[index].compare_exchange_weak(old_state, new_state,
                                                        std::memory_order_release,
                                                        std::memory_order_acquire)) {
                    break; // 成功更新状态，继续下一帧
                }
                // CAS 失败，重试（old_state 已更新）
            } else { // referenced=0
                // 尝试移除帧：设置 present=0（保留 referenced=0）
                uint32_t new_state = old_state & ~1;
                if (states_[index].compare_exchange_weak(old_state, new_state,
                                                        std::memory_order_release,
                                                        std::memory_order_acquire)) {
                    *frame_id = static_cast<frame_id_t>(index); // 返回牺牲帧
                    return true;
                }
                // CAS 失败，重试（old_state 已更新）
            }
        }
    }
    return false; // 未找到牺牲帧
}

void ClockReplacer::pin(frame_id_t frame_id) {
    size_t index = static_cast<size_t>(frame_id);
    uint32_t old_state = states_[index].load(std::memory_order_acquire);
    
    while (true) {
        // 如果帧已不在替换器中（present=0），直接返回
        if ((old_state & 1) == 0) {
            return;
        }
        // 尝试设置 present=0（保留 referenced 位）
        uint32_t new_state = old_state & ~1;
        if (states_[index].compare_exchange_weak(old_state, new_state,
                                                std::memory_order_release,
                                                std::memory_order_acquire)) {
            return; // 成功更新状态
        }
        // CAS 失败，重试（old_state 已更新）
    }
}

void ClockReplacer::unpin(frame_id_t frame_id) {
    size_t index = static_cast<size_t>(frame_id);
    uint32_t old_state = states_[index].load(std::memory_order_acquire);
    
    while (true) {
        // 设置 present=1 和 referenced=1
        uint32_t new_state = old_state | 3; // 二进制: 0b11
        // 如果状态已为目标状态，直接返回
        if (old_state == new_state) {
            return;
        }
        if (states_[index].compare_exchange_weak(old_state, new_state,
                                                std::memory_order_release,
                                                std::memory_order_acquire)) {
            return; // 成功更新状态
        }
        // CAS 失败，重试（old_state 已更新）
    }
}

size_t ClockReplacer::Size() {
    size_t count = 0;
    for (size_t i = 0; i < num_frames_; ++i) {
        // 统计所有在替换器中的帧（present=1）
        if (states_[i].load(std::memory_order_relaxed) & 1) {
            ++count;
        }
    }
    return count;
}