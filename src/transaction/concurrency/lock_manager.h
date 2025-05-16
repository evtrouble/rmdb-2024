/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <mutex>
#include <condition_variable>
#include "transaction/transaction.h"
#include "common/common.h"
#include "system/sm_meta.h"

class GapLock
{
    struct Interval {
    public:
        Value upper_;
        Value lower_;
        bool upper_is_closed_;
        bool lower_is_closed_;
        bool init = false;

        bool overlap(const Interval &other) {
            // 计算最大下界和最小上界
            Value max_lower = std::max(lower_, other.lower_);
            Value min_upper = std::min(lower_, other.upper_);
        
            // 情况1：区间无交集
            if (min_upper < max_lower) return false;
        
            // 情况2：区间存在明确交集（非端点）
            if (max_lower < min_upper) return true;
        
            // 情况3：端点重合，检查闭合性
            return is_point_in_interval(max_lower) && other.is_point_in_interval(max_lower);
        }
        // 计算两个区间的交集
        bool intersect(const Interval& other) {
            // 计算交集下界及闭合性
            if (other.lower_ < lower_) {
            } else if (lower_ < other.lower_) {
                lower_is_closed_ = other.lower_is_closed_;
            } else {
                lower_is_closed_ = lower_is_closed_ && other.lower_is_closed_;
            }
            lower_ = std::max(lower_, other.lower_);
            // 计算交集上界及闭合性
            if (upper_ < other.upper_) {
            } else if (other.upper_ < upper_) {
                upper_is_closed_ = other.upper_is_closed_;
            } else {
                upper_is_closed_ = upper_is_closed_ && other.upper_is_closed_;
            }
            upper_ = std::min(upper_, other.upper_);
            // 验证交集有效性
            return is_valid_interval();
        }

        private:
        bool is_point_in_interval(const Value& x) const {
            // x 超出区间范围
            if (x < lower_ || upper_ < x) return false;
            // x 在区间内部
            if (lower_ < x && x < upper_) return true;
            // 检查端点闭合性
            if (x == lower_) return lower_is_closed_;
            if (x == upper_) return upper_is_closed_;
            return false;
        }
        // 判断区间是否有效（下界 <= 上界）
        inline bool is_valid_interval() {
            return (lower_ < upper_) ||
                (lower_ == upper_ && lower_is_closed_ && upper_is_closed_);
        }

    };
    public:
        std::vector<Interval> intervals;

        bool compatible(const GapLock &other) {
            assert(intervals.size() == other.intervals.size());
            for (size_t id = 0; id < intervals.size(); ++id) {
                if(!(intervals[id].init && other.intervals[id].init))
                    continue;
                if(!intervals[id].overlap(other.intervals[id]))
                    return true;
            }
            return false;
        }

        bool init(const std::vector<Condition> &conds, const TabMeta &tab)
        {
            if(conds.empty())
                return true;
            intervals.resize(tab.cols.size());
            for (auto &cond : conds)
            {
                size_t id = tab.cols_map.at(cond.lhs_col.col_name);
                Interval interval;
                auto &pos = intervals[id];
                auto &col = tab.cols[id];
                switch (cond.op)
                {
                    case CompOp::OP_EQ:
                        interval.lower_ = interval.upper_ = cond.rhs_val;
                        interval.lower_is_closed_ = interval.upper_is_closed_ = true;
                        break;
                    case CompOp::OP_GE:
                        interval.lower_ = cond.rhs_val;
                        interval.upper_.set_max(col.type, col.len);
                        interval.lower_is_closed_ = interval.upper_is_closed_ = true;
                        break;
                    case CompOp::OP_GT:
                        interval.lower_ = cond.rhs_val;
                        interval.upper_.set_max(col.type, col.len);
                        interval.upper_is_closed_ = true;
                        interval.lower_is_closed_ = false;
                        break;
                    case CompOp::OP_LE:
                        interval.upper_ = cond.rhs_val;
                        interval.lower_.set_min(col.type, col.len);
                        interval.lower_is_closed_ = interval.upper_is_closed_ = true;
                        break;
                    case CompOp::OP_LT:
                        interval.upper_ = cond.rhs_val;
                        interval.lower_.set_min(col.type, col.len);
                        interval.lower_is_closed_ = true;
                        interval.upper_is_closed_ = false;
                        break;
                    default:
                        break;
                    }
                if(!pos.init) {
                    pos = std::move(interval);
                    pos.init = true;
                }
                else if(!pos.intersect(interval)){
                    intervals.clear();
                    return false;
                }
            }
            return true;
        }

        inline bool valid() const { return intervals.size(); }

        bool init(const std::vector<Value> &values)
        {
            intervals.reserve(values.size());
            for(auto& value : values) {
                intervals.emplace_back(Interval{.upper_ = value, .lower_ = value, 
                    .upper_is_closed_ = true, .lower_is_closed_ = true, .init = true});
            }
            return true;
        }
};

class LockManager {
    /* 已加锁类型，包括无锁，共享锁、排他锁 */
    enum class LockMode { NONE, SHARED, EXLUCSIVE};
    /* 事务的加锁申请 */
    class LockRequest
    {
    public:
        
        LockMode mode = LockMode::NONE;
        std::vector<GapLock> shared_gaps;
        std::vector<GapLock> exclusive_gaps;

        bool shared_gap_compatible(const GapLock &other) {
            if(mode == LockMode::EXLUCSIVE)
                return false;
            return std::all_of(exclusive_gaps.begin(), exclusive_gaps.end(), [&](GapLock &gaplock)
                               { return gaplock.compatible(other); });
        }
        bool exclusive_gap_compatible(const GapLock &other) {
            if(mode != LockMode::NONE)
                return false;
            return std::all_of(shared_gaps.begin(), shared_gaps.end(), [&](GapLock &gaplock)
                               { return gaplock.compatible(other); }) &&
                   std::all_of(exclusive_gaps.begin(), exclusive_gaps.end(),
                               [&](GapLock &gaplock)
                               { return gaplock.compatible(other); });
        }
        bool shared_table_compatible() {
            if(mode == LockMode::EXLUCSIVE || exclusive_gaps.size())
                return false;
            return true;
        }
        bool exclusive_table_compatible() {
            if(mode != LockMode::NONE)
                return false;
            if(shared_gaps.size() || exclusive_gaps.size())
                return false;
            return true;
        }
    };

    /* 数据项上的加锁队列 */
    class LockRequestQueue {
    public:
        std::unordered_map<Transaction*, LockRequest> request_queue_;
        std::condition_variable cv_;            // 条件变量，用于唤醒正在等待加锁的申请，在no-wait策略下无需使用
        std::mutex latch_;
        // 自定义移动构造函数
        LockRequestQueue() = default;
        LockRequestQueue(LockRequestQueue &&other) noexcept 
            : request_queue_(std::move(other.request_queue_)) {}
        LockRequestQueue &operator=(LockRequestQueue &&other) noexcept
        {
            request_queue_ = std::move(other.request_queue_);
            return *this;
        }
    };

public:
    LockManager() : table_num_(MAX_TABLE_NUMBER), lock_table_(MAX_TABLE_NUMBER) {}

    ~LockManager() {}

    void lock_shared_on_gap(Transaction* txn, int tab_fd, const GapLock &gaplock);

    void lock_exclusive_on_gap(Transaction* txn, int tab_fd, const GapLock &gaplock);

    void lock_shared_on_table(Transaction* txn, int tab_fd);

    void lock_exclusive_on_table(Transaction* txn, int tab_fd);

    void unlock(Transaction* txn, int tab_fd);

private:
    // std::unordered_map<int, int>
    std::shared_mutex lock_;
    std::atomic_int32_t table_num_;
    std::vector<LockRequestQueue> lock_table_; // 全局锁表
};
