#include "iterator.h"

MergeIterator::MergeIterator(std::vector<std::shared_ptr<BaseIterator>> &iters, LsmFileHdr *file_hdr, bool filter)
    : iters_(std::move(iters)), file_hdr_(file_hdr), min_heap(compare_key), cur_pos(0), filter_(filter)
{
    int num = std::max(block_size_ / (sizeof(Rid) + sizeof(size_t) + sizeof(file_hdr_->col_tot_len_)),
            iters_.size());
    for (size_t id = 0; id < iters_.size(); id++)
    {
        auto &iter = iters_[id];
        if (iter->is_end())
            continue;
        min_heap.emplace(std::move((*iter)->first), (*iter)->second, id);
        ++(*iter);
    }
    fill(num);
}

BaseIterator &MergeIterator::operator++()
{
    int num = min_heap.size();
    if(num)
    {
        cached_value.reset();
        std::string key;
        Rid rid;
        do
        {
            key = std::move(min_heap.top().key);
            rid = min_heap.top().value;
            do
            {
                size_t id = min_heap.top().id;
                min_heap.pop();
                auto iter = iters_[id];
                if (!iter->is_end())
                {
                    min_heap.emplace(std::move((*iter)->first), (*iter)->second, id);
                    ++(*iter);
                }    
                /* code */
            } while (min_heap.size() && compare_key(min_heap.top().key, key) == 0);
            fill(num);
        } while (filter_ && !rid.is_valid() && min_heap.size());
    }
    return *this;
}

bool MergeIterator::operator==(const BaseIterator &other) const
{
    if (other.get_type() != IteratorType::MergeIterator)
        return false;
    auto other2 = static_cast<const MergeIterator &>(other);
    if(iters_.size() != other2.iters_.size())
        return false;
    for (size_t id = 0; id < iters_.size();id++)
    {
        if(iters_[id] != other2.iters_[id])
            return false;
    }
    return true;
}

bool MergeIterator::operator!=(const BaseIterator &other) const
{
    return !(*this == other);
}

MergeIterator::T& MergeIterator::operator*() const
{
    if (min_heap.empty())
        throw std::runtime_error("Dereferencing invalid iterator");
    if(!cached_value.has_value()) {
        cached_value = std::make_pair(min_heap.top().key, min_heap.top().value);
    }
    return cached_value.value();
}

MergeIterator::T* MergeIterator::operator->() const
{
    return &this->operator*(); // 通过调用operator*来实现operator->
}

IteratorType MergeIterator::get_type() const
{
    return IteratorType::MergeIterator;
}

bool MergeIterator::is_end() const
{
    return min_heap.empty();
}