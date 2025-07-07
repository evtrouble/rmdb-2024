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

#include "ix_defs.h"
#include "transaction/transaction.h"
#include "ix_manager.h"

enum class Operation
{
    FIND = 0,
    INSERT,
    DELETE
}; // 三种操作：查找、插入、删除

inline int ix_compare(const char *a, const char *b, ColType type, int col_len)
{
    switch (type)
    {
    case TYPE_INT:
    {
        int ia = *(int *)a;
        int ib = *(int *)b;
        return (ia < ib) ? -1 : ((ia > ib) ? 1 : 0);
    }
    case TYPE_FLOAT:
    {
        float fa = *(float *)a;
        float fb = *(float *)b;
        return (fa < fb) ? -1 : ((fa > fb) ? 1 : 0);
    }
    case TYPE_STRING:
    case TYPE_DATETIME:
        return memcmp(a, b, col_len);
    default:
        throw InternalError("Unexpected data type");
    }
}

inline int ix_compare(const char *a, const char *b, const std::vector<ColType> &col_types, const std::vector<int> &col_lens)
{
    int offset = 0;
    for (size_t i = 0; i < col_types.size(); ++i)
    {
        int res = ix_compare(a + offset, b + offset, col_types[i], col_lens[i]);
        if (res != 0)
            return res;
        offset += col_lens[i];
    }
    return 0;
}

/* 管理B+树中的每个节点 */
// 结点内有n个元素就会n个子结点（经典b+树，更适合需要频繁插入和删除操作的情况，tpc-c测试中select
// 占比较少，只有4%）；每个元素是子结点元素里的最小值。
class IxNodeHandle
{
    friend class IxIndexHandle;
    friend class IxScan;

private:
    const IxFileHdr *file_hdr; // 节点所在文件的头部信息
    Page_Final *page;          // 存储节点的页面
    IxPageHdr *page_hdr;       // page->data的第一部分，指针指向首地址，长度为sizeof(IxPageHdr)
    char *keys;                // page->data的第二部分，指针指向首地址，长度为file_hdr->keys_size，每个key的长度为file_hdr->col_len
    Rid *rids;                 // page->data的第三部分，指针指向首地址

public:
    IxNodeHandle() = default;

    IxNodeHandle(const IxFileHdr *file_hdr_, Page_Final *page_) : file_hdr(file_hdr_), page(page_)
    {
        page_hdr = reinterpret_cast<IxPageHdr *>(page->get_data());
        keys = page->get_data() + sizeof(IxPageHdr);
        rids = reinterpret_cast<Rid *>(keys + file_hdr->keys_size_);
    }

    inline int get_size() const { return page_hdr->num_key; }

    inline void set_size(int size) { page_hdr->num_key = size; }

    inline int get_max_size() { return file_hdr->btree_order_ + 1; }

    inline int get_min_size() { return get_max_size() / 2; }

    inline int key_at(int i) { return *(int *)get_key(i); }

    /* 得到第i个孩子结点的page_no */
    inline page_id_t value_at(int i) { return get_rid(i)->page_no; }

    inline page_id_t get_page_no() { return page->get_page_id().page_no; }

    inline PageId_Final get_page_id() { return page->get_page_id(); }

    inline page_id_t get_next_leaf() const { return page_hdr->next_leaf; }

    inline page_id_t get_parent_page_no() { return page_hdr->parent; }

    inline bool is_leaf_page() { return page_hdr->is_leaf; }

    inline bool is_root_page() { return get_parent_page_no() == INVALID_PAGE_ID; }

    inline void set_next_leaf(page_id_t page_no) { page_hdr->next_leaf = page_no; }

    inline void set_parent_page_no(page_id_t parent) { page_hdr->parent = parent; }

    inline char *get_key(int key_idx) const { return keys + key_idx * file_hdr->col_tot_len_; }

    inline Rid *get_rid(int rid_idx) const { return &rids[rid_idx]; }

    inline void set_key(int key_idx, const char *key) { memcpy(keys + key_idx * file_hdr->col_tot_len_, key, file_hdr->col_tot_len_); }

    inline void set_rid(int rid_idx, const Rid &rid) { rids[rid_idx] = rid; }

    int lower_bound(const char *target) const;

    int upper_bound(const char *target) const;

    int upper_bound_adjust(const char *target) const;

    void insert_pairs(int pos, const char *key, const Rid *rid, int n);

    page_id_t internal_lookup(const char *key);

    bool leaf_lookup(const char *key, Rid **value);

    int insert(const char *key, const Rid &value);

    // 用于在结点中的指定位置插入单个键值对
    inline void insert_pair(int pos, const char *key, const Rid &rid) { insert_pairs(pos, key, &rid, 1); }

    void erase_pair(int pos);

    int remove(const char *key);

    /**
     * @brief used in internal node to remove the last key in root node, and return the last child
     *
     * @return the last child
     */
    page_id_t remove_and_return_only_child()
    {
        assert(get_size() == 1);
        page_id_t child_page_no = value_at(0);
        erase_pair(0);
        assert(get_size() == 0);
        return child_page_no;
    }

    /**
     * @brief 由parent调用，寻找child，返回child在parent中的rid_idx∈[0,page_hdr->num_key)
     * @param child
     * @return int
     */
    int find_child(IxNodeHandle &child)
    {
        int rid_idx;
        for (rid_idx = 0; rid_idx < page_hdr->num_key; ++rid_idx)
        {
            if (get_rid(rid_idx)->page_no == child.get_page_no())
            {
                break;
            }
        }
        assert(rid_idx < page_hdr->num_key);
        return rid_idx;
    }

    bool is_safe(Operation operation);
};

/* B+树 */
class IxIndexHandle
{
    friend class IxScan;
    friend class IxManager;

private:
    IxManager *ix_manager_; // 索引管理器
    int fd_;                // 存储B+树的文件
    IxFileHdr *file_hdr_;   // 存了root_page，但其初始化为2（第0页存FILE_HDR_PAGE，第1页存LEAF_HEADER_PAGE）
    std::shared_mutex root_lacth_;
    bool is_deleted = false;

public:
    IxIndexHandle(IxManager *ix_manager, int fd);
    ~IxIndexHandle()
    {
        if (is_deleted)
        {
            auto index_name = ix_manager_->disk_manager_->get_file_name(fd_); // 获取文件名
            ix_manager_->close_index(this, false);
            ix_manager_->disk_manager_->destroy_file(index_name); // 删除文件
        }
        else
        {
            ix_manager_->close_index(this);
        }
        delete file_hdr_;
    }

    inline int get_fd() { return fd_; }
    inline void mark_deleted() { is_deleted = true; }

    // for search
    // bool get_value(const char *key, std::vector<Rid> *result, Transaction *transaction);
    bool get_value(const char *key, Rid *result, Transaction *transaction);

    IxNodeHandle find_leaf_page(const char *key, Operation operation, Transaction *transaction,
                                bool find_first = true);

    // for insert
    page_id_t insert_entry(const char *key, const Rid &value, Transaction *transaction, bool abort = false);

    IxNodeHandle split(IxNodeHandle &node);

    void insert_into_parent(IxNodeHandle &old_node, const char *key, IxNodeHandle &new_node);

    // for delete
    bool delete_entry(const char *key, const Rid &value, Transaction *transaction, bool abort = false);

    bool coalesce_or_redistribute(IxNodeHandle &node, Transaction *transaction);
    bool coalesce_or_redistribute_internal(IxNodeHandle &node, Transaction *transaction);
    bool adjust_root(IxNodeHandle &old_root_node);

    void redistribute(IxNodeHandle &neighbor_node, IxNodeHandle &node, IxNodeHandle &parent, int index);

    bool coalesce(IxNodeHandle neighbor_node, IxNodeHandle node, IxNodeHandle &parent, int index, Transaction *transaction);

    std::pair<IxNodeHandle, int> lower_bound(const char *key);

    std::pair<IxNodeHandle, int> upper_bound(const char *key);

    // Iid leaf_end();

    // Iid leaf_begin();

    void lock_shared(IxNodeHandle &page_no);
    void unlock_shared(IxNodeHandle &page_no) const;

private:
    // 辅助函数
    inline void update_root_page_no(page_id_t root) { file_hdr_->root_page_ = root; }

    inline bool is_empty() const { return file_hdr_->root_page_ == IX_NO_PAGE; }

    // for get/create node
    IxNodeHandle fetch_node(int page_no) const;

    IxNodeHandle create_node();

    // for maintain data structure
    void maintain_parent(IxNodeHandle &node);

    // void erase_leaf(IxNodeHandle &leaf);

    void maintain_child(IxNodeHandle &node, int child_idx);

    void release_all_xlock(std::shared_ptr<std::deque<Page_Final *>> page_set, bool dirty);

    // for index test
    Rid get_rid(const Iid &iid) const;
};