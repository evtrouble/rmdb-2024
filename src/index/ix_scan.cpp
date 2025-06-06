/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "ix_scan.h"

/**
 * @brief 
 * @todo 加上读锁（需要使用缓冲池得到page）
 */
void IxScan::next() {
    assert(!is_end());
    IxNodeHandle node = ih_->fetch_node(iid_.page_no);
    ih_->lock_shared(node);
    assert(node.is_leaf_page());
    assert(iid_.slot_no < node.get_size());
    // increment slot no
    ++iid_.slot_no;
    if (node.get_next_leaf() != IX_LEAF_HEADER_PAGE && iid_.slot_no == node.get_size()) {
        // go to next leaf
        iid_.slot_no = 0;
        iid_.page_no = node.get_next_leaf();
    }
    ih_->unlock_shared(node);
}

Rid IxScan::rid() const {
    return ih_->get_rid(iid_);
}