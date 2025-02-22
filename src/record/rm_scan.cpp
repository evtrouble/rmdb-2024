/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "rm_scan.h"
#include "rm_file_handle.h"

/**
 * @brief 初始化file_handle和rid
 * @param file_handle
 */
RmScan::RmScan(const RmFileHandle *file_handle) : file_handle_(file_handle)
{
    // 初始化file_handle和rid（指向第一个存放了记录的位置）
    rid_.page_no = RM_FIRST_RECORD_PAGE; // 从第一个记录页开始
    rid_.slot_no = 0;                    // 从第一个slot开始

    // 如果当前位置没有记录，移动到下一个记录
    if (!file_handle_->is_record(rid_))
    {
        next();
    }
}

/**
 * @brief 找到文件中下一个存放了记录的位置
 */
void RmScan::next()
{
    // 获取文件头信息
    RmFileHdr file_hdr = file_handle_->get_file_hdr();

    while (rid_.page_no < file_hdr.num_pages)
    {
        // 在当前页内查找下一个记录
        while (rid_.slot_no < file_hdr.num_records_per_page)
        {
            rid_.slot_no++;
            if (rid_.slot_no == file_hdr.num_records_per_page)
            {
                break; // 当前页已经遍历完
            }
            if (file_handle_->is_record(rid_))
            {
                return; // 找到下一个记录
            }
        }

        // 当前页遍历完，移动到下一页
        rid_.page_no++;
        rid_.slot_no = -1; // 设为-1是因为外层循环开始会先slot_no++

        if (rid_.page_no >= file_hdr.num_pages)
        {
            break; // 所有页都遍历完了
        }
    }
}

/**
 * @brief ​ 判断是否到达文件末尾
 */
bool RmScan::is_end() const
{
    RmFileHdr file_hdr = file_handle_->get_file_hdr();
    // 如果当前页号超出了总页数，或者是最后一页且slot号超出了每页记录数，就表示到达文件末尾
    return rid_.page_no >= file_hdr.num_pages ||
           (rid_.page_no == file_hdr.num_pages - 1 && rid_.slot_no >= file_hdr.num_records_per_page);
}

/**
 * @brief RmScan内部存放的rid
 */
Rid RmScan::rid() const
{
    return rid_;
}