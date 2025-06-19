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

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <fstream>
#include <iostream>
#include <string>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>

#include "common/config.h"
#include "errors.h"

/**
 * @description: DiskManager的作用主要是根据上层的需要对磁盘文件进行操作
 */
class DiskManager
{
public:
        explicit DiskManager();

        ~DiskManager() = default;

        void write_page(int fd, page_id_t page_no, const char *offset, int num_bytes);

        void read_page(int fd, page_id_t page_no, char *offset, int num_bytes);

        page_id_t allocate_page(int fd);

        void deallocate_page(page_id_t page_id);

        /*目录操作*/
        bool is_dir(const std::string &path);

        void create_dir(const std::string &path);

        void destroy_dir(const std::string &path);

        /*文件操作*/
        bool is_file(const std::string &path);

        void create_file(const std::string &path);

        inline void clear_log() {
                if(read_log_fd_ != -1) {
                        ftruncate(read_log_fd_, 0);
                }
        }

        void destroy_file(const std::string &path);

        int open_file(const std::string &path);

        void close_file(int fd);

        int get_file_size(const std::string &file_name);

        std::string get_file_name(int fd);

        int get_file_fd(const std::string &file_name);

        /*日志操作*/
        int read_log(char *log_data, int size, int offset);

        void write_log(char *log_data, int size);

        // void SetLogFd(int log_fd) { write_log_fd_ = log_fd; }

        // int GetLogFd() { return write_log_fd_; }
        void create_new_log_file() {
                if(!is_file(LOG_BAK_FILE_NAME))
                {
                        create_file(LOG_BAK_FILE_NAME);
                }
                write_log_fd_ = ::open(LOG_BAK_FILE_NAME.c_str(), O_RDWR);
        }

        void change_log_file() {
                ::close(read_log_fd_);
                ::unlink(LOG_FILE_NAME.c_str());
                ::rename(LOG_BAK_FILE_NAME.c_str(), LOG_FILE_NAME.c_str());
                read_log_fd_ = write_log_fd_;
        }

        /**
         * @description: 设置文件已经分配的页面个数
         * @param {int} fd 文件对应的文件句柄
         * @param {int} start_page_no 已经分配的页面个数，即文件接下来从start_page_no开始分配页面编号
         */
        void set_fd2pageno(int fd, int start_page_no) { fd2pageno_[fd] = start_page_no; }

        /**
         * @description: 获得文件目前已分配的页面个数，即如果文件要分配一个新页面，需要从fd2pagenp_[fd]开始分配
         * @return {page_id_t} 已分配的页面个数
         * @param {int} fd 文件对应的句柄
         */
        page_id_t get_fd2pageno(int fd) { return fd2pageno_[fd]; }

        /**
         * @description: 确保文件大小足够容纳指定的页面
         * @param {int} fd 文件句柄
         * @param {page_id_t} page_no 页面编号
         */
        void ensure_file_size(int fd, page_id_t page_no);

        static constexpr int MAX_FD = 8192;

private:
        // 文件打开列表，用于记录文件是否被打开
        std::unordered_map<std::string, int> path2fd_; //<Page文件磁盘路径,Page fd>哈希表
        std::unordered_map<int, std::string> fd2path_; //<Page fd,Page文件磁盘路径>哈希表
        std::shared_mutex path2fd_mutex_; // 保护path2fd_和fd2path_的互斥锁

        int read_log_fd_ = -1;   // WAL日志文件的文件句柄，默认为-1，代表未打开日志文件
        int write_log_fd_ = -1;
        std::atomic<page_id_t> fd2pageno_[MAX_FD]{}; // 文件中已经分配的页面个数，初始值为0
};