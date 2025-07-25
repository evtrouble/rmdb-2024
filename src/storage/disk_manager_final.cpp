/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/disk_manager_final.h"

#include <assert.h>   // for assert
#include <string.h>   // for memset
#include <sys/stat.h> // for stat
#include <unistd.h>   // for lseek

#include "defs.h"

DiskManager_Final::DiskManager_Final() : fd2pageno_{0}
{
    // memset(fd2pageno_, 0, MAX_FD * (sizeof(std::atomic<page_id_t>) / sizeof(char)));
}

/**
 * @description: 将数据写入文件的指定磁盘页面中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 写入目标页面的page_id
 * @param {char} *offset 要写入磁盘的数据
 * @param {int} num_bytes 要写入磁盘的数据大小
 */
void DiskManager_Final::write_page(int fd, page_id_t page_no, const char *offset, int num_bytes)
{
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用write()函数
    // 注意write返回值与num_bytes不等时 throw InternalError("DiskManager_Final::write_page Error");
    ::lseek(fd, page_no * PAGE_SIZE, SEEK_SET);
    if (::write(fd, offset, num_bytes) != num_bytes)
    {
        throw InternalError("DiskManager_Final::write_page Error");
    }
}

/**
 * @description: 读取文件中指定编号的页面中的部分数据到内存中
 * @param {int} fd 磁盘文件的文件句柄
 * @param {page_id_t} page_no 指定的页面编号
 * @param {char} *offset 读取的内容写入到offset中
 * @param {int} num_bytes 读取的数据量大小
 */
void DiskManager_Final::read_page(int fd, page_id_t page_no, char *offset, int num_bytes)
{
    // 1.lseek()定位到文件头，通过(fd,page_no)可以定位指定页面及其在磁盘文件中的偏移量
    // 2.调用read()函数
    // 注意read返回值与num_bytes不等时，throw InternalError("DiskManager_Final::read_page Error");
    ::lseek(fd, page_no * PAGE_SIZE, SEEK_SET);
    // if (::read(fd, offset, num_bytes) != num_bytes) {
    if (::read(fd, offset, num_bytes) < 0)
    {
        throw InternalError("DiskManager_Final::read_page Error");
    }
}

/**
 * @description: 分配一个新的页号
 * @return {page_id_t} 分配的新页号
 * @param {int} fd 指定文件的文件句柄
 */
page_id_t DiskManager_Final::allocate_page(int fd)
{
    // 简单的自增分配策略，指定文件的页面编号加1
    assert(fd >= 0 && fd < MAX_FD);
    return fd2pageno_[fd]++;
}

void DiskManager_Final::deallocate_page(__attribute__((unused)) page_id_t page_id) {}

bool DiskManager_Final::is_dir(const std::string &path)
{
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

void DiskManager_Final::create_dir(const std::string &path)
{
    // Create a subdirectory
    std::string cmd = "mkdir " + path;
    if (system(cmd.c_str()) < 0)
    { // 创建一个名为path的目录
        throw UnixError();
    }
}

void DiskManager_Final::destroy_dir(const std::string &path)
{
    std::string cmd = "rm -r " + path;
    if (system(cmd.c_str()) < 0)
    {
        throw UnixError();
    }
}

/**
 * @description: 判断指定路径文件是否存在
 * @return {bool} 若指定路径文件存在则返回true
 * @param {string} &path 指定路径文件
 */
bool DiskManager_Final::is_file(const std::string &path)
{
    // 用struct stat获取文件信息
    struct stat st;
    return stat(path.c_str(), &st) == 0 && S_ISREG(st.st_mode);
}

/**
 * @description: 用于创建指定路径文件
 * @return {*}
 * @param {string} &path
 */
void DiskManager_Final::create_file(const std::string &path)
{
    // 调用open()函数，使用O_CREAT模式
    // 注意不能重复创建相同文件
    if (is_file(path))
    {
        throw FileExistsError(path);
    }

    int fd = ::open(path.c_str(), O_CREAT | O_EXCL, 0600);
    if (fd == -1)
    {
        throw InternalError("file creates error");
    }
    ::close(fd);
}

/**
 * @description: 删除指定路径的文件
 * @param {string} &path 文件所在路径
 */
void DiskManager_Final::destroy_file(const std::string &path)
{
    // 调用unlink()函数
    // 注意不能删除未关闭的文件
    if (!is_file(path))
    {
        throw FileNotFoundError(path);
    }

    {
        std::shared_lock lock(path2fd_mutex_);
        if (path2fd_.count(path))
        {
            throw FileNotClosedError(path);
        }
    }
    ::unlink(path.c_str());
}

/**
 * @description: 打开指定路径文件
 * @return {int} 返回打开的文件的文件句柄
 * @param {string} &path 文件所在路径
 */
int DiskManager_Final::open_file(const std::string &path)
{
    // 调用open()函数，使用O_RDWR模式
    // 注意不能重复打开相同文件，并且需要更新文件打开列表
    if (!is_file(path))
    {
        throw FileNotFoundError(path);
    }

    std::lock_guard lock(path2fd_mutex_);
    if (path2fd_.count(path))
        return path2fd_[path];
    int fd = ::open(path.c_str(), O_RDWR);
    if (fd != -1)
    {
        path2fd_.emplace(path, fd);
        fd2path_.emplace(fd, path);
    }
    return fd;
}

/**
 * @description:用于关闭指定路径文件
 * @param {int} fd 打开的文件的文件句柄
 */
void DiskManager_Final::close_file(int fd)
{
    // 调用close()函数
    // 注意不能关闭未打开的文件，并且需要更新文件打开列表
    std::unique_lock lock(path2fd_mutex_);
    auto iter = fd2path_.find(fd);
    if (iter == fd2path_.end())
    {
        throw FileNotOpenError(fd);
    }

    path2fd_.erase(iter->second);
    fd2path_.erase(iter);

    lock.unlock();
    ::close(fd);
}

/**
 * @description: 获得文件的大小
 * @return {int} 文件的大小
 * @param {string} &file_name 文件名
 */
int DiskManager_Final::get_file_size(const std::string &file_name)
{
    struct stat stat_buf;
    int rc = stat(file_name.c_str(), &stat_buf);
    return rc == 0 ? stat_buf.st_size : -1;
}

/**
 * @description: 根据文件句柄获得文件名
 * @return {string} 文件句柄对应文件的文件名
 * @param {int} fd 文件句柄
 */
std::string DiskManager_Final::get_file_name(int fd)
{
    std::shared_lock lock(path2fd_mutex_);
    if (!fd2path_.count(fd))
    {
        throw FileNotOpenError(fd);
    }
    return fd2path_[fd];
}

/**
 * @description:  获得文件名对应的文件句柄
 * @return {int} 文件句柄
 * @param {string} &file_name 文件名
 */
int DiskManager_Final::get_file_fd(const std::string &file_name)
{
    std::shared_lock lock(path2fd_mutex_);
    if (!path2fd_.count(file_name))
    {
        lock.unlock();
        return open_file(file_name);
    }
    return path2fd_[file_name];
}

/**
 * @description:  读取日志文件内容
 * @return {int} 返回读取的数据量，若为-1说明读取数据的起始位置超过了文件大小
 * @param {char} *log_data 读取内容到log_data中
 * @param {int} size 读取的数据量大小
 * @param {int} offset 读取的内容在文件中的位置
 */
int DiskManager_Final::read_log(char *log_data, int size, int offset)
{
    // read log file from the previous end
    if (read_log_fd_ == -1)
    {
        if (!is_file(LOG_FILE_NAME))
        {
            create_file(LOG_FILE_NAME);
        }
        read_log_fd_ = ::open(LOG_FILE_NAME.c_str(), O_RDWR);
        write_log_fd_ = read_log_fd_; // 读写日志文件使用同一个文件句柄
    }
    int file_size = get_file_size(LOG_FILE_NAME);
    if (offset > file_size)
    {
        return -1;
    }

    size = std::min(size, file_size - offset);
    if (size == 0)
        return 0;
    lseek(read_log_fd_, offset, SEEK_SET);
    ssize_t bytes_read = read(read_log_fd_, log_data, size);
    assert(bytes_read == size);
    return bytes_read;
}

/**
 * @description: 写日志内容
 * @param {char} *log_data 要写入的日志内容
 * @param {int} size 要写入的内容大小
 */
void DiskManager_Final::write_log(char *log_data, int size)
{
    if (write_log_fd_ == -1)
    {
        if (!is_file(LOG_FILE_NAME))
        {
            create_file(LOG_FILE_NAME);
        }
        write_log_fd_ = ::open(LOG_FILE_NAME.c_str(), O_RDWR);
        read_log_fd_ = write_log_fd_; // 读写日志文件使用同一个文件句柄
    }

    // write from the file_end
    lseek(write_log_fd_, 0, SEEK_END);
    ssize_t bytes_write = write(write_log_fd_, log_data, size);
    if (bytes_write != size)
    {
        throw UnixError();
    }
}

void DiskManager_Final::ensure_file_size(int fd, page_id_t page_no)
{
    // 计算所需的文件大小
    int required_size = page_no * PAGE_SIZE;

    // 获取当前文件大小
    std::string file_name = get_file_name(fd);
    int current_size = get_file_size(file_name);

    // 如果需要，扩展文件大小
    if (current_size < required_size)
    {
        // 使用ftruncate扩展文件
        if (ftruncate(fd, required_size) != 0)
        {
            throw InternalError("DiskManager_Final::ensure_file_size Error");
        }
    }
}