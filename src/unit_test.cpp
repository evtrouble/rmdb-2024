/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#undef NDEBUG

#define private public

#include "record/rm.h"
#include "storage/buffer_pool_manager.h"

#undef private

#include <algorithm>
#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <iostream>
#include <memory>
#include <random>
#include <set>
#include <string>
#include <thread> // NOLINT
#include <unordered_map>
#include <vector>
#include <fstream>

#include "gtest/gtest.h"
#include "replacer/lru_replacer.h"
#include "storage/disk_manager.h"
#include "common/debug_log.h"

const std::string TEST_DB_NAME = "BufferPoolManagerTest_db"; // 以数据库名作为根目录
const std::string TEST_FILE_NAME = "basic";                  // 测试文件的名字
const std::string TEST_FILE_NAME_CCUR = "concurrency";       // 测试文件的名字
const std::string TEST_FILE_NAME_BIG = "bigdata";            // 测试文件的名字
constexpr int MAX_FILES = 32;
constexpr int MAX_PAGES = 128;
constexpr size_t TEST_BUFFER_POOL_SIZE = MAX_FILES * MAX_PAGES;

// 创建BufferPoolManager
auto disk_manager = std::make_unique<DiskManager>();
auto buffer_pool_manager = std::make_unique<BufferPoolManager>(TEST_BUFFER_POOL_SIZE, disk_manager.get());

std::unordered_map<int, char *> mock;  // fd -> buffer

char *mock_get_page(int fd, int page_no) { return &mock[fd][page_no * PAGE_SIZE]; }

void check_disk(int fd, int page_no) {
    char buf[PAGE_SIZE];
    disk_manager->read_page(fd, page_no, buf, PAGE_SIZE);
    char *mock_buf = mock_get_page(fd, page_no);
    assert(memcmp(buf, mock_buf, PAGE_SIZE) == 0);
}

void check_disk_all() {
    for (auto &file : mock) {
        int fd = file.first;
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_disk(fd, page_no);
        }
    }
}

void check_cache(int fd, int page_no) {
    Page *page = buffer_pool_manager->fetch_page(PageId{fd, page_no});
    char *mock_buf = mock_get_page(fd, page_no);  // &mock[fd][page_no * PAGE_SIZE];
    assert(memcmp(page->get_data(), mock_buf, PAGE_SIZE) == 0);
    buffer_pool_manager->unpin_page(PageId{fd, page_no}, false);
}

void check_cache_all() {
    for (auto &file : mock) {
        int fd = file.first;
        for (int page_no = 0; page_no < MAX_PAGES; page_no++) {
            check_cache(fd, page_no);
        }
    }
}

void rand_buf(int size, char *buf) {
    for (int i = 0; i < size; i++) {
        int rand_ch = rand() & 0xff;
        buf[i] = rand_ch;
    }
}

int rand_fd() {
    assert(mock.size() == MAX_FILES);
    int fd_idx = rand() % MAX_FILES;
    auto it = mock.begin();
    for (int i = 0; i < fd_idx; i++) {
        it++;
    }
    return it->first;
}

struct rid_hash_t {
    size_t operator()(const Rid &rid) const { return (rid.page_no << 16) | rid.slot_no; }
};

struct rid_equal_t {
    bool operator()(const Rid &x, const Rid &y) const { return x.page_no == y.page_no && x.slot_no == y.slot_no; }
};

void check_equal(const RmFileHandle *file_handle,
                 const std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> &mock)
{
    std::cout << "\n=== 开始检查记录 ===" << std::endl;
    std::cout << "文件信息: " << std::endl;
    std::cout << "- 记录大小: " << file_handle->file_hdr_.record_size << std::endl;
    std::cout << "- 总页数: " << file_handle->file_hdr_.num_pages << std::endl;
    std::cout << "- 每页记录数: " << file_handle->file_hdr_.num_records_per_page << std::endl;
    std::cout << "- 当前记录总数: " << mock.size() << std::endl;

    // Test all records
    std::cout << "\n1. 检查所有已知记录..." << std::endl;
    for (auto &entry : mock)
    {
        Rid rid = entry.first;
        auto mock_buf = (char *)entry.second.c_str();
        std::cout << "检查记录 " << rid << " ";
        auto rec = file_handle->get_record(rid, nullptr);
        if (memcmp(mock_buf, rec->data, file_handle->file_hdr_.record_size) == 0)
        {
            std::cout << "✓" << std::endl;
        }
        else
        {
            std::cout << "✗ (数据不匹配)" << std::endl;
        }
        assert(memcmp(mock_buf, rec->data, file_handle->file_hdr_.record_size) == 0);
    }

    // Randomly get record
    std::cout << "\n2. 随机检查记录存在性..." << std::endl;
    for (int i = 0; i < 10; i++)
    {
        Rid rid = {.page_no = 1 + rand() % (file_handle->file_hdr_.num_pages - 1),
                   .slot_no = rand() % file_handle->file_hdr_.num_records_per_page};
        bool mock_exist = mock.count(rid) > 0;
        bool rm_exist = file_handle->is_record(rid);
        std::cout << "检查随机位置 " << rid << " ";
        std::cout << "(预期:" << (mock_exist ? "存在" : "不存在")
                  << ", 实际:" << (rm_exist ? "存在" : "不存在") << ") ";
        if (mock_exist == rm_exist)
        {
            std::cout << "✓" << std::endl;
        }
        else
        {
            std::cout << "✗" << std::endl;
        }
        assert(rm_exist == mock_exist);
    }

    // Test RM scan
    std::cout << "\n3. 测试记录扫描..." << std::endl;
    size_t num_records = 0;
    std::cout << "开始扫描..." << std::endl;
    for (RmScan scan(file_handle); !scan.is_end(); scan.next())
    {
        Rid cur_rid = scan.rid();
        std::cout << "扫描到记录 " << cur_rid << " ";
        if (mock.count(cur_rid) > 0)
        {
            auto rec = file_handle->get_record(cur_rid, nullptr);
            if (memcmp(rec->data, mock.at(cur_rid).c_str(), file_handle->file_hdr_.record_size) == 0)
            {
                std::cout << "✓" << std::endl;
            }
            else
            {
                std::cout << "✗ (数据不匹配)" << std::endl;
            }
            assert(memcmp(rec->data, mock.at(cur_rid).c_str(), file_handle->file_hdr_.record_size) == 0);
        }
        else
        {
            std::cout << "✗ (记录不应存在)" << std::endl;
        }
        assert(mock.count(scan.rid()) > 0);
        num_records++;
    }
    std::cout << "扫描完成，共扫描到 " << num_records << " 条记录" << std::endl;
    std::cout << "预期记录数: " << mock.size() << std::endl;
    if (num_records == mock.size())
    {
        std::cout << "记录数匹配 ✓" << std::endl;
    }
    else
    {
        std::cout << "记录数不匹配 ✗" << std::endl;
    }
    assert(num_records == mock.size());
    std::cout << "=== 检查完成 ===\n"
              << std::endl;
}

/** 注意：每个测试点只测试了单个文件！
 * 对于每个测试点，先创建和进入目录TEST_DB_NAME
 * 然后在此目录下创建和打开文件TEST_FILE_NAME_BIG，记录其文件描述符fd */

class BigStorageTest : public ::testing::Test {
   public:
    std::unique_ptr<DiskManager> disk_manager_;
    int fd_ = -1;  // 此文件描述符为disk_manager_->open_file的返回值

   public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
        // For each test, we create a new DiskManager
        disk_manager_ = std::make_unique<DiskManager>();
        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (disk_manager_->is_file(TEST_FILE_NAME_BIG)) {
            disk_manager_->destroy_file(TEST_FILE_NAME_BIG);
        }
        // 创建测试文件
        disk_manager_->create_file(TEST_FILE_NAME_BIG);
        assert(disk_manager_->is_file(TEST_FILE_NAME_BIG));
        // 打开测试文件
        fd_ = disk_manager_->open_file(TEST_FILE_NAME_BIG);
        assert(fd_ != -1);
    }

    // This function is called after every test.
    void TearDown() override
    {
        disk_manager_->close_file(fd_);
        // disk_manager_->destroy_file(TEST_FILE_NAME_BIG);  // you can choose to delete the file

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    };
};

TEST(LRUReplacerTest, SampleTest)
{
    std::ofstream debug_log("lru_test.log", std::ios::out | std::ios::app);
    debug_log << "=== 开始LRU替换器测试 ===" << std::endl;

    LRUReplacer lru_replacer(7);

    // Scenario: unpin six elements, i.e. add them to the replacer.
    debug_log << "添加6个元素到替换器中" << std::endl;
    lru_replacer.unpin(1);
    lru_replacer.unpin(2);
    lru_replacer.unpin(3);
    lru_replacer.unpin(4);
    lru_replacer.unpin(5);
    lru_replacer.unpin(6);
    lru_replacer.unpin(1);
    EXPECT_EQ(6, lru_replacer.Size());
    debug_log << "替换器大小验证: 期望=6, 实际=" << lru_replacer.Size() << std::endl;

    // Scenario: get three victims from the lru.
    debug_log << "开始获取3个victim" << std::endl;
    int value;
    lru_replacer.victim(&value);
    EXPECT_EQ(1, value);
    debug_log << "第1个victim: " << value << std::endl;

    lru_replacer.victim(&value);
    EXPECT_EQ(2, value);
    debug_log << "第2个victim: " << value << std::endl;

    lru_replacer.victim(&value);
    EXPECT_EQ(3, value);
    debug_log << "第3个victim: " << value << std::endl;

    // Scenario: pin elements in the replacer.
    debug_log << "开始pin元素" << std::endl;
    lru_replacer.pin(3);
    lru_replacer.pin(4);
    EXPECT_EQ(2, lru_replacer.Size());
    debug_log << "pin后替换器大小: " << lru_replacer.Size() << std::endl;

    // Scenario: unpin 4. We expect that the reference bit of 4 will be set to 1.
    debug_log << "unpin元素4" << std::endl;
    lru_replacer.unpin(4);

    // Scenario: continue looking for victims. We expect these victims.
    debug_log << "继续获取victims" << std::endl;
    lru_replacer.victim(&value);
    EXPECT_EQ(5, value);
    debug_log << "获取victim: " << value << std::endl;

    lru_replacer.victim(&value);
    EXPECT_EQ(6, value);
    debug_log << "获取victim: " << value << std::endl;

    lru_replacer.victim(&value);
    EXPECT_EQ(4, value);
    debug_log << "获取victim: " << value << std::endl;

    debug_log << "=== LRU替换器测试完成 ===" << std::endl;
    debug_log.close();
}

/** 注意：每个测试点只测试了单个文件！
 * 对于每个测试点，先创建和进入目录TEST_DB_NAME
 * 然后在此目录下创建和打开文件TEST_FILE_NAME，记录其文件描述符fd */
class BufferPoolManagerTest : public ::testing::Test
{
public:
    std::unique_ptr<DiskManager> disk_manager_;
    int fd_ = -1; // 此文件描述符为disk_manager_->open_file的返回值

public:
    // This function is called before every test.
    void SetUp() override
    {
        ::testing::Test::SetUp();
        // For each test, we create a new DiskManager
        disk_manager_ = std::make_unique<DiskManager>();
        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager_->is_dir(TEST_DB_NAME)) {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0) {
            throw UnixError();
        }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (disk_manager_->is_file(TEST_FILE_NAME)) {
            disk_manager_->destroy_file(TEST_FILE_NAME);
        }
        // 创建测试文件
        disk_manager_->create_file(TEST_FILE_NAME);
        assert(disk_manager_->is_file(TEST_FILE_NAME));
        // 打开测试文件
        fd_ = disk_manager_->open_file(TEST_FILE_NAME);
        assert(fd_ != -1);
    }

    // This function is called after every test.
    void TearDown() override {
        disk_manager_->close_file(fd_);
        // disk_manager_->destroy_file(TEST_FILE_NAME);  // you can choose to delete the file

        // 返回上一层目录
        if (chdir("..") < 0) {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    };
};

// NOLINTNEXTLINE
TEST_F(BufferPoolManagerTest, SampleTest)
{
    std::ofstream debug_log("buffer_pool_test.log", std::ios::out | std::ios::app);
    debug_log << "=== 开始缓冲池管理器测试 ===" << std::endl;

    // create BufferPoolManager
    const size_t buffer_pool_size = 10;
    debug_log << "创建缓冲池管理器，大小: " << buffer_pool_size << std::endl;

    auto disk_manager = BufferPoolManagerTest::disk_manager_.get();
    auto bpm = std::make_unique<BufferPoolManager>(buffer_pool_size, disk_manager);

    // create tmp PageId
    int fd = BufferPoolManagerTest::fd_;
    debug_log << "文件描述符: " << fd << std::endl;

    PageId page_id_temp = {.fd = fd, .page_no = INVALID_PAGE_ID};
    debug_log << "尝试创建新页面" << std::endl;
    auto *page0 = bpm->new_page(&page_id_temp);

    // Scenario: The buffer pool is empty. We should be able to create a new page.
    debug_log << "检查页面创建结果" << std::endl;
    ASSERT_NE(nullptr, page0);
    EXPECT_EQ(0, page_id_temp.page_no);
    debug_log << "成功创建页面，页号: " << page_id_temp.page_no << std::endl;

    // Scenario: Once we have a page, we should be able to read and write content.
    debug_log << "写入数据: 'Hello'" << std::endl;
    snprintf(page0->get_data(), sizeof(page0->get_data()), "Hello");
    EXPECT_EQ(0, strcmp(page0->get_data(), "Hello"));
    debug_log << "数据写入成功" << std::endl;

    // Scenario: We should be able to create new pages until we fill up the buffer pool.
    debug_log << "尝试填满缓冲池" << std::endl;
    for (size_t i = 1; i < buffer_pool_size; ++i)
    {
        debug_log << "创建第 " << i << " 个新页面" << std::endl;
        EXPECT_NE(nullptr, bpm->new_page(&page_id_temp));
    }
    debug_log << "缓冲池已填满" << std::endl;

    // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
    debug_log << "测试缓冲池已满的情况" << std::endl;
    for (size_t i = buffer_pool_size; i < buffer_pool_size * 2; ++i)
    {
        debug_log << "尝试创建第 " << i << " 个新页面" << std::endl;
        EXPECT_EQ(nullptr, bpm->new_page(&page_id_temp));
    }
    debug_log << "验证无法创建新页面成功" << std::endl;

    // Scenario: After unpinning pages {0, 1, 2, 3, 4} and pinning another 4 new pages,
    // there would still be one cache frame left for reading page 0.
    debug_log << "开始测试页面解锁和重新分配" << std::endl;
    for (int i = 0; i < 5; ++i)
    {
        debug_log << "解锁页面 " << i << std::endl;
        EXPECT_EQ(true, bpm->unpin_page(PageId{fd, i}, true));
    }
    debug_log << "成功解锁5个页面" << std::endl;

    debug_log << "尝试创建4个新页面" << std::endl;
    for (int i = 0; i < 4; ++i)
    {
        debug_log << "创建第 " << i << " 个新页面" << std::endl;
        EXPECT_NE(nullptr, bpm->new_page(&page_id_temp));
    }
    debug_log << "成功创建4个新页面" << std::endl;

    // Scenario: We should be able to fetch the data we wrote a while ago.
    debug_log << "测试读取之前写入的数据" << std::endl;
    page0 = bpm->fetch_page(PageId{fd, 0});
    EXPECT_EQ(0, strcmp(page0->get_data(), "Hello"));
    debug_log << "成功读取页面0的数据" << std::endl;

    EXPECT_EQ(true, bpm->unpin_page(PageId{fd, 0}, true));
    debug_log << "解锁页面0" << std::endl;

    // new_page again, and now all buffers are pinned. Page 0 would be failed to fetch.
    debug_log << "测试所有缓冲区被锁定的情况" << std::endl;
    EXPECT_NE(nullptr, bpm->new_page(&page_id_temp));
    debug_log << "创建新页面成功" << std::endl;

    debug_log << "尝试获取页面0（应该失败）" << std::endl;
    EXPECT_EQ(nullptr, bpm->fetch_page(PageId{fd, 0}));
    debug_log << "验证无法获取页面0成功" << std::endl;

    debug_log << "刷新所有页面到磁盘" << std::endl;
    bpm->flush_all_pages(fd);
    debug_log << "刷新完成" << std::endl;

    debug_log << "=== 缓冲池管理器测试完成 ===" << std::endl;
    debug_log.close();
}

/** 注意：每个测试点只测试了单个文件！
 * 对于每个测试点，先创建和进入目录TEST_DB_NAME
 * 然后在此目录下创建和打开文件TEST_FILE_NAME_CCUR，记录其文件描述符fd */

// Add by jiawen
class BufferPoolManagerConcurrencyTest : public ::testing::Test
{
public:
    std::unique_ptr<DiskManager> disk_manager_;
    int fd_ = -1; // 此文件描述符为disk_manager_->open_file的返回值

public:
    // This function is called before every test.
    void SetUp() override
    {
        ::testing::Test::SetUp();
        // For each test, we create a new DiskManager
        disk_manager_ = std::make_unique<DiskManager>();
        // 如果测试目录不存在，则先创建测试目录
        if (!disk_manager_->is_dir(TEST_DB_NAME))
        {
            disk_manager_->create_dir(TEST_DB_NAME);
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
        // 进入测试目录
        if (chdir(TEST_DB_NAME.c_str()) < 0)
        {
            throw UnixError();
        }
        // 如果测试文件存在，则先删除原文件（最后留下来的文件存的是最后一个测试点的数据）
        if (disk_manager_->is_file(TEST_FILE_NAME_CCUR))
        {
            disk_manager_->destroy_file(TEST_FILE_NAME_CCUR);
        }
        // 创建测试文件
        disk_manager_->create_file(TEST_FILE_NAME_CCUR);
        assert(disk_manager_->is_file(TEST_FILE_NAME_CCUR));
        // 打开测试文件
        fd_ = disk_manager_->open_file(TEST_FILE_NAME_CCUR);
        assert(fd_ != -1);
    }

    // This function is called after every test.
    void TearDown() override
    {
        disk_manager_->close_file(fd_);
        // disk_manager_->destroy_file(TEST_FILE_NAME_CCUR);  // you can choose to delete the file

        // 返回上一层目录
        if (chdir("..") < 0)
        {
            throw UnixError();
        }
        assert(disk_manager_->is_dir(TEST_DB_NAME));
    };
};

TEST_F(BufferPoolManagerConcurrencyTest, ConcurrencyTest)
{
    const int num_threads = 5;
    const int num_runs = 50;

    // get fd
    int fd = BufferPoolManagerConcurrencyTest::fd_;

    for (int run = 0; run < num_runs; run++)
    {
        // create BufferPoolManager
        auto disk_manager = BufferPoolManagerConcurrencyTest::disk_manager_.get();
        std::shared_ptr<BufferPoolManager> bpm{new BufferPoolManager(50, disk_manager)};

        std::vector<std::thread> threads;
        for (int tid = 0; tid < num_threads; tid++)
        {
            threads.push_back(std::thread([&bpm, fd]() { // NOLINT
                PageId temp_page_id = {.fd = fd, .page_no = INVALID_PAGE_ID};
                std::vector<PageId> page_ids;
                for (int i = 0; i < 10; i++)
                {
                    auto new_page = bpm->new_page(&temp_page_id);
                    EXPECT_NE(nullptr, new_page);
                    ASSERT_NE(nullptr, new_page);
                    strcpy(new_page->get_data(), std::to_string(temp_page_id.page_no).c_str()); // NOLINT
                    page_ids.push_back(temp_page_id);
                }
                for (int i = 0; i < 10; i++)
                {
                    EXPECT_EQ(1, bpm->unpin_page(page_ids[i], true));
                }
                for (int j = 0; j < 10; j++)
                {
                    auto page = bpm->fetch_page(page_ids[j]);
                    EXPECT_NE(nullptr, page);
                    ASSERT_NE(nullptr, page);
                    EXPECT_EQ(0, std::strcmp(std::to_string(page_ids[j].page_no).c_str(), (page->get_data())));
                    EXPECT_EQ(1, bpm->unpin_page(page_ids[j], true));
                }
                for (int j = 0; j < 10; j++)
                {
                    EXPECT_EQ(1, bpm->delete_page(page_ids[j]));
                }
                bpm->flush_all_pages(fd); // add this test by jiawen
            }));
        } // end loop tid=[0,num_threads)

        for (int i = 0; i < num_threads; i++)
        {
            threads[i].join();
        }
    } // end loop run=[0,num_runs)
}

TEST(StorageTest, SimpleTest)
{
    std::ofstream debug_log("storage_test.log", std::ios::out | std::ios::app);
    debug_log << "\n=== 开始存储测试 ===" << std::endl;

    try {
        srand((unsigned)time(nullptr));

        /** Test disk_manager */
        std::vector<std::string> filenames(MAX_FILES);
        std::unordered_map<int, std::string> fd2name;
        debug_log << "[Test] 开始磁盘管理器测试，创建" << MAX_FILES << "个文件" << std::endl;

        for (size_t i = 0; i < filenames.size(); i++)
        {
            try {
                auto &filename = filenames[i];
                filename = std::to_string(i) + ".txt";

                if (disk_manager->is_file(filename))
                {
                    disk_manager->destroy_file(filename);
                }

                disk_manager->create_file(filename);
                assert(disk_manager->is_file(filename));

                int fd = disk_manager->open_file(filename);
                char *tmp = new char[PAGE_SIZE * MAX_PAGES];
                if (tmp == nullptr) {
                    throw std::bad_alloc();
                }

                mock[fd] = tmp;
                fd2name[fd] = filename;
                disk_manager->set_fd2pageno(fd, 0);
            } catch (const std::exception &e) {
                debug_log << "[错误] 处理文件" << filenames[i] << "失败: " << e.what() << std::endl;
                throw;
            }
        }

        /** Test buffer_pool_manager*/
        debug_log << "[Test] 开始缓冲池管理器测试" << std::endl;
        int num_pages = 0;
        char init_buf[PAGE_SIZE];

        for (auto &fh : mock)
        {
            try {
                int fd = fh.first;
                for (page_id_t i = 0; i < MAX_PAGES; i++)
                {
                    rand_buf(PAGE_SIZE, init_buf);
                    PageId tmp_page_id = {.fd = fd, .page_no = INVALID_PAGE_ID};
                    Page *page = buffer_pool_manager->new_page(&tmp_page_id);
                    if (page == nullptr) {
                        debug_log << "[错误] 创建页面失败" << std::endl;
                        throw std::runtime_error("Failed to create new page");
                    }

                    int page_no = tmp_page_id.page_no;
                    assert(page_no != INVALID_PAGE_ID);
                    assert(page_no == i);

                    memcpy(page->get_data(), init_buf, PAGE_SIZE);
                    buffer_pool_manager->unpin_page(PageId{fd, page_no}, true);

                    char *mock_buf = mock_get_page(fd, page_no);
                    memcpy(mock_buf, init_buf, PAGE_SIZE);

                    num_pages++;
                    check_cache(fd, page_no);
                }
            } catch (const std::exception &e) {
                debug_log << "[错误] 处理文件描述符" << fh.first << "失败: " << e.what() << std::endl;
                throw;
            }
        }

        debug_log << "[Test] 验证页面数量: " << num_pages << std::endl;
        assert(num_pages == TEST_BUFFER_POOL_SIZE);
        check_cache_all();

        /** Test flush_all_pages() */
        debug_log << "[Test] 开始测试页面刷新" << std::endl;
        for (auto &entry : fd2name)
        {
            int fd = entry.first;
            buffer_pool_manager->flush_all_pages(fd);
            for (int page_no = 0; page_no < MAX_PAGES; page_no++)
            {
                check_disk(fd, page_no);
            }
        }
        check_disk_all();

        debug_log << "[Test] 开始随机操作测试" << std::endl;
        for (int r = 0; r < 10000; r++)
        {
            if (r % 1000 == 0)
            {
                debug_log << "[Test] 执行第" << r << "次随机操作" << std::endl;
            }
            int fd = rand_fd();
            int page_no = rand() % MAX_PAGES;

            Page *page = buffer_pool_manager->fetch_page(PageId{fd, page_no});
            char *mock_buf = mock_get_page(fd, page_no);
            assert(memcmp(page->get_data(), mock_buf, PAGE_SIZE) == 0);

            rand_buf(PAGE_SIZE, init_buf);
            memcpy(page->get_data(), init_buf, PAGE_SIZE);
            memcpy(mock_buf, init_buf, PAGE_SIZE);

            buffer_pool_manager->unpin_page(page->get_page_id(), true);

            if (rand() % 10 == 0)
            {
                buffer_pool_manager->flush_page(page->get_page_id());
                check_disk(fd, page_no);
            }
            if (rand() % 100 == 0)
            {
                buffer_pool_manager->flush_all_pages(fd);
            }
            if (rand() % 100 == 0)
            {
                disk_manager->close_file(fd);
                auto filename = fd2name[fd];
                char *buf = mock[fd];
                fd2name.erase(fd);
                mock.erase(fd);
                int new_fd = disk_manager->open_file(filename);
                mock[new_fd] = buf;
                fd2name[new_fd] = filename;
            }
            check_cache(fd, page_no);
        }
        check_cache_all();

        debug_log << "[Test] 清理测试文件" << std::endl;
        for (auto &entry : fd2name)
        {
            int fd = entry.first;
            auto &filename = entry.second;
            disk_manager->close_file(fd);
            disk_manager->destroy_file(filename);
            delete[] mock[fd];
        }
        debug_log << "=== 存储测试完成 ===" << std::endl;
        debug_log.close();
    } catch (const std::exception &e) {
        debug_log << "[错误] 测试失败: " << e.what() << std::endl;
        debug_log.close();
        throw;
    }
}

TEST(RecordManagerTest, SimpleTest)
{
    std::ofstream debug_log("record_manager_test.log", std::ios::out | std::ios::app);
    debug_log << "=== 开始记录管理器测试 ===" << std::endl;

    srand((unsigned)time(nullptr));

    // 创建RmManager类的对象rm_manager
    auto disk_manager = std::make_unique<DiskManager>();
    auto buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
    auto rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());

    std::unordered_map<Rid, std::string, rid_hash_t, rid_equal_t> mock;

    std::string filename = "abc.txt";
    debug_log << "测试文件名: " << filename << std::endl;

    int record_size = 4 + rand() % 256;
    debug_log << "记录大小: " << record_size << std::endl;

    // test files
    {
        if (disk_manager->is_file(filename))
        {
            disk_manager->destroy_file(filename);
            debug_log << "删除已存在的同名文件" << std::endl;
        }

        rm_manager->create_file(filename, record_size);
        debug_log << "创建新文件" << std::endl;

        std::unique_ptr<RmFileHandle> file_handle = rm_manager->open_file(filename);
        debug_log << "打开文件成功" << std::endl;

        debug_log << "检查文件头信息:" << std::endl;
        debug_log << "- 记录大小: " << file_handle->file_hdr_.record_size << std::endl;
        debug_log << "- 第一个空闲页: " << file_handle->file_hdr_.first_free_page_no << std::endl;
        debug_log << "- 总页数: " << file_handle->file_hdr_.num_pages << std::endl;

        int rand_val = rand();
        file_handle->file_hdr_.num_pages = rand_val;
        debug_log << "设置随机页数: " << rand_val << std::endl;

        rm_manager->close_file(file_handle.get());
        debug_log << "关闭文件" << std::endl;

        // reopen file
        file_handle = rm_manager->open_file(filename);
        debug_log << "重新打开文件" << std::endl;
        debug_log << "验证页数: " << file_handle->file_hdr_.num_pages << std::endl;

        rm_manager->close_file(file_handle.get());
        rm_manager->destroy_file(filename);
        debug_log << "清理测试文件" << std::endl;
    }

    // test pages
    rm_manager->create_file(filename, record_size);
    auto file_handle = rm_manager->open_file(filename);
    debug_log << "开始页面测试" << std::endl;

    char write_buf[PAGE_SIZE];
    size_t add_cnt = 0;
    size_t upd_cnt = 0;
    size_t del_cnt = 0;

    for (int round = 0; round < 1000; round++)
    {
        if (round % 100 == 0)
        {
            debug_log << "\n第 " << round << " 轮操作" << std::endl;
            debug_log << "当前记录数: " << mock.size() << std::endl;
        }

        double insert_prob = 1. - mock.size() / 250.;
        double dice = rand() * 1. / RAND_MAX;
        if (mock.empty() || dice < insert_prob)
        {
            rand_buf(file_handle->file_hdr_.record_size, write_buf);
            Rid rid = file_handle->insert_record(write_buf, nullptr);
            mock[rid] = std::string((char *)write_buf, file_handle->file_hdr_.record_size);
            add_cnt++;
            if (round % 100 == 0)
            {
                debug_log << "插入记录: " << rid << std::endl;
            }
        }
        else
        {
            int rid_idx = rand() % mock.size();
            auto it = mock.begin();
            for (int i = 0; i < rid_idx; i++)
            {
                it++;
            }
            auto rid = it->first;
            if (rand() % 2 == 0)
            {
                rand_buf(file_handle->file_hdr_.record_size, write_buf);
                file_handle->update_record(rid, write_buf, nullptr);
                mock[rid] = std::string((char *)write_buf, file_handle->file_hdr_.record_size);
                upd_cnt++;
                if (round % 100 == 0)
                {
                    debug_log << "更新记录: " << rid << std::endl;
                }
            }
            else
            {
                file_handle->delete_record(rid, nullptr);
                mock.erase(rid);
                del_cnt++;
                if (round % 100 == 0)
                {
                    debug_log << "删除记录: " << rid << std::endl;
                }
            }
        }

        if (round % 50 == 0)
        {
            debug_log << "重新打开文件..." << std::endl;
            rm_manager->close_file(file_handle.get());
            file_handle = rm_manager->open_file(filename);
        }
        check_equal(file_handle.get(), mock);
    }

    debug_log << "\n=== 测试完成 ===" << std::endl;
    debug_log << "总计:" << std::endl;
    debug_log << "- 插入: " << add_cnt << std::endl;
    debug_log << "- 删除: " << del_cnt << std::endl;
    debug_log << "- 更新: " << upd_cnt << std::endl;
    debug_log << "- 最终记录数: " << mock.size() << std::endl;

    rm_manager->close_file(file_handle.get());
    rm_manager->destroy_file(filename);
    debug_log << "清理完成" << std::endl;

    debug_log << "=== 记录管理器测试完成 ===" << std::endl;
    debug_log.close();
}
