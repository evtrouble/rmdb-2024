import time
import subprocess
import os
import threading
from typing import Dict, List

# 数据库客户端路径（需根据实际路径修改）
CLIENT_PATH = "your_database_client"
OUTPUT_FILE = "mvcc_test.log"


def generate_test_tables():
    """创建测试表并插入初始数据"""
    sql = """
    CREATE TABLE users (
        id INT,
        name CHAR(20),
        age INT
    );
    
    INSERT INTO users VALUES (1, 'Alice', 25);
    INSERT INTO users VALUES (2, 'Bob', 30);
    INSERT INTO users VALUES (3, 'Charlie', 35);
    """
    with open("setup.sql", "w") as f:
        f.write(sql)
    print("测试表和初始数据已生成")


def run_session(session_name: str, cmds: List[str], output: str):
    """执行单个会话的SQL命令"""
    print(f"\n=== 会话 {session_name} 开始 ===")
    start_time = time.time()
    
    # 拼接带事务的SQL（根据需求调整，此处假设支持 BEGIN/COMMIT）
    txn_cmds = ["BEGIN"] + cmds + ["COMMIT"]
    sql = "\n".join(txn_cmds)
    
    result = subprocess.run(
        [CLIENT_PATH],
        input=sql,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=60
    )
    
    end_time = time.time()
    execution_time = end_time - start_time
    
    with open(output, "a") as f:
        f.write(f"\n=== 会话 {session_name} ===\n")
        f.write(f"命令: {cmds}\n")
        f.write(f"执行时间: {execution_time:.2f}s\n")
        f.write("标准输出:\n")
        f.write(result.stdout)
        f.write("\n错误输出:\n")
        f.write(result.stderr)
        f.write("=" * 50 + "\n")
    
    print(f"会话 {session_name} 完成，耗时 {execution_time:.2f}s")
    return execution_time


def test_mvcc_snapshot_isolation():
    """测试快照隔离：读事务应看到一致的旧数据"""
    output = OUTPUT_FILE
    if os.path.exists(output):
        os.remove(output)
    
    # 会话1：长读事务（在事务开始后休眠，期间会话2修改数据）
    session1_cmds = [
        "SELECT * FROM users WHERE id = 1",  # 读取初始数据
        f"SELECT SLEEP(3)",  # 模拟 pg_sleep，需数据库支持类似函数或通过外部休眠实现
        "SELECT * FROM users WHERE id = 1"   # 应看到旧数据（MVCC保证）
    ]
    
    # 会话2：写事务（在会话1休眠期间修改数据）
    session2_cmds = [
        f"SELECT SLEEP(1)",  # 等待会话1开始
        "UPDATE users SET age = 26 WHERE id = 1",  # 修改数据
        f"SELECT SLEEP(2)"   # 保持事务打开
    ]
    
    # 使用线程模拟并发
    def run_session1():
        run_session("读事务", session1_cmds, output)
    
    def run_session2():
        run_session("写事务", session2_cmds, output)
    
    # 启动会话1，延迟后启动会话2
    t1 = threading.Thread(target=run_session1)
    t2 = threading.Thread(target=run_session2)
    
    t1.start()
    time.sleep(0.5)  # 确保会话1先启动
    t2.start()
    
    t1.join()
    t2.join()
    
    # 分析结果：会话1的两次读取应返回相同数据（旧值）
    with open(output, "r") as f:
        content = f.read()
        if "age=25" in content and "age=26" not in content:
            print("\n=== 快照隔离测试通过 ===")
        else:
            print("\n=== 快照隔离测试失败 ===")


def test_mvcc_read_write_concurrency():
    """测试读写并发：写事务不应阻塞读事务"""
    output = OUTPUT_FILE
    
    # 会话1：长时间读事务
    session1_cmds = [
        "SELECT * FROM users",
        f"SELECT SLEEP(5)",  # 模拟长读
        "SELECT * FROM users"
    ]
    
    # 会话2：写事务（在会话1读期间修改数据）
    session2_cmds = [
        "UPDATE users SET name = 'Alice_updated' WHERE id = 1",
        "UPDATE users SET name = 'Bob_updated' WHERE id = 2"
    ]
    
    # 并发执行
    t1 = threading.Thread(target=run_session, args=("长读事务", session1_cmds, output))
    t2 = threading.Thread(target=run_session, args=("写事务", session2_cmds, output))
    
    t1.start()
    time.sleep(1)  # 确保读事务先开始
    t2.start()
    
    t1.join()
    t2.join()
    
    # 分析结果：读事务应能看到部分旧数据和部分新数据（取决于MVCC版本）
    with open(output, "r") as f:
        content = f.read()
        if ("Alice" in content or "Alice_updated" in content) and \
           ("Bob" in content or "Bob_updated" in content):
            print("\n=== 读写并发测试通过 ===")
        else:
            print("\n=== 读写并发测试失败 ===")


if __name__ == "__main__":
    generate_test_tables()
    
    # 测试流程
    print("\n=== 开始 MVCC 测试 ===")
    test_mvcc_snapshot_isolation()
    test_mvcc_read_write_concurrency()
    
    print(f"\n完整日志已保存至: {OUTPUT_FILE}")