#!/usr/bin/env python3
import time
import subprocess
import os
import threading
from typing import List, Tuple

DB_CLIENT = "rmdb_client/build/rmdb_client"  # 替换为实际客户端路径
OUTPUT_FILE = "mvcc_test.log"
TMP_SQL_FILE = "tmp.sql"

def create_test_table():
    """创建测试表并插入初始数据"""
    sql = """
    CREATE TABLE mvcc_test (id INT,val INT,name CHAR(15));
    
    INSERT INTO mvcc_test VALUES (1, 100, 'data_00000001');
    INSERT INTO mvcc_test VALUES (2, 200, 'data_00000002');
    """
    with open(TMP_SQL_FILE, 'w') as f:
        f.write(sql)
    subprocess.run([DB_CLIENT], input=sql, text=True)
    print("测试表和初始数据已创建")

def run_session(session_name: str, cmds: List[str]) -> str:
    """执行单个会话的SQL命令并返回输出"""
    sql = "\n".join(cmds)
    result = subprocess.run(
        [DB_CLIENT],
        input=sql,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30
    )
    output = result.stdout + result.stderr
    with open(OUTPUT_FILE, "a") as f:
        f.write(f"\n=== {session_name} ===\n{sql}\n{output}\n")
    
    # 检查错误输出
    if "ERROR" in output or "abort" in output.lower():
        print(f"警告: {session_name} 执行时出现错误: {output}")
    return output

def test_snapshot_isolation():
    """测试快照隔离：事务内两次读取结果一致"""
    print("\n=== 开始快照隔离测试 ===")
    
    # 使用 Event 精确控制线程同步
    first_read_done = threading.Event()
    update_done = threading.Event()
    
    # 会话1：读取数据并等待，期间会话2修改数据
    def session1():
        return run_session("读事务", [
            "BEGIN;",
            "SELECT val FROM mvcc_test WHERE id = 1;",  # 第一次读取
            "SELECT val FROM mvcc_test WHERE id = 1;",  # 第二次读取
            "COMMIT;"
        ])
    
    # 会话2：在会话1两次读取之间修改数据
    def session2():
        first_read_done.wait()  # 等待会话1第一次读取完成
        run_session("写事务", [
            "BEGIN;",
            "UPDATE mvcc_test SET val = 150 WHERE id = 1;",
            "COMMIT;"
        ])
        update_done.set()  # 通知会话1更新已完成
    
    t1 = threading.Thread(target=session1)
    t2 = threading.Thread(target=session2)
    
    t1.start()
    time.sleep(0.5)  # 给会话1一些时间开始
    first_read_done.set()  # 通知会话2可以开始更新
    update_done.wait()  # 等待会话2完成更新
    t1.join()
    
    output = session1()
    
    # 验证两次读取均为初始值100（未看到会话2的修改）
    if "100" in output and output.count("100") == 2:
        print("快照隔离测试通过：两次读取结果一致")
    else:
        print("快照隔离测试失败：数据不一致")

def test_read_write_concurrency():
    """测试读写并发：写事务不阻塞读事务"""
    print("\n=== 开始读写并发测试 ===")
    
    # 使用 Event 控制同步
    read_started = threading.Event()
    read_complete = threading.Event()
    
    # 会话1：长时间读事务（模拟查询）
    def session1():
        output = run_session("长读事务", [
            "BEGIN;",
            "SELECT * FROM mvcc_test;",  # 模拟长查询
            "COMMIT;"
        ])
        read_complete.set()  # 通知读事务已完成
        return output
    
    # 会话2：在会话1执行期间修改数据
    def session2():
        read_started.wait()  # 等待读事务开始
        run_session("写事务", [
            "BEGIN;",
            "UPDATE mvcc_test SET val = 250 WHERE id = 2;",
            "COMMIT;"
        ])
    
    t1 = threading.Thread(target=session1)
    t2 = threading.Thread(target=session2)
    
    t1.start()
    time.sleep(0.5)  # 给读事务一些时间开始
    read_started.set()  # 通知写事务可以开始
    t2.start()
    t1.join()
    t2.join()
    
    # 验证写事务是否成功（最终val应为250）
    final_val = subprocess.run(
        [DB_CLIENT],
        input="SELECT val FROM mvcc_test WHERE id = 2;",
        stdout=subprocess.PIPE,
        text=True
    ).stdout
    
    if "250" in final_val:
        print("读写并发测试通过：写操作未被阻塞")
    else:
        print("读写并发测试失败：写操作被读事务阻塞")

def test_write_write_conflict():
    """测试写-写冲突：后提交事务应回滚"""
    print("\n=== 开始写-写冲突测试 ===")
    
    # 使用 Event 精确控制事务顺序
    update1_started = threading.Event()
    update2_started = threading.Event()
    commit1_ready = threading.Event()
    
    # 会话1：更新id=1，保持事务打开，等待会话2尝试更新
    def session1():
        output = run_session("写事务1", [
            "BEGIN;",
            "UPDATE mvcc_test SET val = 120 WHERE id = 1;"
        ])
        update1_started.set()  # 通知会话1已开始更新
        commit1_ready.wait()   # 等待继续执行的信号
        
        # 尝试提交
        commit_output = run_session("写事务1", ["COMMIT;"])
        return output + commit_output
    
    # 会话2：在会话1未提交时并发更新id=1
    def session2():
        update1_started.wait()  # 等待会话1开始更新
        time.sleep(0.5)         # 确保会话1已获取锁
        
        output = run_session("写事务2", [
            "BEGIN;",
            "UPDATE mvcc_test SET val = 130 WHERE id = 1;"
        ])
        update2_started.set()  # 通知会话2已开始更新
        
        # 尝试提交（此时应检测到冲突）
        commit_output = run_session("写事务2", ["COMMIT;"])
        return output + commit_output
    
    t1 = threading.Thread(target=session1)
    t2 = threading.Thread(target=session2)
    
    t1.start()
    t2.start()
    
    # 等待两个事务都开始更新
    update1_started.wait()
    update2_started.wait()
    
    # 允许会话1提交
    commit1_ready.set()
    
    t1.join()
    t2.join()
    
    # 验证最终值：假设会话1先提交，会话2应回滚，最终值为120
    final_val = subprocess.run(
        [DB_CLIENT],
        input="SELECT val FROM mvcc_test WHERE id = 1;",
        stdout=subprocess.PIPE,
        text=True
    ).stdout
    
    if "120" in final_val:
        print("写-写冲突测试通过：后提交事务回滚")
    else:
        print(f"写-写冲突测试失败：最终值为 {final_val.strip()}")

def cleanup():
    """清理测试环境"""
    subprocess.run([DB_CLIENT], input="DROP TABLE mvcc_test;", text=True)
    if os.path.exists(TMP_SQL_FILE):
        os.remove(TMP_SQL_FILE)

if __name__ == "__main__":
    if os.path.exists(OUTPUT_FILE):
        os.remove(OUTPUT_FILE)
    
    create_test_table()
    
    test_snapshot_isolation()
    test_read_write_concurrency()
    test_write_write_conflict()
    
    cleanup()
    print(f"\n测试结果已保存至: {OUTPUT_FILE}")