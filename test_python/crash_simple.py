#!/usr/bin/env python3
import time
import subprocess
import os

def generate_sql_files():
    """生成SQL文件"""
    # 阶段1: 创建表并插入数据 (100,000条记录)
    with open("stage1_insert.sql", 'w') as f:
        f.write("CREATE TABLE test_recovery (id INT, value CHAR(8), data CHAR(12));\n\n")
        print("正在生成INSERT语句 (100,000条记录)...")
        for i in range(1, 100001):
            unique_str = f"{i:08d}"[-8:]  # 8位
            data = f"d{i:011d}"[-12:]     # 12位
            f.write(f"INSERT INTO test_recovery VALUES ({i}, '{unique_str}', '{data}');\n")
            
            # 每10000条记录显示进度
            if i % 10000 == 0:
                print(f"已生成 {i} 条INSERT语句")
                
        print("INSERT SQL文件已生成")

    # 阶段2: 事务操作 (更大规模的事务)
    with open("stage2_transaction.sql", 'w') as f:
        print("正在生成事务操作SQL...")
        
        # 事务1: 更新前30000条记录
        for i in range(1, 30001):
            if i % 1000 == 1:
                f.write("BEGIN;\n")
            new_value = f"u1{i:06d}"[-8:]    # 8位
            new_data = f"d1{i:010d}"[-12:]   # 12位
            f.write(f"UPDATE test_recovery SET value = '{new_value}', data = '{new_data}' WHERE id = {i};\n")
            if i % 1000 == 0:
                f.write("COMMIT;\n")
        print("事务1 (更新前30000条) 已生成")
        
        # 事务2: 更新中间30000条记录
        for i in range(30001, 60001):
            if i % 1000 == 1:
                f.write("BEGIN;\n")
            new_value = f"u2{i:06d}"[-8:]    # 8位
            new_data = f"d2{i:010d}"[-12:]   # 12位
            f.write(f"UPDATE test_recovery SET value = '{new_value}', data = '{new_data}' WHERE id = {i};\n")
            if i % 1000 == 0:
                f.write("COMMIT;\n")
        print("事务2 (更新中间30000条) 已生成")
        
        # 事务3: 更新另外20000条记录
        for i in range(60001, 80001):
            if i % 1000 == 1:
                f.write("BEGIN;\n")
            new_value = f"u3{i:06d}"[-8:]    # 8位
            new_data = f"d3{i:010d}"[-12:]   # 12位
            f.write(f"UPDATE test_recovery SET value = '{new_value}', data = '{new_data}' WHERE id = {i};\n")
            if i % 1000 == 0:
                f.write("COMMIT;\n")
        print("事务3 (更新另外20000条) 已生成")
        
        # 事务4: 删除后20000条记录
        for i in range(80001, 100001):
            if i % 1000 == 1:
                f.write("BEGIN;\n")
            f.write(f"DELETE FROM test_recovery WHERE id = {i};\n")
            if i % 1000 == 0:
                f.write("COMMIT;\n")
        print("事务4 (删除后20000条) 已生成")
        
        print("事务SQL文件已生成")

def execute_sql_file(client_path: str, sql_file: str) -> bool:
    """执行SQL文件"""
    print(f"\n执行SQL文件: {sql_file}")
    try:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
            
        print(f"SQL文件大小: {len(sql_content)} 字符")
        
        result = subprocess.run(
            [client_path],
            input=sql_content,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=600  # 增加超时时间到10分钟
        )
        
        if result.stderr:
            print("错误输出:", result.stderr)
            
        if result.stdout:
            print("标准输出:", result.stdout[:500])  # 只显示前500字符
            
        return True
    except subprocess.TimeoutExpired:
        print(f"执行SQL文件超时: {sql_file}")
        return False
    except Exception as e:
        print(f"执行SQL文件失败: {e}")
        return False

def start_database():
    """启动数据库"""
    print("\n=== 启动数据库 ===")
    try:
        process = subprocess.Popen(
            ["./bin/rmdb", "test"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        time.sleep(8)  # 增加等待时间，确保数据库完全启动
        return True
    except Exception as e:
        print(f"错误: 启动数据库失败: {e}")
        return False

def simulate_crash(client_path: str):
    """模拟数据库崩溃"""
    print("\n=== 发送crash命令 ===")
    try:
        subprocess.run(
            [client_path],
            input="crash",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30
        )
        print("已发送crash命令")
        time.sleep(3)  # 等待数据库停止
    except Exception as e:
        print(f"发送crash命令失败: {e}")

def cleanup():
    """清理测试文件"""
    try:
        files_to_remove = []
        if os.path.exists("test_recovery"):
            files_to_remove.append("test_recovery")
            
        for file in os.listdir("."):
            if file.startswith("test_recovery.") or file.endswith(".log"):
                files_to_remove.append(file)
                
        for file in files_to_remove:
            try:
                os.remove(file)
                print(f"已删除文件: {file}")
            except:
                pass
                
    except Exception as e:
        print(f"警告: 清理文件时发生异常: {e}")

def verify_data_count(client_path: str):
    """验证数据条数"""
    print("\n=== 验证数据条数 ===")
    try:
        result = subprocess.run(
            [client_path],
            input="SELECT COUNT(*) FROM test_recovery;",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60
        )
        
        if result.stdout:
            print("当前表中记录数:", result.stdout.strip())
        if result.stderr:
            print("查询错误:", result.stderr)
            
    except Exception as e:
        print(f"验证数据条数失败: {e}")

def main():
    client_path = "./rmdb_client/build/rmdb_client"
    
    if not os.path.exists(client_path):
        print(f"错误: 客户端程序不存在 - {client_path}")
        return
    
    try:
        # 1. 清理环境
        print("=== 清理测试环境 ===")
        cleanup()
        
        # 2. 生成SQL文件
        print("\n=== 生成SQL文件 ===")
        generate_sql_files()
        
        # 3. 启动数据库
        # if not start_database():
        #     raise Exception("数据库启动失败")
        
        print("\n=== 开始执行测试 (10万条数据) ===")
        
        # 4. 执行插入
        print("\n--- 阶段1: 插入100,000条记录 ---")
        start_time = time.time()
        if not execute_sql_file(client_path, "stage1_insert.sql"):
            raise Exception("数据插入失败")
        insert_time = time.time() - start_time
        print(f"数据插入完成，耗时: {insert_time:.2f}秒")
        
        # 验证插入结果
        verify_data_count(client_path)
        
        # 5. 执行更新和删除事务
        print("\n--- 阶段2: 执行大规模事务操作 ---")
        start_time = time.time()
        if not execute_sql_file(client_path, "stage2_transaction.sql"):
            raise Exception("事务操作失败")
        transaction_time = time.time() - start_time
        print(f"事务操作完成，耗时: {transaction_time:.2f}秒")
        
        # 验证事务结果
        verify_data_count(client_path)
        
        # 6. 发送crash命令
        simulate_crash(client_path)
        
        print(f"\n=== 测试完成 ===")
        print(f"总数据量: 100,000条记录")
        print(f"事务操作: 更新80,000条 + 删除20,000条")
        print(f"预期最终记录数: 80,000条")
        print("现在可以手动重启数据库检查恢复结果")
        
    except Exception as e:
        print(f"\n测试中断: {e}")
    finally:
        print("\n=== 清理SQL文件 ===")
        try:
            if os.path.exists("stage1_insert.sql"):
                os.remove("stage1_insert.sql")
            if os.path.exists("stage2_transaction.sql"):
                os.remove("stage2_transaction.sql")
            print("SQL文件已清理")
        except:
            pass

if __name__ == "__main__":
    main()