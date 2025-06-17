#!/usr/bin/env python3
import time
import subprocess
import os

def generate_sql_files():
    """生成SQL文件"""
    # 阶段1: 创建表并插入数据
    with open("stage1_insert.sql", 'w') as f:
        f.write("CREATE TABLE test_recovery (id INT, value CHAR(8), data CHAR(12));\n\n")
        print("正在生成INSERT语句...")
        for i in range(1, 10001):
            unique_str = f"{i:08d}"[-8:]  # 8位
            data = f"d{i:011d}"[-12:]     # 12位
            f.write(f"INSERT INTO test_recovery VALUES ({i}, '{unique_str}', '{data}');\n")
        print("INSERT SQL文件已生成")

    # 阶段2: 事务操作
    with open("stage2_transaction.sql", 'w') as f:
        # 事务1: 更新前3000条记录
        f.write("BEGIN;\n")
        for i in range(1, 3001):
            new_value = f"u1{i:06d}"[-8:]    # 8位
            new_data = f"d1{i:010d}"[-12:]   # 12位
            f.write(f"UPDATE test_recovery SET value = '{new_value}', data = '{new_data}' WHERE id = {i};\n")
        f.write("COMMIT;\n\n")
        
        # 事务2: 更新中间3000条记录
        f.write("BEGIN;\n")
        for i in range(3001, 6001):
            new_value = f"u2{i:06d}"[-8:]    # 8位
            new_data = f"d2{i:010d}"[-12:]   # 12位
            f.write(f"UPDATE test_recovery SET value = '{new_value}', data = '{new_data}' WHERE id = {i};\n")
        f.write("COMMIT;\n\n")
        
        # 事务3: 删除后2000条记录
        f.write("BEGIN;\n")
        for i in range(8001, 10001):
            f.write(f"DELETE FROM test_recovery WHERE id = {i};\n")
        f.write("COMMIT;\n")
        print("事务SQL文件已生成")

def execute_sql_file(client_path: str, sql_file: str) -> bool:
    """执行SQL文件"""
    print(f"\n执行SQL文件: {sql_file}")
    try:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
            
        result = subprocess.run(
            [client_path],
            input=sql_content,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=300
        )
        
        if result.stderr:
            print("错误输出:", result.stderr)
            
        return True
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
        time.sleep(5)  # 等待数据库启动
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
        time.sleep(2)  # 等待数据库停止
    except Exception as e:
        print(f"发送crash命令失败: {e}")

def cleanup():
    """清理测试文件"""
    try:
        if os.path.exists("test_recovery"):
            os.remove("test_recovery")
        for file in os.listdir("."):
            if file.startswith("test_recovery.") or file.endswith(".log"):
                os.remove(file)
    except Exception as e:
        print(f"警告: 清理文件时发生异常: {e}")

def main():
    client_path = "./rmdb_client/build/rmdb_client"
    
    if not os.path.exists(client_path):
        print(f"错误: 客户端程序不存在 - {client_path}")
        return
    
    try:
        # 1. 清理环境
        cleanup()
        
        # 2. 生成SQL文件
        generate_sql_files()
        
        # 3. 启动数据库
        if not start_database():
            raise Exception("数据库启动失败")
        
        print("\n=== 开始执行测试 ===")
        
        # 4. 执行插入
        if not execute_sql_file(client_path, "stage1_insert.sql"):
            raise Exception("数据插入失败")
        print("数据插入完成")
        
        # 5. 执行更新和删除事务
        if not execute_sql_file(client_path, "stage2_transaction.sql"):
            raise Exception("事务操作失败")
        print("事务操作完成")
        
        # 6. 发送crash命令
        simulate_crash(client_path)
        print("\n测试完成，现在可以手动重启数据库检查恢复结果")
        
    except Exception as e:
        print(f"\n测试中断: {e}")

if __name__ == "__main__":
    main()
