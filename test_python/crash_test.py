#!/usr/bin/env python3
import time
import subprocess
import os
import signal
import sys
from typing import List, Dict, Tuple

def generate_sql_files():
    """生成多个阶段的SQL文件"""
    # 阶段1: 创建表并插入初始数据
    with open("stage1_insert.sql", 'w') as f:
        f.write("CREATE TABLE test_recovery (id INT, value CHAR(8), data CHAR(12));\n\n")
        print("正在生成INSERT语句...")
        for i in range(1, 10001):
            unique_str = f"{i:08d}"[-8:]  # 8位字符
            data = f"d{i:011d}"[-12:]     # 12位字符
            f.write(f"INSERT INTO test_recovery VALUES ({i}, '{unique_str}', '{data}');\n")
        print(f"INSERT SQL文件已生成: stage1_insert.sql")

    # 阶段2: 事务操作（更新和删除）
    with open("stage2_transaction.sql", 'w') as f:
        # 事务1: 更新前3000条记录
        f.write("BEGIN;\n")
        for i in range(1, 3001):
            new_value = f"u1{i:06d}"[-8:]     # 8位: u1 + 6位数字
            new_data = f"d1{i:010d}"[-12:]    # 12位: d1 + 10位数字
            f.write(f"UPDATE test_recovery SET value = '{new_value}', data = '{new_data}' WHERE id = {i};\n")
        f.write("COMMIT;\n\n")
        
        # 事务2: 更新中间3000条记录
        f.write("BEGIN;\n")
        for i in range(3001, 6001):
            new_value = f"u2{i:06d}"[-8:]     # 8位: u2 + 6位数字
            new_data = f"d2{i:010d}"[-12:]    # 12位: d2 + 10位数字
            f.write(f"UPDATE test_recovery SET value = '{new_value}', data = '{new_data}' WHERE id = {i};\n")
        f.write("COMMIT;\n\n")
        
        # 事务3: 删除后2000条记录
        f.write("BEGIN;\n")
        for i in range(8001, 10001):
            f.write(f"DELETE FROM test_recovery WHERE id = {i};\n")
        f.write("COMMIT;\n")
        print(f"事务操作SQL文件已生成: stage2_transaction.sql")

    # 阶段3: 检查点与崩溃前验证查询
    with open("stage3_pre_crash_query.sql", 'w') as f:
        # 简单的记录计数查询
        f.write("SELECT COUNT(*) as total_count FROM test_recovery;\n")
        # 检查更新的记录范围
        f.write("SELECT COUNT(*) as updated_range1 FROM test_recovery WHERE id <= 3000;\n")
        f.write("SELECT COUNT(*) as updated_range2 FROM test_recovery WHERE id > 3000 AND id <= 6000;\n")
        # 检查剩余记录
        f.write("SELECT COUNT(*) as remaining_count FROM test_recovery WHERE id > 6000;\n")
        print(f"验证查询SQL文件已生成: stage3_pre_crash_query.sql")

def execute_sql_file(client_path: str, sql_file: str, timeout: int = 300) -> Tuple[bool, float]:
    """执行SQL文件并返回执行状态和耗时"""
    print(f"\n执行SQL文件: {sql_file}")
    start_time = time.time()
    try:
        result = subprocess.run(
            [client_path],
            input=open(sql_file, 'r').read(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout
        )
        execution_time = time.time() - start_time
        print(f"执行时间: {execution_time:.2f} 秒")
        if result.stderr:
            print("错误输出:", result.stderr)
        return True, execution_time
    except Exception as e:
        print(f"执行SQL文件失败: {e}")
        return False, time.time() - start_time

def verify_data_consistency(client_path: str, output_file: str) -> bool:
    """验证数据一致性"""
    print("\n=== 验证数据一致性 ===")
    try:
        result = subprocess.run(
            [client_path],
            input=open("stage3_pre_crash_query.sql", 'r').read(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60
        )

        # 解析结果
        lines = result.stdout.strip().split('\n')
        verification_results = {}
        
        # 预期结果
        expected = {
            'total_count': 8000,      # 10000 - 2000(deleted)
            'updated_range1': 3000,    # 第一个更新范围(1-3000)
            'updated_range2': 3000,    # 第二个更新范围(3001-6000)
            'remaining_count': 2000    # 剩余记录(6001-8000)
        }
        
        # 解析查询结果 - 适应新的输出格式
        # 数据格式是每两行一组：第一行是名称，第二行是值
        i = 0
        while i < len(lines):
            header = lines[i].strip('| ').strip()
            if (i + 1) < len(lines):
                value = lines[i + 1].strip('| ').strip()
                try:
                    verification_results[header] = int(value)
                except ValueError:
                    print(f"警告: 无法解析值: {value}")
            i += 2
        
        # 记录验证结果
        with open(output_file, 'a') as f:
            f.write("\n=== 数据一致性验证结果 ===\n")
            f.write("预期结果:\n")
            for key, value in expected.items():
                f.write(f"{key}: {value}\n")
            
            f.write("\n实际结果:\n")
            for key, value in verification_results.items():
                f.write(f"{key}: {value}\n")
            
            # 验证是否符合预期
            all_matched = True
            for key in expected:
                if key in verification_results:
                    if expected[key] != verification_results[key]:
                        all_matched = False
                        f.write(f"\n不匹配: {key}")
                        f.write(f"\n  预期: {expected[key]}")
                        f.write(f"\n  实际: {verification_results[key]}\n")
                else:
                    all_matched = False
                    f.write(f"\n缺少结果: {key}\n")
            
            f.write(f"\n验证结果: {'通过' if all_matched else '失败'}\n")
            f.write("="*50 + "\n")
            
        return all_matched
        
    except Exception as e:
        print(f"错误: 验证数据时发生异常: {str(e)}")
        return False

def start_database() -> bool:
    """启动数据库并等待其就绪"""
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
        print(f"错误: 启动数据库失败: {str(e)}")
        return False

def stop_database(client_path: str):
    """优雅地停止数据库"""
    print("\n=== 停止数据库 ===")
    try:
        # 首先尝试正常关闭
        subprocess.run(
            [client_path],
            input="quit",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=30
        )
    except Exception:
        # 如果正常关闭失败，强制终止进程
        try:
            ps = subprocess.run(["pgrep", "rmdb"], stdout=subprocess.PIPE, text=True)
            if ps.stdout:
                pid = int(ps.stdout.strip())
                os.kill(pid, signal.SIGKILL)
                print(f"已强制终止数据库进程 (PID: {pid})")
        except Exception as e:
            print(f"错误: 停止数据库时发生异常: {str(e)}")
    time.sleep(2)  # 等待进程完全终止

def simulate_crash(client_path: str):
    """模拟数据库崩溃"""
    print("\n=== 模拟数据库崩溃 ===")
    try:
        result = subprocess.run(
            [client_path],
            input="crash",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30
        )
        print("已发送crash命令")
        time.sleep(2)  # 等待数据库完全停止
        return True
    except Exception as e:
        print(f"错误: 模拟崩溃失败: {str(e)}")
        return False

def measure_recovery_time() -> float:
    """测量数据库恢复时间"""
    print("\n=== 测量恢复时间 ===")
    start_time = time.time()
    
    try:
        # 启动数据库进程
        process = subprocess.Popen(
            ["./bin/rmdb", "test"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1  # 行缓冲
        )
        
        # 设置超时时间
        timeout = 300  # 5分钟超时
        recovery_found = False
        
        # 非阻塞地检查输出
        while time.time() - start_time < timeout:
            # 检查stderr
            for line in process.stderr:
                if "recovery completed" in line.lower() or "recovery done" in line.lower():
                    recovery_found = True
                    break
            
            if recovery_found:
                break
                
            # 检查进程是否还活着
            if process.poll() is not None:
                print("警告: 数据库进程已终止")
                break
                
            time.sleep(0.1)  # 短暂休眠，避免CPU过度使用
        
        recovery_time = time.time() - start_time
        
        if recovery_found:
            print(f"数据库恢复完成，耗时: {recovery_time:.2f} 秒")
        else:
            print("警告: 未检测到恢复完成信号，但数据库已启动")
            
        # 确保数据库正在运行
        time.sleep(2)  # 等待数据库完全就绪
        return recovery_time
    
    except Exception as e:
        print(f"错误: 测量恢复时间时发生异常: {str(e)}")
        return -1

def cleanup():
    """清理测试数据"""
    try:
        if os.path.exists("test_recovery"):
            os.remove("test_recovery")
        for file in os.listdir("."):
            if file.startswith("test_recovery.") or file.endswith(".log"):
                os.remove(file)
    except Exception as e:
        print(f"警告: 清理文件时发生异常: {str(e)}")

def main():
    # 初始化
    client_path = "./rmdb_client/build/rmdb_client"
    output_file = "crash_test_results.txt"
    
    if not os.path.exists(client_path):
        print(f"错误: 客户端程序不存在 - {client_path}")
        return
    
    # 清理旧文件
    cleanup()
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # 记录测试开始
    start_time = time.time()
    with open(output_file, 'w') as f:
        f.write("=== 数据库崩溃恢复测试 ===\n")
        f.write(f"开始时间: {time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("="*50 + "\n")
    
    try:
        # 1. 生成测试SQL文件
        generate_sql_files()
        
        # 2. 启动数据库
        if not start_database():
            raise Exception("数据库启动失败")
        
        # 3. 执行初始数据插入
        success, insert_time = execute_sql_file(client_path, "stage1_insert.sql")
        if not success:
            raise Exception("数据插入失败")
        
        # 4. 执行事务操作
        success, transaction_time = execute_sql_file(client_path, "stage2_transaction.sql")
        if not success:
            raise Exception("事务操作失败")
        
        # 5. 崩溃前验证
        pre_crash_consistency = verify_data_consistency(client_path, output_file)
        if not pre_crash_consistency:
            print("警告: 崩溃前数据一致性验证失败")
        
        # 6. 模拟崩溃
        if not simulate_crash(client_path):
            raise Exception("模拟崩溃失败")
        
        # 7. 测量恢复时间
        recovery_time = measure_recovery_time()
        if recovery_time < 0:
            raise Exception("恢复过程失败")
        
        # 8. 验证恢复后的数据一致性
        post_crash_consistency = verify_data_consistency(client_path, output_file)
        
        # 9. 生成最终报告
        total_time = time.time() - start_time
        with open(output_file, 'a') as f:
            f.write("\n=== 测试总结 ===\n")
            f.write(f"总执行时间: {total_time:.2f} 秒\n")
            f.write(f"数据插入时间: {insert_time:.2f} 秒\n")
            f.write(f"事务操作时间: {transaction_time:.2f} 秒\n")
            f.write(f"恢复时间: {recovery_time:.2f} 秒\n")
            f.write(f"崩溃前一致性: {'通过' if pre_crash_consistency else '失败'}\n")
            f.write(f"恢复后一致性: {'通过' if post_crash_consistency else '失败'}\n")
            f.write("="*50 + "\n")
        
        # 10. 停止数据库
        stop_database(client_path)
        
        # 11. 输出最终结果
        if post_crash_consistency:
            print("\n✅ 崩溃恢复测试通过")
            print(f"- 总耗时: {total_time:.2f} 秒")
            print(f"- 恢复耗时: {recovery_time:.2f} 秒")
        else:
            print("\n❌ 崩溃恢复测试失败")
        
        print(f"\n详细测试报告已保存至: {output_file}")
    
    except Exception as e:
        print(f"\n测试过程中断: {str(e)}")
        stop_database(client_path)
    
    finally:
        cleanup()

if __name__ == "__main__":
    main()
