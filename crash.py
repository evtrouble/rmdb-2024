#!/usr/bin/env python3
import time
import subprocess
import os
from typing import List, Dict, Tuple

def generate_sql_files():
    """生成多个阶段的SQL文件"""
    # 阶段1: 创建表并插入初始数据
    with open("stage1_insert.sql", 'w') as f:
        f.write("CREATE TABLE test_table (id INT, value CHAR(8));\n\n")
        print("正在生成INSERT语句...")
        for i in range(1, 101):
            unique_str = f"{i:08d}"[-8:]
            f.write(f"INSERT INTO test_table VALUES ({i}, '{unique_str}');\n")
        print(f"INSERT SQL文件已生成: stage1_insert.sql")

    # 阶段2: 事务操作（插入、更新、删除）
    with open("stage2_transaction.sql", 'w') as f:
        f.write("begin;\n")
        for i in range(101, 201):
            unique_str = f"{i:08d}"[-8:]
            f.write(f"INSERT INTO test_table VALUES ({i}, '{unique_str}');\n")
        for i in range(1, 51):
            f.write(f"UPDATE test_table SET value = 'updated' WHERE id = {i};\n")
        for i in range(51, 101):
            f.write(f"DELETE FROM test_table WHERE id = {i};\n")
        f.write("commit;\n")
        print(f"事务操作SQL文件已生成: stage2_transaction.sql")

    # 阶段3: 模拟崩溃前的查询
    with open("stage3_pre_crash_query.sql", 'w') as f:
        f.write("SELECT * FROM test_table;\n")
        print(f"崩溃前查询SQL文件已生成: stage3_pre_crash_query.sql")

    # 阶段4: 模拟崩溃
    with open("stage4_crash.sql", 'w') as f:
        f.write("crash\n")
        print(f"崩溃模拟SQL文件已生成: stage4_crash.sql")

    # 阶段5: 崩溃后恢复查询
    with open("stage5_post_crash_query.sql", 'w') as f:
        f.write("SELECT * FROM test_table;\n")
        print(f"崩溃后查询SQL文件已生成: stage5_post_crash_query.sql")

def run_individual_queries(client_path: str, sql_file: str, output_file: str, stage_name: str) -> List[float]:
    """逐行执行SQL查询并记录每个查询的执行时间（支持增删改查）"""
    print(f"\n=== 开始执行阶段: {stage_name} (逐行操作) ===")

    if not os.path.exists(sql_file):
        print(f"错误: SQL文件不存在 - {sql_file}")
        return []

    query_times = []

    try:
        with open(sql_file, 'r') as f:
            queries = f.readlines()

        queries = [q.strip() for q in queries if q.strip() and not q.strip().startswith('--')]
        total_queries = len(queries)
        print(f"准备执行 {total_queries} 个操作")

        with open(output_file, 'a') as out_f:
            out_f.write(f"\n=== 阶段: {stage_name} ===\n")

            for i, query in enumerate(queries):
                start_time = time.time()

                result = subprocess.run(
                    [client_path],
                    input=query,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=30  # 单个操作超时时间
                )

                end_time = time.time()
                execution_time = end_time - start_time
                query_times.append(execution_time)

                if (i + 1) % 50 == 0 or (i + 1) == total_queries:
                    print(f"已执行 {i+1}/{total_queries} 个操作")

                out_f.write(f"操作 {i+1}: {query}\n")
                out_f.write(f"执行时间: {execution_time:.6f} 秒\n")
                out_f.write(f"输出: {result.stdout.strip()}\n")
                if result.stderr:
                    out_f.write(f"错误: {result.stderr.strip()}\n")
                out_f.write("-"*50 + "\n")

            if query_times:
                avg_time = sum(query_times) / len(query_times)
                min_time = min(query_times)
                max_time = max(query_times)

                out_f.write(f"\n=== {stage_name} 统计信息 ===\n")
                out_f.write(f"总操作数: {len(query_times)}\n")
                out_f.write(f"总执行时间: {sum(query_times):.2f} 秒\n")
                out_f.write(f"平均执行时间: {avg_time:.6f} 秒\n")
                out_f.write(f"最小执行时间: {min_time:.6f} 秒\n")
                out_f.write(f"最大执行时间: {max_time:.6f} 秒\n")
                out_f.write("="*50 + "\n")

            print(f"阶段 {stage_name} 完成")
            print(f"执行 {len(query_times)} 个操作，总时间: {sum(query_times):.2f} 秒")

    except subprocess.TimeoutExpired:
        print(f"错误: 操作执行超时 (30秒)")
    except Exception as e:
        print(f"错误: 执行阶段 {stage_name} 时发生异常: {str(e)}")

    return query_times

def run_stage(client_path: str, stage_name: str, sql_file: str, output_file: str) -> float:
    """执行批量操作阶段并返回执行时间"""
    print(f"\n=== 开始执行阶段: {stage_name} ===")

    try:
        start_time = time.time()

        result = subprocess.run(
            [client_path],
            input=open(sql_file, 'r').read(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=1800  # 阶段超时时间
        )

        end_time = time.time()
        execution_time = end_time - start_time

        print(f"阶段 {stage_name} 完成")
        print(f"执行时间: {execution_time:.2f} 秒")

        with open(output_file, 'a') as f:
            f.write(f"\n=== 阶段: {stage_name} ===\n")
            f.write(f"执行时间: {execution_time:.2f} 秒\n\n")
            f.write("标准输出:\n")
            f.write(result.stdout)
            f.write("\n错误输出:\n")
            f.write(result.stderr)
            f.write("\n" + "="*50 + "\n")

        return execution_time

    except subprocess.TimeoutExpired:
        print(f"错误: 阶段 {stage_name} 执行超时 (1800秒)")
        return -1
    except Exception as e:
        print(f"错误: 执行阶段 {stage_name} 时发生异常: {str(e)}")
        return -1

def start_database():
    """启动数据库"""
    print("\n=== 启动数据库 ===")
    try:
        subprocess.Popen(["bin/rmdb", "test"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        print("数据库已启动")
        time.sleep(10)  # 等待数据库完全启动
    except Exception as e:
        print(f"错误: 启动数据库时发生异常: {str(e)}")

def stop_database(client_path):
    """停止数据库，向数据库发送crash指令"""
    print("\n=== 停止数据库 ===")
    try:
        result = subprocess.run(
            [client_path],
            input="crash",
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=30
        )
        print("数据库已停止")
        if result.stderr:
            print(f"错误信息: {result.stderr.strip()}")
    except Exception as e:
        print(f"错误: 停止数据库时发生异常: {str(e)}")

if __name__ == "__main__":
    generate_sql_files()

    client_path = "rmdb_client/build/rmdb_client"
    output_file = "test_results.txt"

    if os.path.exists(output_file):
        os.remove(output_file)

    if not os.path.exists(client_path):
        print(f"错误: 客户端程序不存在 - {client_path}")
        exit(1)

    stage_times = {}

    start_database()

    # 阶段1: 插入数据
    stage_times["插入数据"] = run_stage(client_path, "插入数据", "stage1_insert.sql", output_file)

    # 阶段2: 事务操作
    if stage_times["插入数据"] >= 0:
        stage_times["事务操作"] = run_stage(client_path, "事务操作", "stage2_transaction.sql", output_file)

    # 阶段3: 崩溃前查询
    if stage_times["事务操作"] >= 0:
        pre_crash_results = run_individual_queries(client_path, "stage3_pre_crash_query.sql", output_file, "崩溃前查询")

    # 阶段4: 模拟崩溃
    if pre_crash_results:
        print("\n=== 模拟数据库崩溃 ===")
        stop_database(client_path)

    start_database()

    # 阶段5: 崩溃后查询
    post_crash_results = run_individual_queries(client_path, "stage5_post_crash_query.sql", output_file, "崩溃后查询")

    # 比较崩溃前后的查询结果，判断恢复是否正确
    if pre_crash_results and post_crash_results:
        print("\n=== 恢复验证 ===")
        # 这里可以添加更复杂的结果比较逻辑，如解析查询结果并比较
        print("需要手动比较崩溃前后的查询结果，判断数据库恢复是否正确。")

    stop_database(client_path)
    print(f"\n完整结果已保存至: {output_file}")