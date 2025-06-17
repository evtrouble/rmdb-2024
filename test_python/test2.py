#!/usr/bin/env python3
import time
import subprocess
import os
from typing import List, Dict, Tuple

def generate_sql_files():
    """生成多个阶段的SQL文件（新增删除阶段）"""
    # 阶段1: 创建表并插入数据
    with open("stage1_insert.sql", 'w') as f:
        f.write("CREATE TABLE warehouse (w_id INT, name CHAR(8));\n\n")  # 添加PRIMARY KEY约束
        print("正在生成INSERT语句...")
        for i in range(1, 3001):
            unique_str = f"{i:08d}"[-8:]
            f.write(f"INSERT INTO warehouse VALUES ({i}, '{unique_str}');\n")
        print(f"INSERT SQL文件已生成: stage1_insert.sql")
    
    # 阶段2: 无索引查询测试
    with open("stage2_no_index.sql", 'w') as f:
        f.write("-- 无索引查询测试开始\n")
        for i in range(1, 3001):
            f.write(f"SELECT * FROM warehouse WHERE w_id = {i};\n")
        f.write("-- 无索引查询测试结束\n")
        print(f"无索引查询SQL文件已生成: stage2_no_index.sql")
    
    # 阶段3: 创建索引
    with open("stage3_create_index.sql", 'w') as f:
        f.write("CREATE INDEX warehouse(w_id);\n")
        print(f"创建索引SQL文件已生成: stage3_create_index.sql")
    
    # 阶段4: 有索引查询测试
    with open("stage4_with_index.sql", 'w') as f:
        f.write("-- 有索引查询测试开始\n")
        for i in range(1, 3001):
            f.write(f"SELECT * FROM warehouse WHERE w_id = {i};\n")
        f.write("-- 有索引查询测试结束\n")
        print(f"有索引查询SQL文件已生成: stage4_with_index.sql")
    
    # 新增阶段5: 删除所有数据（3000个删除操作）
    with open("stage5_delete.sql", 'w') as f:
        f.write("-- 删除测试开始\n")
        for i in range(1, 3001):
            f.write(f"DELETE FROM warehouse WHERE w_id = {i};\n")
        f.write("-- 删除测试结束\n")
        print(f"删除SQL文件已生成: stage5_delete.sql")
    
    # 阶段6: 清理
    with open("stage6_cleanup.sql", 'w') as f:
        f.write("DROP TABLE warehouse;\n")
        print(f"清理SQL文件已生成: stage6_cleanup.sql")

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
                
                if (i + 1) % 500 == 0 or (i + 1) == total_queries:
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

def analyze_query_times(no_index_times: List[float], index_times: List[float], delete_times: List[float], output_file: str):
    """分析查询和删除操作的时间性能"""
    print("\n=== 性能分析报告 ===")
    
    # 基础统计
    def print_statistics(stage: str, times: List[float]):
        if not times:
            print(f"警告: {stage} 操作数据为空")
            return
        avg = sum(times)/len(times)
        print(f"\n{stage} 统计:")
        print(f"  总操作数: {len(times)}")
        print(f"  总时间: {sum(times):.2f} 秒")
        print(f"  平均时间: {avg:.6f} 秒")
        print(f"  最小时间: {min(times):.6f} 秒")
        print(f"  最大时间: {max(times):.6f} 秒")
    
    # 查询性能对比
    if no_index_times and index_times:
        speedup = (sum(no_index_times)/len(no_index_times)) / (sum(index_times)/len(index_times))
        print("\n=== 查询性能对比 ===")
        print_statistics("无索引查询", no_index_times)
        print_statistics("有索引查询", index_times)
        print(f"\n索引提升: {speedup:.2f}x")
    
    # 删除性能分析
    if delete_times:
        print("\n=== 删除性能分析 ===")
        print_statistics("索引删除操作", delete_times)
    
    # 写入结果文件
    with open(output_file, 'a') as f:
        f.write("\n=== 完整性能报告 ===\n")
        if no_index_times and index_times:
            f.write("\n--- 查询性能 ---\n")
            f.write(f"无索引查询总时间: {sum(no_index_times):.2f} 秒\n")
            f.write(f"有索引查询总时间: {sum(index_times):.2f} 秒\n")
            f.write(f"性能提升: {speedup:.2f}x\n")
        if delete_times:
            f.write("\n--- 删除性能 ---\n")
            f.write(f"删除操作总时间: {sum(delete_times):.2f} 秒\n")
            f.write(f"平均删除时间: {sum(delete_times)/len(delete_times):.6f} 秒\n")

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
    no_index_times = []
    index_times = []
    delete_times = []
    
    # 阶段1: 插入数据
    stage_times["插入数据"] = run_stage(client_path, "插入数据", "stage1_insert.sql", output_file)
    
    # 阶段2: 无索引查询
    if stage_times["插入数据"] >= 0:
        no_index_times = run_individual_queries(client_path, "stage2_no_index.sql", output_file, "无索引查询")
    
    # 阶段3: 创建索引
    if no_index_times:
        stage_times["创建索引"] = run_stage(client_path, "创建索引", "stage3_create_index.sql", output_file)
    
    # 阶段4: 有索引查询
    if stage_times.get("创建索引", -1) >= 0:
        index_times = run_individual_queries(client_path, "stage4_with_index.sql", output_file, "有索引查询")
    
    # 新增阶段5: 删除操作（依赖索引存在）
    if index_times:
        delete_times = run_individual_queries(client_path, "stage5_delete.sql", output_file, "索引删除操作")
    
    # 阶段6: 清理
    if delete_times or index_times:
        run_stage(client_path, "清理", "stage6_cleanup.sql", output_file)
    
    # 生成分析报告
    analyze_query_times(no_index_times, index_times, delete_times, output_file)
    print(f"\n完整结果已保存至: {output_file}")