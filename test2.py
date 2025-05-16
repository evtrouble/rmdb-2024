#!/usr/bin/env python3
import time
import subprocess
import os
import re
from typing import List, Dict, Tuple

def generate_sql_files():
    """生成多个阶段的SQL文件"""
    # 阶段1: 创建表并插入数据
    with open("stage1_insert.sql", 'w') as f:
        f.write("CREATE TABLE warehouse (w_id INT, name CHAR(8));\n\n")
        print("正在生成INSERT语句...")
        for i in range(1, 3001):
            unique_str = f"{i:08d}"[-8:]
            f.write(f"INSERT INTO warehouse VALUES ({i}, '{unique_str}');\n")
        print(f"INSERT SQL文件已生成: stage1_insert.sql")
    
    # 阶段2: 无索引查询测试 (不使用数据库计时)
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
    
    # 阶段4: 有索引查询测试 (不使用数据库计时)
    with open("stage4_with_index.sql", 'w') as f:
        f.write("-- 有索引查询测试开始\n")
        for i in range(1, 3001):
            f.write(f"SELECT * FROM warehouse WHERE w_id = {i};\n")
        f.write("-- 有索引查询测试结束\n")
        print(f"有索引查询SQL文件已生成: stage4_with_index.sql")
    
    # 阶段5: 清理
    with open("stage5_cleanup.sql", 'w') as f:
        f.write("DROP TABLE warehouse;\n")
        print(f"清理SQL文件已生成: stage5_cleanup.sql")

def run_individual_queries(client_path: str, sql_file: str, output_file: str, stage_name: str) -> List[float]:
    """逐行执行SQL查询并记录每个查询的执行时间"""
    print(f"\n=== 开始执行阶段: {stage_name} (逐行查询) ===")
    
    if not os.path.exists(sql_file):
        print(f"错误: SQL文件不存在 - {sql_file}")
        return []
    
    query_times = []
    
    try:
        # 读取所有查询
        with open(sql_file, 'r') as f:
            queries = f.readlines()
        
        # 过滤掉注释和空行
        queries = [q.strip() for q in queries if q.strip() and not q.strip().startswith('--')]
        
        total_queries = len(queries)
        print(f"准备执行 {total_queries} 个查询")
        
        # 打开输出文件
        with open(output_file, 'a') as out_f:
            out_f.write(f"\n=== 阶段: {stage_name} ===\n")
            
            # 逐个执行查询并计时
            for i, query in enumerate(queries):
                start_time = time.time()
                
                result = subprocess.run(
                    [client_path],
                    input=query,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                    timeout=30  # 单个查询30秒超时
                )
                
                end_time = time.time()
                execution_time = end_time - start_time
                query_times.append(execution_time)
                
                # 打印进度
                if (i + 1) % 500 == 0 or (i + 1) == total_queries:
                    print(f"已执行 {i+1}/{total_queries} 个查询")
                
                # 记录结果
                out_f.write(f"查询 {i+1}: {query}\n")
                out_f.write(f"执行时间: {execution_time:.6f} 秒\n")
                out_f.write(f"输出: {result.stdout.strip()}\n")
                if result.stderr:
                    out_f.write(f"错误: {result.stderr.strip()}\n")
                out_f.write("-"*50 + "\n")
            
            # 写入统计信息
            if query_times:
                avg_time = sum(query_times) / len(query_times)
                min_time = min(query_times)
                max_time = max(query_times)
                
                out_f.write(f"\n=== {stage_name} 统计信息 ===\n")
                out_f.write(f"总查询数: {len(query_times)}\n")
                out_f.write(f"总执行时间: {sum(query_times):.2f} 秒\n")
                out_f.write(f"平均执行时间: {avg_time:.6f} 秒\n")
                out_f.write(f"最小执行时间: {min_time:.6f} 秒\n")
                out_f.write(f"最大执行时间: {max_time:.6f} 秒\n")
                out_f.write("="*50 + "\n")
            
            print(f"阶段 {stage_name} 完成")
            print(f"执行 {len(query_times)} 个查询，总时间: {sum(query_times):.2f} 秒")
    
    except subprocess.TimeoutExpired:
        print(f"错误: 查询执行超时 (30秒)")
    except Exception as e:
        print(f"错误: 执行阶段 {stage_name} 时发生异常: {str(e)}")
    
    return query_times

def run_stage(client_path: str, stage_name: str, sql_file: str, output_file: str) -> float:
    """执行单个阶段并返回执行时间"""
    print(f"\n=== 开始执行阶段: {stage_name} ===")
    
    try:
        start_time = time.time()
        
        result = subprocess.run(
            [client_path],
            input=open(sql_file, 'r').read(),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=1800  # 设置30分钟超时
        )
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        print(f"阶段 {stage_name} 完成")
        print(f"执行时间: {execution_time:.2f} 秒")
        
        # 将阶段输出追加到结果文件
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

def analyze_query_times(no_index_times: List[float], index_times: List[float], output_file: str):
    """分析查询时间并生成报告"""
    print("\n=== 分析查询时间 ===")
    
    if not no_index_times or not index_times:
        print("警告: 没有足够的查询时间数据进行分析")
        return
    
    # 计算统计信息
    avg_no_index_time = sum(no_index_times) / len(no_index_times)
    avg_index_time = sum(index_times) / len(index_times)
    speedup = avg_no_index_time / avg_index_time if avg_index_time > 0 else 0
    
    # 计算百分位数
    def percentile(arr, p):
        arr_sorted = sorted(arr)
        pos = (len(arr) - 1) * p
        floor = int(pos)
        ceil = floor + 1
        if ceil > len(arr) - 1:
            return arr_sorted[floor]
        return arr_sorted[floor] + (arr_sorted[ceil] - arr_sorted[floor]) * (pos - floor)
    
    percentiles = [50, 90, 95, 99]
    no_index_percentiles = {p: percentile(no_index_times, p/100) for p in percentiles}
    index_percentiles = {p: percentile(index_times, p/100) for p in percentiles}
    
    print(f"无索引查询:")
    print(f"  平均时间: {avg_no_index_time:.6f} 秒")
    for p in percentiles:
        print(f"  {p}% 百分位: {no_index_percentiles[p]:.6f} 秒")
    
    print(f"\n有索引查询:")
    print(f"  平均时间: {avg_index_time:.6f} 秒")
    for p in percentiles:
        print(f"  {p}% 百分位: {index_percentiles[p]:.6f} 秒")
    
    print(f"\n性能提升: {speedup:.2f}x")
    
    # 将统计信息追加到结果文件
    with open(output_file, 'a') as f:
        f.write("\n=== 查询性能统计 ===\n")
        f.write(f"无索引查询次数: {len(no_index_times)}\n")
        f.write(f"有索引查询次数: {len(index_times)}\n")
        f.write(f"\n无索引查询:\n")
        f.write(f"  总时间: {sum(no_index_times):.2f} 秒\n")
        f.write(f"  平均时间: {avg_no_index_time:.6f} 秒\n")
        for p in percentiles:
            f.write(f"  {p}% 百分位: {no_index_percentiles[p]:.6f} 秒\n")
        
        f.write(f"\n有索引查询:\n")
        f.write(f"  总时间: {sum(index_times):.2f} 秒\n")
        f.write(f"  平均时间: {avg_index_time:.6f} 秒\n")
        for p in percentiles:
            f.write(f"  {p}% 百分位: {index_percentiles[p]:.6f} 秒\n")
        
        f.write(f"\n性能提升: {speedup:.2f}x\n")

if __name__ == "__main__":
    # 生成SQL文件
    generate_sql_files()
    
    # 配置数据库客户端路径
    client_path = "rmdb_client/build/rmdb_client"
    output_file = "test_results.txt"
    
    # 清除之前的结果文件
    if os.path.exists(output_file):
        os.remove(output_file)
    
    # 检查客户端程序是否存在
    if not os.path.exists(client_path):
        print(f"错误: 客户端程序不存在 - {client_path}")
        print("请确保路径正确，或者修改脚本中的client_path变量")
    else:
        # 执行各阶段测试
        stage_times = {}
        
        # 阶段1: 插入数据
        stage_times["插入数据"] = run_stage(client_path, "插入数据", "stage1_insert.sql", output_file)
        
        # 阶段2: 无索引查询 (逐行执行并计时)
        no_index_times = []
        if stage_times["插入数据"] >= 0:
            no_index_times = run_individual_queries(client_path, "stage2_no_index.sql", output_file, "无索引查询")
            if not no_index_times:
                print("无索引查询阶段失败，跳过后续测试")
        else:
            print("插入数据阶段失败，跳过后续测试")
        
        # 阶段3: 创建索引
        if no_index_times:
            stage_times["创建索引"] = run_stage(client_path, "创建索引", "stage3_create_index.sql", output_file)
        else:
            print("无索引查询阶段失败，跳过后续测试")
        
        # 阶段4: 有索引查询 (逐行执行并计时)
        index_times = []
        if stage_times.get("创建索引", -1) >= 0:
            index_times = run_individual_queries(client_path, "stage4_with_index.sql", output_file, "有索引查询")
        else:
            print("创建索引阶段失败，跳过后续测试")
        
        # 阶段5: 清理
        if index_times:
            run_stage(client_path, "清理", "stage5_cleanup.sql", output_file)
        
        # 打印总览
        print("\n=== 测试总览 ===")
        for stage, time_taken in stage_times.items():
            if time_taken >= 0:
                print(f"{stage}: {time_taken:.2f} 秒")
            else:
                print(f"{stage}: 失败")
        
        # 分析查询时间
        if no_index_times and index_times:
            analyze_query_times(no_index_times, index_times, output_file)
        else:
            print("无法分析查询时间：缺少必要的查询数据")
        
        print(f"\n完整测试结果已保存到: {output_file}")