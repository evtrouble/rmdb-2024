#!/usr/bin/env python3
import time
import subprocess
import os
from typing import List, Dict, Tuple

def generate_sql_files():
    """生成多个阶段的SQL文件，测试不同数据类型的多列索引"""
    # 阶段1: 创建测试表
    with open("stage1_create_tables.sql", 'w') as f:
        f.write("""
-- 创建测试表
CREATE TABLE test_int (id INT, col1 INT, col2 INT);
CREATE TABLE test_float (id INT, col1 FLOAT, col2 FLOAT);
CREATE TABLE test_char (id INT, col1 CHAR(10), col2 CHAR(10));
CREATE TABLE test_varchar (id INT, col1 CHAR(20), col2 CHAR(20));
CREATE TABLE test_mixed1 (id INT, col1 INT, col2 FLOAT);
CREATE TABLE test_mixed2 (id INT, col1 INT, col2 CHAR(10));
CREATE TABLE test_mixed3 (id INT, col1 FLOAT, col2 CHAR(20));
CREATE TABLE test_all (id INT, col1 INT, col2 FLOAT, col3 CHAR(10), col4 CHAR(20));
        """)
        print(f"创建表SQL文件已生成: stage1_create_tables.sql")
    
    # 阶段2: 插入测试数据
    with open("stage2_insert_data.sql", 'w') as f:
        f.write("-- 插入测试数据\n")
        
        # 为每个表生成1000条测试数据
        for table in ["test_int", "test_float", "test_char", "test_varchar", 
                      "test_mixed1", "test_mixed2", "test_mixed3", "test_all"]:
            for i in range(1, 1001):
                if table == "test_int":
                    values = f"{i}, {i*2}, {i*3}"
                elif table == "test_float":
                    values = f"{i}, {i*1.1}, {i*2.2}"
                elif table == "test_char":
                    values = f"{i}, 'char_{i:04d}', 'data_{i:04d}'"
                elif table == "test_varchar":
                    values = f"{i}, 'varchar_{i:04d}', 'value_{i:04d}'"
                elif table == "test_mixed1":
                    values = f"{i}, {i*2}, {i*1.5}"
                elif table == "test_mixed2":
                    values = f"{i}, {i*2}, 'char_{i:04d}'"
                elif table == "test_mixed3":
                    values = f"{i}, {i*1.5}, 'varchar_{i:04d}'"
                elif table == "test_all":
                    values = f"{i}, {i*2}, {i*1.5}, 'char_{i:04d}', 'varchar_{i:04d}'"
                
                f.write(f"INSERT INTO {table} VALUES ({values});\n")
        
        print(f"插入数据SQL文件已生成: stage2_insert_data.sql")
    
    # 阶段3: 创建多列索引
    with open("stage3_create_indexes.sql", 'w') as f:
        f.write("-- 创建多列索引\n")
        f.write("CREATE INDEX test_int(col1, col2);\n")
        f.write("CREATE INDEX test_float(col1, col2);\n")
        f.write("CREATE INDEX test_char(col1, col2);\n")
        f.write("CREATE INDEX test_varchar(col1, col2);\n")
        f.write("CREATE INDEX test_mixed1(col1, col2);\n")
        f.write("CREATE INDEX test_mixed2(col1, col2);\n")
        f.write("CREATE INDEX test_mixed3(col1, col2);\n")
        f.write("CREATE INDEX test_all(col1, col2, col3, col4);\n")
        print(f"创建索引SQL文件已生成: stage3_create_indexes.sql")
    
    # 阶段4: 查询测试（无索引）
    with open("stage4_no_index_queries.sql", 'w') as f:
        f.write("-- 无索引查询测试\n")
        
        # 为每个表生成查询
        for table in ["test_int", "test_float", "test_char", "test_varchar", 
                      "test_mixed1", "test_mixed2", "test_mixed3", "test_all"]:
            for i in range(1, 1001, 10):  # 每10条记录查询一次
                if table == "test_int":
                    cond = f"col1 = {i*2} AND col2 = {i*3}"
                elif table == "test_float":
                    cond = f"col1 = {i*1.1:.6f} AND col2 = {i*2.2:.6f}"
                elif table == "test_char":
                    cond = f"col1 = 'char_{i:04d}' AND col2 = 'data_{i:04d}'"
                elif table == "test_varchar":
                    cond = f"col1 = 'varchar_{i:04d}' AND col2 = 'value_{i:04d}'"
                elif table == "test_mixed1":
                    cond = f"col1 = {i*2} AND col2 = {i*1.5:.6f}"
                elif table == "test_mixed2":
                    cond = f"col1 = {i*2} AND col2 = 'char_{i:04d}'"
                elif table == "test_mixed3":
                    cond = f"col1 = {i*1.5:.6f} AND col2 = 'varchar_{i:04d}'"
                elif table == "test_all":
                    cond = f"col1 = {i*2} AND col2 = {i*1.5:.6f} AND col3 = 'char_{i:04d}' AND col4 = 'varchar_{i:04d}'"
                
                f.write(f"SELECT * FROM {table} WHERE {cond};\n")
        
        print(f"无索引查询SQL文件已生成: stage4_no_index_queries.sql")
    
    # 阶段5: 查询测试（有索引）
    with open("stage5_with_index_queries.sql", 'w') as f:
        f.write("-- 有索引查询测试\n")
        # 复制阶段4的查询
        f.write(open("stage4_no_index_queries.sql", 'r').read())
        print(f"有索引查询SQL文件已生成: stage5_with_index_queries.sql")
    
    # 阶段6: 清理
    with open("stage6_cleanup.sql", 'w') as f:
        f.write("-- 清理测试表\n")
        
        for table in ["test_int", "test_float", "test_char", "test_varchar", 
                      "test_mixed1", "test_mixed2", "test_mixed3", "test_all"]:
            f.write(f"DROP TABLE {table};\n")
        print(f"清理SQL文件已生成: stage6_cleanup.sql")

def run_individual_queries(client_path: str, sql_file: str, output_file: str, stage_name: str) -> Dict[str, List[float]]:
    """逐行执行SQL查询并按表记录每个查询的执行时间"""
    print(f"\n=== 开始执行阶段: {stage_name} (逐行操作) ===")
    
    if not os.path.exists(sql_file):
        print(f"错误: SQL文件不存在 - {sql_file}")
        return {}
    
    table_times = {}  # 按表名存储查询时间
    tables = ["test_int", "test_float", "test_char", "test_varchar", 
              "test_mixed1", "test_mixed2", "test_mixed3", "test_all"]
    table_count = 0
    
    try:
        with open(sql_file, 'r') as f:
            queries = f.readlines()
        
        queries = [q.strip() for q in queries if q.strip() and not q.strip().startswith('--')]
        total_queries = len(queries)
        print(f"准备执行 {total_queries} 个操作")
        
        with open(output_file, 'a') as out_f:
            out_f.write(f"\n=== 阶段: {stage_name} ===\n")
            
            for i, query in enumerate(queries):
                # 提取表名
                table_name = query.split()[2] if query.startswith("SELECT") else "unknown"
                
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
                
                # 记录每个表的查询时间
                if table_name not in table_times:
                    table_times[table_name] = []
                table_times[table_name].append(execution_time)
                
                if (i + 1) % 100 == 0 or (i + 1) == total_queries:
                    print(f"已执行 {i+1}/{total_queries} 个操作")
                
                out_f.write(f"操作 {i+1}: {query}\n")
                out_f.write(f"执行时间: {execution_time:.6f} 秒\n")
                out_f.write(f"输出: {result.stdout.strip()}\n")
                if result.stderr:
                    out_f.write(f"错误: {result.stderr.strip()}\n")
                out_f.write("-"*50 + "\n")

                # 检查是否完成一个表的查询
                if table_name == tables[table_count] and (i + 1) % 100 == 0:
                    table_count += 1
                    if table_count <= len(tables):
                        print(f"第 {table_count} 个表 {table_name} 的查询执行完毕")
            
            # 输出每个表的统计信息
            out_f.write(f"\n=== {stage_name} 按表统计信息 ===\n")
            for table, times in table_times.items():
                if times:
                    avg_time = sum(times) / len(times)
                    out_f.write(f"表: {table}\n")
                    out_f.write(f"  查询次数: {len(times)}\n")
                    out_f.write(f"  总执行时间: {sum(times):.2f} 秒\n")
                    out_f.write(f"  平均执行时间: {avg_time:.6f} 秒\n")
                    out_f.write(f"  最小执行时间: {min(times):.6f} 秒\n")
                    out_f.write(f"  最大执行时间: {max(times):.6f} 秒\n")
                    out_f.write("-"*50 + "\n")
            
            print(f"阶段 {stage_name} 完成")
    
    except subprocess.TimeoutExpired:
        print(f"错误: 操作执行超时 (30秒)")
    except Exception as e:
        print(f"错误: 执行阶段 {stage_name} 时发生异常: {str(e)}")
    
    return table_times

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

def analyze_query_times(no_index_times: Dict[str, List[float]], index_times: Dict[str, List[float]], output_file: str):
    """分析不同表的查询性能"""
    print("\n=== 性能分析报告 ===")
    
    with open(output_file, 'a') as f:
        f.write("\n=== 完整性能报告 ===\n")
        
        for table in no_index_times.keys() & index_times.keys():
            if not no_index_times[table] or not index_times[table]:
                continue
                
            avg_no_index = sum(no_index_times[table]) / len(no_index_times[table])
            avg_with_index = sum(index_times[table]) / len(index_times[table])
            speedup = avg_no_index / avg_with_index
            
            f.write(f"\n--- 表: {table} ---\n")
            f.write(f"无索引总时间: {sum(no_index_times[table]):.2f} 秒\n")
            f.write(f"有索引总时间: {sum(index_times[table]):.2f} 秒\n")
            f.write(f"平均查询时间（无索引）: {avg_no_index:.6f} 秒\n")
            f.write(f"平均查询时间（有索引）: {avg_with_index:.6f} 秒\n")
            f.write(f"性能提升: {speedup:.2f}x\n")
            f.write("-"*50 + "\n")
            
            print(f"\n--- 表: {table} ---")
            print(f"无索引总时间: {sum(no_index_times[table]):.2f} 秒")
            print(f"有索引总时间: {sum(index_times[table]):.2f} 秒")
            print(f"性能提升: {speedup:.2f}x")

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
    no_index_query_times = {}
    index_query_times = {}
    
    # 阶段1: 创建表
    stage_times["创建表"] = run_stage(client_path, "创建表", "stage1_create_tables.sql", output_file)
    
    # 阶段2: 插入数据
    if stage_times["创建表"] >= 0:
        stage_times["插入数据"] = run_stage(client_path, "插入数据", "stage2_insert_data.sql", output_file)
    
    # 阶段3: 创建索引
    if stage_times.get("插入数据", -1) >= 0:
        stage_times["创建索引"] = run_stage(client_path, "创建索引", "stage3_create_indexes.sql", output_file)
    
    # 阶段4: 无索引查询
    if stage_times.get("插入数据", -1) >= 0:
        no_index_query_times = run_individual_queries(client_path, "stage4_no_index_queries.sql", output_file, "无索引查询")
    
    # 阶段5: 有索引查询
    if stage_times.get("创建索引", -1) >= 0:
        index_query_times = run_individual_queries(client_path, "stage5_with_index_queries.sql", output_file, "有索引查询")
    
    # 阶段6: 清理
    run_stage(client_path, "清理", "stage6_cleanup.sql", output_file)
    
    # 生成分析报告
    analyze_query_times(no_index_query_times, index_query_times, output_file)
    print(f"\n完整结果已保存至: {output_file}")