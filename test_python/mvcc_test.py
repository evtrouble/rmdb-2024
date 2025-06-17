#!/usr/bin/env python3
import time
import subprocess
import os

def generate_sql_files():
    """生成多个阶段的SQL文件（创建表、插入、删除、插入、更新、清理）"""
    # 阶段1: 创建表并插入数据
    with open("stage1_setup.sql", 'w') as f:
        f.write("CREATE TABLE test_table (id INT, value CHAR(20));\n")
        for i in range(1, 6):  # 插入5条数据
            f.write(f"INSERT INTO test_table VALUES ({i}, 'Initial');\n")
        print(f"表创建和初始数据插入SQL文件已生成: stage1_setup.sql")
    
    # 阶段2: 事务T1更新数据
    with open("stage2_t1_update.sql", 'w') as f:
        f.write("BEGIN;\n")
        for i in range(1, 6):
            f.write(f"UPDATE test_table SET value = 'T1_Updated' WHERE id = {i};\n")
        # 不提交，保持事务开启
        print(f"事务T1更新SQL文件已生成: stage2_t1_update.sql")
    
    # 阶段3: 事务T2尝试删除和插入数据
    with open("stage3_t2_delete_insert.sql", 'w') as f:
        f.write("BEGIN;\n")
        # 删除数据
        for i in range(1, 3):  # 删除前2条
            f.write(f"DELETE FROM test_table WHERE id = {i};\n")
        # 插入新数据（可能产生写写冲突）
        for i in range(1, 3):
            f.write(f"INSERT INTO test_table VALUES ({i}, 'T2_Inserted');\n")
        # 提交事务T2
        f.write("COMMIT;\n")
        print(f"事务T2删除和插入SQL文件已生成: stage3_t2_delete_insert.sql")
    
    # 阶段4: 提交事务T1
    with open("stage4_commit_t1.sql", 'w') as f:
        f.write("COMMIT;\n")
        print(f"事务T1提交SQL文件已生成: stage4_commit_t1.sql")
    
    # 阶段5: 检查最终数据
    with open("stage5_check_data.sql", 'w') as f:
        f.write("SELECT * FROM test_table;\n")
        print(f"数据检查SQL文件已生成: stage5_check_data.sql")
    
    # 阶段6: 清理
    with open("stage6_cleanup.sql", 'w') as f:
        f.write("DROP TABLE test_table;\n")
        print(f"清理SQL文件已生成: stage6_cleanup.sql")

def run_sql_file(client_path: str, sql_file: str, output_file: str, stage_name: str):
    """执行SQL文件并记录输出"""
    print(f"\n=== 执行阶段: {stage_name} ===")
    try:
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        
        result = subprocess.run(
            [client_path],
            input=sql_content,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=60  # 整个文件超时时间
        )
        
        with open(output_file, 'a') as out_f:
            out_f.write(f"\n=== 阶段: {stage_name} ===\n")
            out_f.write("标准输出:\n")
            out_f.write(result.stdout)
            out_f.write("\n错误输出:\n")
            out_f.write(result.stderr)
            out_f.write("\n" + "="*50 + "\n")
        
        print(f"阶段 {stage_name} 执行完成")
    
    except subprocess.TimeoutExpired:
        print(f"错误: 阶段 {stage_name} 执行超时")
    except Exception as e:
        print(f"错误: 执行阶段 {stage_name} 时发生异常: {str(e)}")

if __name__ == "__main__":
    generate_sql_files()
    
    client_path = "rmdb_client/build/rmdb_client"
    output_file = "mvcc_test_results.txt"
    
    if os.path.exists(output_file):
        os.remove(output_file)
    
    if not os.path.exists(client_path):
        print(f"错误: 客户端程序不存在 - {client_path}")
        exit(1)
    
    # 执行各个阶段
    run_sql_file(client_path, "stage1_setup.sql", output_file, "创建表和插入初始数据")
    run_sql_file(client_path, "stage2_t1_update.sql", output_file, "事务T1更新数据（保持开启）")
    run_sql_file(client_path, "stage3_t2_delete_insert.sql", output_file, "事务T2删除和插入数据")
    run_sql_file(client_path, "stage4_commit_t1.sql", output_file, "提交事务T1")
    run_sql_file(client_path, "stage5_check_data.sql", output_file, "检查最终数据")
    run_sql_file(client_path, "stage6_cleanup.sql", output_file, "清理")
    
    print(f"\n完整结果已保存至: {output_file}")