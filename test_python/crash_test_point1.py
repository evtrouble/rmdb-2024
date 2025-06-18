#!/usr/bin/env python3
"""
测试点1: 单线程发送事务，数据量较小，不包括建立检查点
手动启动数据库版本
"""

import socket
import time
import random
import json
from datetime import datetime

class SingleThreadTester:
    def __init__(self, host="127.0.0.1", port=8765):
        self.server_host = host
        self.server_port = port
        self.buffer_size = 8192

    def log(self, message: str):
        """记录日志"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")

    def send_sql(self, sql: str, timeout: int = 30) -> tuple[bool, str]:
        """发送SQL命令到服务器"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect((self.server_host, self.server_port))
                sock.sendall((sql + '\0').encode('utf-8'))
                data = sock.recv(self.buffer_size)
                response = data.decode('utf-8').strip()
                return True, response
        except Exception as e:
            return False, str(e)

    def generate_test_data(self, table_name: str, num_records: int):
        """生成测试数据SQL"""
        sql_statements = []
        
        # 创建warehouse表
        sql_statements.append("""
CREATE TABLE warehouse (
    w_id INT,
    w_name CHAR(10),
    w_street_1 CHAR(20),
    w_street_2 CHAR(20),
    w_city CHAR(20),
    w_state CHAR(2),
    w_zip CHAR(9),
    w_tax FLOAT,
    w_ytd FLOAT
);""")

        # 生成插入数据
        for i in range(1, num_records + 1):
            sql_statements.append(f"""
INSERT INTO warehouse VALUES (
    {i},
    'WH_{i:03d}',
    'Street1_{i}',
    'Street2_{i}',
    'City{i}',
    'ST',
    '12345-{i:04d}',
    {0.05 + (i % 10) * 0.01},
    {1000.0 + i * 10.0}
);""")

        return sql_statements

    def generate_transaction_queries(self, num_transactions: int):
        """生成事务查询SQL"""
        sql_statements = []
        
        for i in range(num_transactions):
            # 开始事务
            sql_statements.append("BEGIN;")
            
            # 随机选择操作类型
            operation = random.choice(['select', 'update', 'insert', 'delete'])
            
            if operation == 'select':
                # 范围查询
                start_id = random.randint(1, 45)
                sql_statements.append(
                    f"SELECT * FROM warehouse WHERE w_id BETWEEN {start_id} AND {start_id + 5};"
                )
            
            elif operation == 'update':
                # 批量更新
                start_id = random.randint(1, 45)
                amount = random.randint(100, 1000)
                sql_statements.append(
                    f"UPDATE warehouse SET w_ytd = w_ytd + {amount} "
                    f"WHERE w_id BETWEEN {start_id} AND {start_id + 5};"
                )
            
            elif operation == 'insert':
                # 插入新记录
                new_id = 1000 + i
                sql_statements.append(f"""
INSERT INTO warehouse VALUES (
    {new_id},
    'NEW_{new_id:03d}',
    'NewStreet1_{new_id}',
    'NewStreet2_{new_id}',
    'NewCity{new_id}',
    'ST',
    '54321-{i:04d}',
    {0.06 + (i % 10) * 0.01},
    {2000.0 + i * 10.0}
);""")
            
            else:  # delete
                # 删除一条记录
                target_id = random.randint(1000, 1000 + num_transactions - 1)
                sql_statements.append(f"DELETE FROM warehouse WHERE w_id = {target_id};")
            
            # 提交事务
            sql_statements.append("COMMIT;")
        
        return sql_statements

    def check_connection(self):
        """检查数据库连接"""
        success, response = self.send_sql("SELECT 1;")
        if success:
            self.log("数据库连接成功")
            return True
        else:
            self.log(f"数据库连接失败: {response}")
            return False

    def run_test(self, print_queries=False):
        """执行测试点1"""
        try:
            # 1. 检查连接
            if not self.check_connection():
                return False

            # 2. 生成并执行初始数据
            self.log("开始创建表和插入初始数据...")
            init_statements = self.generate_test_data("warehouse", 50)
            
            for sql in init_statements:
                if print_queries:
                    self.log(f"执行SQL: {sql}")
                success, response = self.send_sql(sql.strip())
                if not success:
                    self.log(f"初始化失败: {response}")
                    return False
            
            self.log("初始化完成")

            # 3. 生成并执行事务
            self.log("开始执行事务...")
            transaction_statements = self.generate_transaction_queries(20)
            
            executed_count = 0
            for sql in transaction_statements:
                if print_queries:
                    self.log(f"执行SQL: {sql}")
                success, response = self.send_sql(sql.strip())
                executed_count += 1
                
                if not success:
                    self.log(f"事务执行失败: {response}")
                    return False
                    
                # 在执行一半事务后有一定概率触发crash
                if executed_count > len(transaction_statements) // 2:
                    if random.random() < 0.1:  # 10%概率crash
                        self.log(f"已执行 {executed_count} 个语句，现在触发crash")
                        return True

            self.log("所有事务执行完成")
            return True

        except Exception as e:
            self.log(f"测试执行异常: {str(e)}")
            return False

def main():
    # 创建测试器实例
    tester = SingleThreadTester()
    
    # 执行测试
    print("=" * 50)
    print("测试点1: 单线程发送事务测试")
    print("说明: 请确保数据库已经启动并监听8765端口")
    print("=" * 50)
    
    input("按Enter键开始测试...")
    
    success = tester.run_test(print_queries=True)
    
    if success:
        print("\n测试完成。请检查：")
        print("1. 数据库是否正确crash")
        print("2. 重启后是否能够正确恢复")
        print("3. 数据是否一致")
    else:
        print("\n测试失败，请检查错误信息")

if __name__ == "__main__":
    main()
