#!/usr/bin/env python3
"""
基于静态检查点的故障恢复测试脚本
模拟题目中描述的6个测试点的测评过程
"""

import os
import sys
import time
import signal
import subprocess
import threading
import socket
import random
import json
from datetime import datetime
from typing import List, Dict, Tuple, Optional
import tempfile
import shutil

class CrashRecoveryTester:
    def __init__(self, rmdb_path: str = "./bin/rmdb", client_path: str = "./rmdb_client/build/rmdb_client"):
        self.rmdb_path = rmdb_path
        self.client_path = client_path
        self.server_process = None
        self.test_results = {}
        self.recovery_times = {}
        self.io_stats = {}  # 新增: IO统计
        
        # 测试配置
        self.server_host = "127.0.0.1"
        self.server_port = 8765
        self.buffer_size = 8192
        self.startup_timeout = 30    # 服务器启动超时时间(秒)
        self.recovery_timeout = 180  # 恢复超时时间(秒) 
        self.check_interval = 0.1    # 恢复检查间隔(秒)

        # 创建测试目录
        self.test_dir = "crash_recovery_test"
        os.makedirs(self.test_dir, exist_ok=True)

    def log(self, message: str):
        """记录日志"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")

    def start_server(self, db_name: str = "test_db") -> bool:
        """启动数据库服务器"""
        try:
            self.log(f"启动数据库服务器: {self.rmdb_path} {db_name}")
            
            # 启动前确保端口未被占用
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind((self.server_host, self.server_port))
            except:
                self.log(f"端口 {self.server_port} 已被占用")
                return False
                
            # 启动服务器进程
            self.server_process = subprocess.Popen(
                [self.rmdb_path, db_name],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            # 等待服务器启动
            start_time = time.time()
            while time.time() - start_time < self.startup_timeout:
                # 检查进程是否存活
                if self.server_process.poll() is not None:
                    stdout, stderr = self.server_process.communicate()
                    self.log(f"服务器启动失败，退出码: {self.server_process.returncode}")
                    if stdout: self.log(f"输出: {stdout}")
                    if stderr: self.log(f"错误: {stderr}")
                    return False
                    
                # 尝试连接
                try:
                    success, response = self.send_sql("SELECT 1;", timeout=1)
                    if success:
                        self.log(f"服务器启动成功，耗时: {time.time() - start_time:.3f}秒")
                        return True
                except:
                    pass
                    
                time.sleep(0.1)
                
            self.log("服务器启动超时")
            return False
            
        except Exception as e:
            self.log(f"启动服务器时发生错误: {str(e)}")
            return False

    def stop_server(self):
        """停止数据库服务器"""
        if self.server_process:
            self.log("停止数据库服务器")
            self.server_process.terminate()
            try:
                self.server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.server_process.kill()
            self.server_process = None

    def crash_server(self):
        """模拟服务器崩溃"""
        if self.server_process:
            self.log("模拟服务器崩溃")
            self.server_process.kill()
            self.server_process = None

    def send_sql(self, sql: str, timeout: int = 30) -> Tuple[bool, str]:
        """发送SQL命令到服务器"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect((self.server_host, self.server_port))

                # 发送SQL命令
                sock.sendall((sql + '\0').encode('utf-8'))

                # 接收响应
                data = sock.recv(self.buffer_size)
                response = data.decode('utf-8').strip()

                return True, response

        except Exception as e:
            return False, str(e)

    def wait_for_server_recovery(self, test_query: str = "SELECT 1;") -> Tuple[float, Dict]:
        """等待服务器恢复并记录恢复时间和IO统计"""
        self.log("开始等待服务器恢复...")
        
        stats = {
            'total_io': 0,
            'read_pages': 0,
            'write_pages': 0,
            'recovery_phases': []
        }
        
        start_time = time.time()
        last_log_time = start_time
        phase = "unknown"
        
        while time.time() - start_time < self.recovery_timeout:
            try:
                # 尝试连接并执行测试查询
                success, response = self.send_sql(test_query, timeout=1)
                if success and "error" not in response.lower():
                    recovery_time = time.time() - start_time
                    self.log(f"服务器恢复成功，总恢复时间: {recovery_time:.3f}秒")
                    
                    # 尝试获取IO统计
                    _, io_stats = self.send_sql("SHOW IO STATISTICS;")
                    if io_stats and isinstance(io_stats, str):
                        try:
                            stats.update(json.loads(io_stats))
                        except:
                            pass
                            
                    return recovery_time, stats
                    
                # 检查恢复阶段
                _, phase_info = self.send_sql("SHOW RECOVERY PHASE;")
                if phase_info and phase_info != phase:
                    phase = phase_info
                    stats['recovery_phases'].append({
                        'phase': phase,
                        'time': time.time() - start_time
                    })
                    
                # 每10秒记录一次进度
                if time.time() - last_log_time >= 10:
                    self.log(f"恢复仍在进行中... 当前阶段: {phase}")
                    last_log_time = time.time()
                    
            except Exception as e:
                pass
                
            time.sleep(self.check_interval)
            
        self.log("服务器恢复超时")
        return -1, stats

    def generate_test_data(self, table_name: str, num_records: int) -> List[str]:
        """生成测试数据SQL"""
        sql_statements = []

        # 创建表结构（基于题目中的表结构）
        if table_name == "warehouse":
            sql_statements.append(f"""
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
);
""")
        elif table_name == "district":
            sql_statements.append(f"""
CREATE TABLE district (
    d_id INT,
    d_w_id INT,
    d_name CHAR(10),
    d_street_1 CHAR(20),
    d_street_2 CHAR(20),
    d_city CHAR(20),
    d_state CHAR(2),
    d_zip CHAR(9),
    d_tax FLOAT,
    d_ytd FLOAT,
    d_next_o_id INT
);
""")
        elif table_name == "customer":
            sql_statements.append(f"""
CREATE TABLE customer (
    c_id INT,
    c_d_id INT,
    c_w_id INT,
    c_first CHAR(16),
    c_middle CHAR(2),
    c_last CHAR(16),
    c_street_1 CHAR(20),
    c_street_2 CHAR(20),
    c_city CHAR(20),
    c_state CHAR(2),
    c_zip CHAR(9),
    c_phone CHAR(16),
    c_since CHAR(30),
    c_credit CHAR(2),
    c_credit_lim INT,
    c_discount FLOAT,
    c_balance FLOAT,
    c_ytd_payment FLOAT,
    c_payment_cnt INT,
    c_delivery_cnt INT,
    c_data CHAR(50)
);
""")

        # 生成插入数据
        for i in range(1, num_records + 1):
            if table_name == "warehouse":
                sql_statements.append(f"""
INSERT INTO warehouse VALUES (
    {i},
    'Warehouse{i:03d}',
    'Street1_{i}',
    'Street2_{i}',
    'City{i}',
    'ST',
    '12345-{i:04d}',
    {0.05 + (i % 10) * 0.01},
    {1000.0 + i * 10.0}
);
""")
            elif table_name == "district":
                sql_statements.append(f"""
INSERT INTO district VALUES (
    {i},
    {i % 10 + 1},
    'District{i:03d}',
    'Street1_{i}',
    'Street2_{i}',
    'City{i}',
    'ST',
    '12345-{i:04d}',
    {0.04 + (i % 8) * 0.01},
    {500.0 + i * 5.0},
    {1000 + i}
);
""")
            elif table_name == "customer":
                sql_statements.append(f"""
INSERT INTO customer VALUES (
    {i},
    {i % 10 + 1},
    {i % 5 + 1},
    'First{i:03d}',
    'M',
    'Last{i:03d}',
    'Street1_{i}',
    'Street2_{i}',
    'City{i}',
    'ST',
    '12345-{i:04d}',
    '555-{i:04d}',
    '2023-01-01',
    'GC',
    {1000 + i * 100},
    {0.05 + (i % 10) * 0.01},
    {100.0 + i * 2.0},
    {50.0 + i},
    {i % 10},
    {i % 5},
    'Customer data {i}'
);
""")

        return sql_statements

    def generate_transaction_queries(self, num_transactions: int, batch_size: int = 10) -> List[str]:
        """生成事务查询SQL，支持批量操作"""
        sql_statements = []
        
        for i in range(0, num_transactions, batch_size):
            # 开始事务
            sql_statements.append("BEGIN;")
            
            # 每个事务中执行多个操作
            for j in range(min(batch_size, num_transactions - i)):
                # 随机选择操作类型及其权重
                operation = random.choices(
                    ['select', 'update', 'insert', 'delete'],
                    weights=[0.4, 0.3, 0.2, 0.1]
                )[0]
                
                if operation == 'select':
                    # 批量查询
                    table = random.choice(['warehouse', 'district', 'customer'])
                    id_range = random.randint(1, 90)
                    sql_statements.append(f"SELECT * FROM {table} WHERE {table[0]}_id BETWEEN {id_range} AND {id_range + 10};")
                    
                elif operation == 'update':
                    # 批量更新
                    if random.random() < 0.5:
                        start_id = random.randint(1, 95)
                        sql_statements.append(f"UPDATE warehouse SET w_ytd = w_ytd + {random.randint(10, 100)} WHERE w_id BETWEEN {start_id} AND {start_id + 5};")
                    else:
                        start_id = random.randint(1, 90)
                        sql_statements.append(f"UPDATE district SET d_ytd = d_ytd + {random.randint(5, 50)} WHERE d_id BETWEEN {start_id} AND {start_id + 10};")
                        
                elif operation == 'insert':
                    # 批量插入
                    values = []
                    base_id = 1000 + i + j * 10
                    for k in range(5):  # 每次插入5条
                        values.append(f"""(
                            {base_id + k},
                            'NewWH{base_id + k:04d}',
                            'Street1_{base_id + k}',
                            'Street2_{base_id + k}',
                            'City{base_id + k}',
                            'ST',
                            '54321-{k:04d}',
                            {0.06 + (k % 10) * 0.01},
                            {2000.0 + k * 10.0}
                        )""")
                    sql_statements.append(f"INSERT INTO warehouse VALUES {','.join(values)};")
                    
                elif operation == 'delete':
                    # 批量删除
                    start_id = random.randint(1000, 2000)
                    sql_statements.append(f"DELETE FROM warehouse WHERE w_id BETWEEN {start_id} AND {start_id + 5};")
            
            # 提交事务
            sql_statements.append("COMMIT;")
            
        return sql_statements
        
    def run_test_stage(self, stage_name: str, sql_statements: List[str],
                      expect_crash: bool = False, checkpoint_interval: int = 0) -> Dict:
        """运行测试阶段"""
        self.log(f"开始测试阶段: {stage_name}")
        
        results = {
            'stage': stage_name,
            'total_statements': len(sql_statements),
            'executed_statements': 0,
            'success_count': 0,
            'error_count': 0,
            'execution_time': 0,
            'crash_time': None,
            'recovery_time': None,
            'io_stats': {},          # IO统计
            'checkpoint_times': [],   # 检查点时间点
            'errors': []             # 详细错误信息
        }
        
        start_time = time.time()
        last_checkpoint_time = start_time
        
        try:
            for i, sql in enumerate(sql_statements):
                # 检查是否需要创建检查点
                if checkpoint_interval > 0 and i > 0:
                    current_time = time.time()
                    if current_time - last_checkpoint_time >= checkpoint_interval:
                        self.log(f"创建静态检查点 (语句 {i})")
                        success, response = self.send_sql("CREATE STATIC_CHECKPOINT;")
                        if success:
                            checkpoint_time = time.time() - start_time
                            results['checkpoint_times'].append({
                                'statement_index': i,
                                'time': checkpoint_time,
                                'duration': time.time() - current_time
                            })
                            self.log(f"检查点创建成功，耗时: {time.time() - current_time:.3f}秒")
                            last_checkpoint_time = current_time
                        else:
                            self.log(f"检查点创建失败: {response}")
                            results['errors'].append({
                                'type': 'checkpoint_error',
                                'statement_index': i,
                                'error': response
                            })
                
                # 执行SQL语句
                start_exec_time = time.time()
                success, response = self.send_sql(sql.strip())
                exec_duration = time.time() - start_exec_time
                
                results['executed_statements'] += 1
                
                if success:
                    results['success_count'] += 1
                    # 记录长时间运行的语句
                    if exec_duration > 1.0:
                        self.log(f"警告: 语句执行时间较长 ({exec_duration:.3f}秒): {sql[:100]}...")
                else:
                    results['error_count'] += 1
                    results['errors'].append({
                        'type': 'sql_error',
                        'statement_index': i,
                        'sql': sql,
                        'error': response
                    })
                    self.log(f"SQL执行错误: {response}")
                
                # 定期输出进度和性能指标
                if (i + 1) % 100 == 0:
                    elapsed = time.time() - start_time
                    rate = (i + 1) / elapsed
                    remaining = (len(sql_statements) - (i + 1)) / rate if rate > 0 else 0
                    self.log(
                        f"进度: {i + 1}/{len(sql_statements)} 语句"
                        f" ({(i + 1)/len(sql_statements)*100:.1f}%)"
                        f" 速率: {rate:.1f} 语句/秒"
                        f" 预计剩余时间: {remaining:.1f}秒"
                    )
                
                # 模拟崩溃
                if expect_crash and i > len(sql_statements) // 2:
                    # 根据已执行语句数动态调整崩溃概率
                    crash_prob = 0.05 * (i - len(sql_statements)//2) / (len(sql_statements)//2)
                    if random.random() < crash_prob:
                        results['crash_time'] = time.time() - start_time
                        self.log(
                            f"模拟系统崩溃 (语句 {i}, "
                            f"执行进度 {i/len(sql_statements)*100:.1f}%, "
                            f"运行时间 {results['crash_time']:.1f}秒)"
                        )
                        self.crash_server()
                        break
                        
            results['execution_time'] = time.time() - start_time
            
            # 如果发生崩溃，等待恢复
            if results['crash_time'] is not None:
                self.log("等待系统恢复...")
                recovery_time, io_stats = self.wait_for_server_recovery()
                results['recovery_time'] = recovery_time
                results['io_stats'].update(io_stats)
                
                if recovery_time > 0:
                    self.log(
                        f"系统恢复成功:\n"
                        f"- 恢复时间: {recovery_time:.3f}秒\n"
                        f"- 读取页数: {io_stats.get('read_pages', 'N/A')}\n"
                        f"- 写入页数: {io_stats.get('write_pages', 'N/A')}\n"
                        f"- 总IO次数: {io_stats.get('total_io', 'N/A')}"
                    )
                else:
                    self.log("系统恢复失败")
                    
        except Exception as e:
            self.log(f"测试阶段执行异常: {str(e)}")
            results['error_count'] += 1
            results['errors'].append({
                'type': 'stage_error',
                'error': str(e)
            })
            
        self.log(f"测试阶段 {stage_name} 完成")
        return results

    def test_crash_recovery_single_thread(self) -> Dict:
        """测试点1: 单线程发送事务，数据量较小，不包括建立检查点"""
        self.log("=== 测试点1: 单线程故障恢复测试 ===")

        # 启动服务器
        if not self.start_server("single_thread_test"):
            return {'error': '服务器启动失败'}

        try:
            # 生成测试数据
            create_table_sql = self.generate_test_data("warehouse", 50)
            transaction_sql = self.generate_transaction_queries(20)

            # 执行创建表和插入数据
            results1 = self.run_test_stage("创建表和插入数据", create_table_sql)

            # 执行事务操作
            results2 = self.run_test_stage("事务操作", transaction_sql, expect_crash=True)

            # 验证数据一致性
            consistency_check = self.check_data_consistency()

            return {
                'test_point': 'crash_recovery_single_thread_test',
                'stage1': results1,
                'stage2': results2,
                'consistency_check': consistency_check,
                'overall_success': consistency_check['consistent']
            }

        finally:
            self.stop_server()

    def test_crash_recovery_multi_thread(self) -> Dict:
        """测试点2: 多线程发送事务，数据量较小，不包括建立检查点"""
        self.log("=== 测试点2: 多线程故障恢复测试 ===")

        if not self.start_server("multi_thread_test"):
            return {'error': '服务器启动失败'}

        try:
            # 步骤1: 创建表和插入初始数据
            self.log("第一阶段: 创建表和插入初始数据")
            create_table_sql = self.generate_test_data("warehouse", 200)  # 增加初始数据量
            create_table_sql.extend(self.generate_test_data("district", 100))
            
            results_init = self.run_test_stage("初始化数据", create_table_sql)
            if results_init.get('error_count', 0) > 0:
                return {'error': '初始化数据失败', 'details': results_init}

            # 步骤2: 多线程执行事务
            self.log("第二阶段: 多线程执行事务")
            
            # 线程同步对象
            thread_start_event = threading.Event()
            thread_crash_event = threading.Event()
            thread_results_lock = threading.Lock()
            thread_results = []
            
            def worker_thread(thread_id: int, num_transactions: int):
                thread_results_local = {
                    'thread_id': thread_id,
                    'transactions_executed': 0,
                    'errors': [],
                    'start_time': None,
                    'end_time': None
                }
                
                try:
                    # 等待所有线程就绪
                    thread_start_event.wait()
                    thread_results_local['start_time'] = time.time()
                    
                    # 生成并执行事务
                    transaction_sql = self.generate_transaction_queries(
                        num_transactions,
                        batch_size=5  # 较小的batch size以增加并发性
                    )
                    
                    for i, sql in enumerate(transaction_sql):
                        # 检查是否已经发生crash
                        if thread_crash_event.is_set():
                            self.log(f"线程{thread_id}检测到系统crash，停止执行")
                            break
                            
                        # 执行SQL
                        success, response = self.send_sql(sql.strip())
                        
                        if success:
                            thread_results_local['transactions_executed'] += 1
                        else:
                            thread_results_local['errors'].append({
                                'sql': sql,
                                'error': response
                            })
                            
                        # 模拟崩溃（只在主线程中触发）
                        if thread_id == 0 and i > num_transactions // 2:
                            if random.random() < 0.1:  # 10%概率崩溃
                                self.log(f"主线程触发系统crash (执行了 {i} 个事务)")
                                thread_crash_event.set()
                                self.crash_server()
                                break
                                
                except Exception as e:
                    thread_results_local['errors'].append({
                        'type': 'thread_error',
                        'error': str(e)
                    })
                finally:
                    thread_results_local['end_time'] = time.time()
                    with thread_results_lock:
                        thread_results.append(thread_results_local)

            # 创建并启动线程
            threads = []
            num_threads = 4  # 增加线程数
            transactions_per_thread = 50  # 增加每个线程的事务数
            
            for i in range(num_threads):
                thread = threading.Thread(
                    target=worker_thread,
                    args=(i, transactions_per_thread),
                    name=f"TransactionThread-{i}"
                )
                threads.append(thread)
                thread.start()

            # 等待所有线程就绪后开始执行
            time.sleep(1)
            start_time = time.time()
            thread_start_event.set()
            
            # 等待所有线程完成
            for thread in threads:
                thread.join(timeout=60)  # 设置超时以防止永久等待
                
            end_time = time.time()
            
            # 如果发生了crash，等待恢复
            if thread_crash_event.is_set():
                self.log("等待系统恢复...")
                recovery_time, io_stats = self.wait_for_server_recovery()
                
                if recovery_time > 0:
                    self.log(
                        f"系统恢复成功:\n"
                        f"- 恢复时间: {recovery_time:.3f}秒\n"
                        f"- IO统计: {io_stats}"
                    )
                else:
                    return {'error': '系统恢复失败'}
                    
            # 步骤3: 验证数据一致性
            self.log("第三阶段: 验证数据一致性")
            consistency_check = self.check_data_consistency()
            
            # 汇总结果
            summary = {
                'test_point': 'crash_recovery_multi_thread_test',
                'initialization': results_init,
                'thread_results': thread_results,
                'execution_time': end_time - start_time,
                'crash_occurred': thread_crash_event.is_set(),
                'recovery_time': recovery_time if thread_crash_event.is_set() else None,
                'io_stats': io_stats if thread_crash_event.is_set() else None,
                'consistency_check': consistency_check,
                'threads_completed': sum(1 for t in threads if not t.is_alive()),
                'total_transactions_executed': sum(r.get('transactions_executed', 0) for r in thread_results),
                'total_errors': sum(len(r.get('errors', [])) for r in thread_results)
            }
            
            # 判断测试是否成功
            summary['overall_success'] = (
                not thread_crash_event.is_set() or  # 没有发生crash
                (recovery_time > 0 and consistency_check.get('consistent', False))  # 或者成功恢复且数据一致
            )
            
            return summary

        except Exception as e:
            self.log(f"多线程测试发生异常: {str(e)}")
            return {'error': str(e)}
            
        finally:
            self.stop_server()

    def test_crash_recovery_index(self) -> Dict:
        """测试点3: 单线程发送事务，包含建立索引，数据量较大，不包括建立检查点"""
        self.log("=== 测试点3: 索引故障恢复测试 ===")

        if not self.start_server("index_test"):
            return {'error': '服务器启动失败'}

        try:
            # 生成大量测试数据
            create_table_sql = self.generate_test_data("warehouse", 1000)
            create_table_sql.extend(self.generate_test_data("district", 500))

            # 创建索引
            index_sql = [
                "CREATE INDEX idx_warehouse_w_id ON warehouse(w_id);",
                "CREATE INDEX idx_warehouse_w_city ON warehouse(w_city);",
                "CREATE INDEX idx_district_d_id ON district(d_id);",
                "CREATE INDEX idx_district_d_w_id ON district(d_w_id);"
            ]

            # 生成查询操作
            query_sql = []
            for i in range(100):
                query_sql.append(f"SELECT * FROM warehouse WHERE w_id = {random.randint(1, 1000)};")
                query_sql.append(f"SELECT * FROM district WHERE d_id = {random.randint(1, 500)};")

            # 执行测试
            results1 = self.run_test_stage("创建表和插入数据", create_table_sql)
            results2 = self.run_test_stage("创建索引", index_sql)
            results3 = self.run_test_stage("索引查询", query_sql, expect_crash=True)

            consistency_check = self.check_data_consistency()

            return {
                'test_point': 'crash_recovery_index_test',
                'stage1': results1,
                'stage2': results2,
                'stage3': results3,
                'consistency_check': consistency_check,
                'overall_success': consistency_check['consistent']
            }

        finally:
            self.stop_server()

    def test_crash_recovery_large_data(self) -> Dict:
        """测试点4: 多线程发送事务，数据量较大，不包括建立检查点"""
        self.log("=== 测试点4: 大数据量故障恢复测试 ===")

        if not self.start_server("large_data_test"):
            return {'error': '服务器启动失败'}

        try:
            # 生成大量测试数据
            create_table_sql = self.generate_test_data("warehouse", 2000)
            create_table_sql.extend(self.generate_test_data("district", 1000))
            create_table_sql.extend(self.generate_test_data("customer", 5000))

            # 多线程执行大量事务
            def large_data_worker(thread_id: int):
                transaction_sql = self.generate_transaction_queries(50)
                return self.run_test_stage(f"大数据线程{thread_id}", transaction_sql)

            threads = []
            thread_results = []

            for i in range(5):  # 5个线程
                thread = threading.Thread(
                    target=lambda tid=i: thread_results.append(large_data_worker(tid))
                )
                threads.append(thread)
                thread.start()

            for thread in threads:
                thread.join()

            consistency_check = self.check_data_consistency()

            return {
                'test_point': 'crash_recovery_large_data_test',
                'thread_results': thread_results,
                'consistency_check': consistency_check,
                'overall_success': consistency_check['consistent']
            }

        finally:
            self.stop_server()

    def test_crash_recovery_without_checkpoint(self) -> Dict:
        """测试点5: 单线程发送事务，数据量巨大，不包括建立检查点，记录恢复时间t1"""
        self.log("=== 测试点5: 无检查点故障恢复测试 ===")

        if not self.start_server("without_checkpoint_test"):
            return {'error': '服务器启动失败'}

        try:
            # 生成巨大数据量
            create_table_sql = self.generate_test_data("warehouse", 5000)
            create_table_sql.extend(self.generate_test_data("district", 2000))
            create_table_sql.extend(self.generate_test_data("customer", 10000))

            # 生成大量事务
            transaction_sql = self.generate_transaction_queries(200)

            # 执行测试
            results1 = self.run_test_stage("创建表和插入数据", create_table_sql)
            results2 = self.run_test_stage("大量事务操作", transaction_sql, expect_crash=True)

            # 记录恢复时间t1
            t1 = results2.get('recovery_time', -1)
            self.recovery_times['t1'] = t1

            consistency_check = self.check_data_consistency()

            return {
                'test_point': 'crash_recovery_without_checkpoint',
                'stage1': results1,
                'stage2': results2,
                'recovery_time_t1': t1,
                'consistency_check': consistency_check,
                'overall_success': consistency_check['consistent']
            }

        finally:
            self.stop_server()

    def test_crash_recovery_with_checkpoint(self) -> Dict:
        """测试点6: 带检查点的故障恢复测试，要求t2 < t1 * 0.7"""
        self.log("=== 测试点6: 带检查点故障恢复测试 ===")

        if not self.start_server("with_checkpoint_test"):
            return {'error': '服务器启动失败'}

        try:
            # 生成巨大数据量（与测试点5相同）
            create_table_sql = self.generate_test_data("warehouse", 5000)
            create_table_sql.extend(self.generate_test_data("district", 2000))
            create_table_sql.extend(self.generate_test_data("customer", 10000))

            # 生成大量事务
            transaction_sql = self.generate_transaction_queries(200)

            # 执行测试，每50个语句创建一个检查点
            results1 = self.run_test_stage("创建表和插入数据", create_table_sql)
            results2 = self.run_test_stage("带检查点事务操作", transaction_sql,
                                         expect_crash=True, checkpoint_interval=50)

            # 记录恢复时间t2
            t2 = results2.get('recovery_time', -1)
            self.recovery_times['t2'] = t2

            # 检查性能要求
            t1 = self.recovery_times.get('t1', float('inf'))
            performance_improvement = t2 < t1 * 0.7 if t1 > 0 and t2 > 0 else False

            consistency_check = self.check_data_consistency()

            return {
                'test_point': 'crash_recovery_with_checkpoint',
                'stage1': results1,
                'stage2': results2,
                'recovery_time_t1': t1,
                'recovery_time_t2': t2,
                'performance_improvement': performance_improvement,
                'consistency_check': consistency_check,
                'overall_success': consistency_check['consistent'] and performance_improvement
            }

        finally:
            self.stop_server()

    def check_data_consistency(self) -> Dict:
        """检查数据一致性"""
        self.log("检查数据一致性...")

        consistency_checks = []

        try:
            # 检查表是否存在
            success, response = self.send_sql("SHOW TABLES;")
            if success:
                consistency_checks.append({
                    'check': 'tables_exist',
                    'result': 'pass',
                    'details': response
                })
            else:
                consistency_checks.append({
                    'check': 'tables_exist',
                    'result': 'fail',
                    'details': response
                })

            # 检查数据完整性
            success, response = self.send_sql("SELECT COUNT(*) FROM warehouse;")
            if success and "error" not in response.lower():
                consistency_checks.append({
                    'check': 'warehouse_count',
                    'result': 'pass',
                    'details': response
                })
            else:
                consistency_checks.append({
                    'check': 'warehouse_count',
                    'result': 'fail',
                    'details': response
                })

            # 检查索引
            success, response = self.send_sql("SELECT * FROM warehouse WHERE w_id = 1;")
            if success and "error" not in response.lower():
                consistency_checks.append({
                    'check': 'basic_query',
                    'result': 'pass',
                    'details': 'Basic query works'
                })
            else:
                consistency_checks.append({
                    'check': 'basic_query',
                    'result': 'fail',
                    'details': response
                })

        except Exception as e:
            consistency_checks.append({
                'check': 'consistency_check',
                'result': 'error',
                'details': str(e)
            })

        # 判断整体一致性
        all_passed = all(check['result'] == 'pass' for check in consistency_checks)

        return {
            'consistent': all_passed,
            'checks': consistency_checks
        }

    def run_all_tests(self) -> Dict:
        """运行所有测试点"""
        self.log("开始运行所有故障恢复测试...")

        all_results = {}

        # 运行6个测试点
        test_functions = [
            self.test_crash_recovery_single_thread,
            self.test_crash_recovery_multi_thread,
            self.test_crash_recovery_index,
            self.test_crash_recovery_large_data,
            self.test_crash_recovery_without_checkpoint,
            self.test_crash_recovery_with_checkpoint
        ]

        for i, test_func in enumerate(test_functions, 1):
            self.log(f"\n{'='*50}")
            self.log(f"开始测试点 {i}")
            self.log(f"{'='*50}")

            try:
                result = test_func()
                all_results[f'test_point_{i}'] = result

                if result.get('overall_success', False):
                    self.log(f"测试点 {i} 通过")
                else:
                    self.log(f"测试点 {i} 失败")

            except Exception as e:
                self.log(f"测试点 {i} 执行异常: {str(e)}")
                all_results[f'test_point_{i}'] = {'error': str(e)}

            # 测试点之间稍作休息
            time.sleep(2)

        # 生成测试报告
        self.generate_test_report(all_results)

        return all_results

    def generate_test_report(self, results: Dict):
        """生成测试报告"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        report_file = os.path.join(self.test_dir, f'crash_recovery_report_{timestamp}.json')

        # 统计结果
        summary = {
            'total_tests': 6,
            'passed_tests': 0,
            'failed_tests': 0,
            'recovery_times': self.recovery_times,
            'test_results': results
        }

        for test_name, result in results.items():
            if result.get('overall_success', False):
                summary['passed_tests'] += 1
            else:
                summary['failed_tests'] += 1

        # 保存报告
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)

        # 输出摘要
        self.log(f"\n{'='*60}")
        self.log("测试报告摘要")
        self.log(f"{'='*60}")
        self.log(f"总测试数: {summary['total_tests']}")
        self.log(f"通过测试: {summary['passed_tests']}")
        self.log(f"失败测试: {summary['failed_tests']}")

        if 't1' in self.recovery_times and 't2' in self.recovery_times:
            t1 = self.recovery_times['t1']
            t2 = self.recovery_times['t2']
            if t1 > 0 and t2 > 0:
                improvement = (t1 - t2) / t1 * 100
                self.log(f"恢复时间对比:")
                self.log(f"  无检查点恢复时间 (t1): {t1:.3f}秒")
                self.log(f"  有检查点恢复时间 (t2): {t2:.3f}秒")
                self.log(f"  性能提升: {improvement:.1f}%")

                if t2 < t1 * 0.7:
                    self.log("✓ 检查点性能要求满足 (t2 < t1 * 0.7)")
                else:
                    self.log("✗ 检查点性能要求不满足")

        self.log(f"详细报告已保存到: {report_file}")
        self.log(f"{'='*60}")


def main():
    """主函数"""
    print("基于静态检查点的故障恢复测试脚本")
    print("=" * 50)

    # 检查必要文件
    rmdb_path = "./bin/rmdb"
    client_path = "./rmdb_client/build/rmdb_client"

    if not os.path.exists(rmdb_path):
        print(f"错误: 数据库服务器不存在 - {rmdb_path}")
        print("请先编译数据库服务器")
        return 1

    if not os.path.exists(client_path):
        print(f"错误: 客户端不存在 - {client_path}")
        print("请先编译客户端")
        return 1

    # 创建测试器并运行测试
    tester = CrashRecoveryTester(rmdb_path, client_path)

    try:
        results = tester.run_all_tests()

        # 检查整体结果
        passed_count = sum(1 for result in results.values()
                          if result.get('overall_success', False))

        if passed_count == 6:
            print("\n🎉 所有测试点都通过了！")
            return 0
        else:
            print(f"\n⚠️  有 {6 - passed_count} 个测试点失败")
            return 1

    except KeyboardInterrupt:
        print("\n测试被用户中断")
        return 1
    except Exception as e:
        print(f"\n测试执行异常: {str(e)}")
        return 1
    finally:
        tester.stop_server()


if __name__ == "__main__":
    sys.exit(main())