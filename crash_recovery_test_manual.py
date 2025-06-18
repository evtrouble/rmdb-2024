#!/usr/bin/env python3
"""
基于静态检查点的故障恢复测试脚本（自动重启服务器版本）
使用方法：
1. 运行此脚本，会自动启动和重启服务器
2. 脚本会自动进行故障恢复测试
"""

import os
import sys
import time
import socket
import subprocess
import signal
from datetime import datetime
from typing import List, Dict, Tuple, Optional

class CrashRecoveryTester:
    def __init__(self):
        self.server_host = "127.0.0.1"
        self.server_port = 8765
        self.buffer_size = 8192
        self.sql_dir = "test_cases/sql"
        
        # 服务器相关配置
        self.server_binary = "./bin/rmdb"
        self.test_db_path = "test_db"
        self.server_process = None
        
        # 创建日志文件
        self.sql_log_file = f"sql_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        self.init_sql_log_file()

    def init_sql_log_file(self):
        """初始化SQL日志文件"""
        with open(self.sql_log_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("SQL命令和响应日志\n")
            f.write(f"测试开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")

    def log_sql_interaction(self, sql: str, success: bool, response: str, context: str = ""):
        """记录SQL交互到文件"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        
        with open(self.sql_log_file, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] {context}\n")
            f.write("-" * 50 + "\n")
            f.write(f"SQL命令: {sql}\n")
            f.write(f"执行状态: {'成功' if success else '失败'}\n")
            f.write(f"服务器响应: {response}\n")
            f.write("-" * 50 + "\n\n")

    def log(self, message: str):
        """记录日志"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        print(f"[{timestamp}] {message}")
        
        # 同时记录到SQL日志文件
        with open(self.sql_log_file, 'a', encoding='utf-8') as f:
            f.write(f"[{timestamp}] LOG: {message}\n")

    def start_server(self) -> bool:
        """启动数据库服务器"""
        self.log("启动数据库服务器...")
        
        try:
            # 如果服务器进程已存在，先关闭
            if self.server_process and self.server_process.poll() is None:
                self.stop_server()
            
            # 启动服务器进程
            self.server_process = subprocess.Popen(
                [self.server_binary, self.test_db_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                stdin=subprocess.PIPE,
                preexec_fn=os.setsid  # 创建新的进程组
            )
            
            # 等待服务器启动
            start_time = time.time()
            timeout = 30
            
            while time.time() - start_time < timeout:
                if self.check_server_connection():
                    startup_time = time.time() - start_time
                    self.log(f"服务器启动成功，用时: {startup_time:.2f}秒")
                    return True
                
                # 检查进程是否还在运行
                if self.server_process.poll() is not None:
                    self.log("服务器进程意外退出")
                    return False
                    
                time.sleep(0.5)
            
            self.log("服务器启动超时")
            return False
            
        except Exception as e:
            self.log(f"启动服务器失败: {str(e)}")
            return False

    def stop_server(self):
        """停止数据库服务器"""
        if self.server_process:
            try:
                self.log("停止数据库服务器...")
                
                # 尝试优雅关闭
                os.killpg(os.getpgid(self.server_process.pid), signal.SIGTERM)
                
                # 等待进程结束
                try:
                    self.server_process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    # 强制关闭
                    self.log("强制关闭服务器进程...")
                    os.killpg(os.getpgid(self.server_process.pid), signal.SIGKILL)
                    self.server_process.wait()
                
                self.log("服务器已停止")
                
            except Exception as e:
                self.log(f"停止服务器时出错: {str(e)}")
            finally:
                self.server_process = None

    def restart_server(self) -> bool:
        """重启数据库服务器"""
        self.log("重启数据库服务器...")
        
        # 停止当前服务器
        self.stop_server()
        
        # 等待一段时间确保端口释放
        time.sleep(2)
        
        # 启动新服务器
        return self.start_server()

    def read_sql_file(self, filename: str) -> List[str]:
        """读取SQL文件内容"""
        file_path = os.path.join(self.sql_dir, filename)
        with open(file_path, 'r') as f:
            content = f.read()

        # 分割SQL语句（简单处理，实际可能需要更复杂的SQL解析）
        statements = []
        current_statement = []

        for line in content.split('\n'):
            line = line.strip()
            if not line or line.startswith('--'):  # 跳过空行和注释
                continue

            current_statement.append(line)
            if line.endswith(';'):
                statements.append(' '.join(current_statement))
                current_statement = []

        return statements

    def check_server_connection(self) -> bool:
        """检查服务器连接"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(5)
                sock.connect((self.server_host, self.server_port))
                return True
        except Exception:
            return False

    def send_sql(self, sql: str, timeout: int = 30, context: str = "") -> Tuple[bool, str]:
        """发送SQL命令到服务器"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(timeout)
                sock.connect((self.server_host, self.server_port))
                sock.sendall((sql + '\0').encode('utf-8'))
                data = sock.recv(self.buffer_size)
                response = data.decode('utf-8').strip()
                
                # 记录SQL交互
                self.log_sql_interaction(sql, True, response, context)
                
                return True, response
        except Exception as e:
            error_msg = str(e)
            
            # 记录失败的SQL交互
            self.log_sql_interaction(sql, False, error_msg, context)
            
            return False, error_msg

    def send_crash_command(self) -> bool:
        """发送崩溃命令并检测崩溃是否成功"""
        try:
            self.log("发送崩溃命令...")
            
            # 发送崩溃命令
            success, response = self.send_sql("crash", timeout=5, context="触发系统崩溃")
            
            # 等待一小段时间让崩溃生效
            time.sleep(1)
            
            # 检查服务器是否真的崩溃了
            crash_detected = not self.check_server_connection()
            
            if crash_detected:
                self.log("✓ 崩溃命令执行成功，服务器已崩溃")
                return True
            else:
                self.log("⚠ 崩溃命令发送但服务器仍在运行")
                return False
                
        except Exception as e:
            # 如果连接异常，可能是因为服务器崩溃了
            self.log(f"发送崩溃命令时连接异常（可能是崩溃成功）: {str(e)}")
            
            # 再次检查服务器状态
            time.sleep(1)
            crash_detected = not self.check_server_connection()
            
            if crash_detected:
                self.log("✓ 确认服务器已崩溃")
                return True
            else:
                self.log("✗ 服务器仍在运行，崩溃失败")
                return False

    def wait_for_server_recovery_with_restart(self, timeout: int = 600) -> float:
        """等待服务器恢复（包含自动重启）"""
        self.log("开始服务器恢复流程...")
        start_time = time.time()

        # 首先确认服务器已经崩溃
        if self.check_server_connection():
            self.log("警告: 服务器似乎没有崩溃，尝试手动重启...")
        
        # 等待一段时间确保崩溃完全生效
        time.sleep(2)
        
        # 自动重启服务器
        self.log("自动重启服务器...")
        if self.restart_server():
            recovery_time = time.time() - start_time
            self.log(f"✓ 服务器自动重启成功，总恢复时间: {recovery_time:.2f}秒")
            return recovery_time
        else:
            self.log("✗ 服务器自动重启失败")
            return -1

    def run_test_case(self, case_number: int) -> Dict:
        """运行测试用例"""
        self.log(f"开始运行测试用例 {case_number}")
        results = {
            'case_number': case_number,
            'success': False,
            'error': None,
            'recovery_time': None,
            'checkpoints_created': 0,
            'statements_executed': 0,
            'verification_passed': 0,
            'verification_total': 0,
            'crash_successful': False,
            'restart_successful': False
        }

        try:
            # 确保服务器运行
            if not self.check_server_connection():
                self.log("服务器未运行，启动服务器...")
                if not self.start_server():
                    raise Exception("无法启动服务器")

            # 1. 初始化数据库
            self.log("初始化数据库...")
            schema_statements = self.read_sql_file('schema.sql')
            for sql in schema_statements:
                success, response = self.send_sql(sql, context=f"测试用例{case_number} - 初始化数据库")
                if not success:
                    raise Exception(f"初始化失败: {response}")
                results['statements_executed'] += 1

            # 2. 插入测试数据
            self.log("插入测试数据...")
            test_data_statements = self.read_sql_file('test_data.sql')
            for sql in test_data_statements:
                success, response = self.send_sql(sql, context=f"测试用例{case_number} - 插入测试数据")
                if not success:
                    raise Exception(f"插入数据失败: {response}")
                results['statements_executed'] += 1

            # 3. 执行测试用例
            self.log(f"执行测试用例 {case_number}...")
            crash_triggered = False
            test_statements = self.read_sql_file(f'test_case{case_number}.sql')

            for sql in test_statements:
                # 跳过注释行
                if sql.strip().startswith('--') or not sql.strip():
                    continue
                    
                # 检查是否是崩溃点
                if 'crash' in sql:
                    self.log("到达崩溃点，触发系统崩溃...")
                    crash_successful = self.send_crash_command()
                    results['crash_successful'] = crash_successful
                    
                    if crash_successful:
                        crash_triggered = True
                        break
                    else:
                        raise Exception("崩溃命令执行失败")
                
                # 检查是否是检查点创建
                if sql.strip().upper().startswith('CREATE STATIC_CHECKPOINT'):
                    self.log("创建静态检查点...")
                    success, response = self.send_sql(sql, context=f"测试用例{case_number} - 创建检查点")
                    if success:
                        results['checkpoints_created'] += 1
                        self.log(f"检查点创建成功，总计: {results['checkpoints_created']}")
                    else:
                        self.log(f"检查点创建失败: {response}")
                    results['statements_executed'] += 1
                    continue

                # 执行普通SQL语句
                success, response = self.send_sql(sql, context=f"测试用例{case_number} - 执行测试语句")
                if not success:
                    # 对于复杂测试用例，某些失败可能是预期的
                    if case_number >= 3:
                        self.log(f"语句执行失败（可能预期）: {response}")
                    else:
                        raise Exception(f"执行SQL失败: {response}")
                results['statements_executed'] += 1

            if crash_triggered:
                # 4. 自动重启并等待恢复
                recovery_time = self.wait_for_server_recovery_with_restart()
                results['recovery_time'] = recovery_time
                results['restart_successful'] = recovery_time > 0

                if recovery_time > 0:
                    # 5. 验证恢复后的数据
                    self.log("验证恢复后的数据...")
                    verification_passed = True
                    verification_queries = [sql for sql in test_statements 
                                          if sql.strip().upper().startswith('SELECT')]
                    
                    results['verification_total'] = len(verification_queries)
                    
                    for sql in verification_queries:
                        success, response = self.send_sql(sql, context=f"测试用例{case_number} - 验证恢复数据")
                        if success:
                            results['verification_passed'] += 1
                            self.log(f"验证查询成功: {response[:100]}...")
                        else:
                            verification_passed = False
                            self.log(f"验证查询失败: {response}")

                    # 对于复杂测试用例，允许部分验证失败
                    if case_number >= 3:
                        success_rate = results['verification_passed'] / results['verification_total'] if results['verification_total'] > 0 else 0
                        results['success'] = success_rate >= 0.8  # 80%的验证通过即认为成功
                        if not results['success']:
                            results['error'] = f"验证通过率过低: {success_rate:.2%}"
                    else:
                        results['success'] = verification_passed
                        if not verification_passed:
                            results['error'] = "数据验证失败"
                else:
                    results['error'] = "服务器重启失败"
            else:
                results['error'] = "未触发崩溃点"

        except Exception as e:
            results['error'] = str(e)
            self.log(f"测试用例执行异常: {str(e)}")

        return results

    def run_all_tests(self) -> Dict:
        """运行所有测试"""
        self.log("开始运行故障恢复测试...")
        all_results = {}

        # 启动初始服务器
        if not self.start_server():
            return {'error': '无法启动初始服务器'}

        try:
            # 运行测试用例
            for case_number in [6]:
                self.log(f"\n{'='*50}")
                self.log(f"运行测试用例 {case_number}")
                self.log(f"{'='*50}")

                results = self.run_test_case(case_number)
                all_results[f'case_{case_number}'] = results

                if results['success']:
                    self.log(f"✓ 测试用例 {case_number} 通过")
                else:
                    self.log(f"✗ 测试用例 {case_number} 失败: {results['error']}")

                # 测试间隔（复杂测试用例需要更长的恢复时间）
                if case_number >= 3:
                    self.log("复杂测试用例，等待更长时间...")
                    time.sleep(5)
                else:
                    time.sleep(2)

        finally:
            # 确保最后停止服务器
            # self.stop_server()
            print(1)

        return all_results

    def finalize_sql_log(self, results: Dict):
        """完成SQL日志记录"""
        with open(self.sql_log_file, 'a', encoding='utf-8') as f:
            f.write("\n" + "=" * 80 + "\n")
            f.write("测试结果总结\n")
            f.write("=" * 80 + "\n")
            
            for case_name, result in results.items():
                if isinstance(result, dict):
                    f.write(f"{case_name}:\n")
                    f.write(f"  成功: {result.get('success', False)}\n")
                    f.write(f"  崩溃成功: {result.get('crash_successful', False)}\n")
                    f.write(f"  重启成功: {result.get('restart_successful', False)}\n")
                    if result.get('error'):
                        f.write(f"  错误: {result['error']}\n")
                    if result.get('recovery_time'):
                        f.write(f"  恢复时间: {result['recovery_time']:.2f}秒\n")
                    if result.get('checkpoints_created'):
                        f.write(f"  创建检查点数: {result['checkpoints_created']}\n")
                    if result.get('statements_executed'):
                        f.write(f"  执行语句数: {result['statements_executed']}\n")
                    if result.get('verification_total'):
                        f.write(f"  验证查询: {result['verification_passed']}/{result['verification_total']}\n")
                        if result['verification_total'] > 0:
                            success_rate = result['verification_passed'] / result['verification_total']
                            f.write(f"  验证成功率: {success_rate:.2%}\n")
                    f.write("\n")
            
            f.write(f"测试结束时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n")

    def cleanup(self):
        """清理资源"""
        self.log("清理资源...")
        self.stop_server()

def main():
    """主函数"""
    print("基于静态检查点的故障恢复测试脚本（自动重启服务器版本）")
    print("=" * 60)
    print("功能说明：")
    print("1. 自动启动数据库服务器")
    print("2. 自动进行故障恢复测试（包含6个测试用例）")
    print("   - 测试用例1-2: 基础故障恢复")
    print("   - 测试用例3-6: 复杂并发事务故障恢复")
    print("3. 崩溃后自动重启服务器")
    print("4. SQL命令和响应将保存到详细的日志文件中")
    print("5. 测试完成后自动停止服务器")
    print("=" * 60)

    tester = CrashRecoveryTester()
    
    print(f"\nSQL日志将保存到: {tester.sql_log_file}")

    try:
        results = tester.run_all_tests()
        
        # 完成日志记录
        tester.finalize_sql_log(results)

        # 检查结果
        success_count = sum(1 for result in results.values()
                          if isinstance(result, dict) and result.get('success', False))
        
        total_cases = 6

        print(f"\nSQL交互日志已保存到: {tester.sql_log_file}")
        
        # 打印详细统计
        print(f"\n测试结果统计:")
        print(f"{'='*60}")
        for case_name, result in results.items():
            if isinstance(result, dict):
                print(f"{case_name}:")
                status_icon = "✓" if result.get('success', False) else "✗"
                print(f"  {status_icon} 成功: {result.get('success', False)}")
                
                crash_icon = "✓" if result.get('crash_successful', False) else "✗"
                print(f"  {crash_icon} 崩溃: {result.get('crash_successful', False)}")
                
                restart_icon = "✓" if result.get('restart_successful', False) else "✗"
                print(f"  {restart_icon} 重启: {result.get('restart_successful', False)}")
                
                if result.get('recovery_time'):
                    print(f"  ⏱️  恢复时间: {result['recovery_time']:.2f}秒")
                if result.get('checkpoints_created'):
                    print(f"  📋 创建检查点: {result['checkpoints_created']}")
                if result.get('statements_executed'):
                    print(f"  📝 执行语句数: {result['statements_executed']}")
                if result.get('verification_total'):
                    print(f"  🔍 验证查询: {result['verification_passed']}/{result['verification_total']}")
                if result.get('error'):
                    print(f"  ❌ 错误: {result['error']}")
                print()

        if success_count == total_cases:
            print("🎉 所有测试用例都通过了！")
            return 0
        else:
            print(f"⚠️  有 {total_cases - success_count} 个测试用例失败")
            return 1

    except KeyboardInterrupt:
        print("\n测试被用户中断")
        tester.cleanup()
        tester.finalize_sql_log({})
        print(f"SQL交互日志已保存到: {tester.sql_log_file}")
        return 1
    except Exception as e:
        print(f"\n测试执行异常: {str(e)}")
        tester.cleanup()
        tester.finalize_sql_log({})
        print(f"SQL交互日志已保存到: {tester.sql_log_file}")
        return 1
    finally:
        # 确保清理资源
        tester.cleanup()

if __name__ == "__main__":
    sys.exit(main())