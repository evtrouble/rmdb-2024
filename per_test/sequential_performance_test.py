#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TPC-C顺序性能测试脚本
用于执行完整的TPC-C性能测试流程（顺序执行，无并发）
"""

import os
import sys
import time
import argparse
import subprocess
import signal
import shutil
from datetime import datetime
from typing import List, Dict, Any, Tuple

# 添加父目录到路径以便导入sql_client
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tpcc_generator import TPCCTransactionGenerator
from sql_client import SQLClient

class TPCCSequentialPerformanceTest:
    def __init__(self, host: str = "localhost", port: int = 5432,
                 num_warehouses: int = 10, output_dir: str = "sqls", dbname: str = "testdb"):
        """
        初始化TPC-C顺序性能测试

        Args:
            host: 数据库服务器地址
            port: 数据库服务器端口
            num_warehouses: 仓库数量
            output_dir: SQL文件输出目录
            dbname: 数据库名称
        """
        self.host = host
        self.port = port
        self.num_warehouses = num_warehouses
        self.output_dir = output_dir
        self.test_dir = output_dir
        self.dbname = dbname

        # 数据库服务器相关
        self.db_server_process = None
        self.perf_process = None
        self.db_path = f"/home/nero/diff/db2025/build/{dbname}"
        self.db_binary = "/home/nero/diff/db2025/build/bin/rmdb"

        # 创建perf输出目录
        self.perf_dir = "./perf"
        os.makedirs(self.perf_dir, exist_ok=True)

        # 为这次测试运行创建唯一的run_id
        self.run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.client = None  # 延迟初始化

        # 创建事务生成器
        self.generator = TPCCTransactionGenerator(
            num_warehouses=num_warehouses,
            output_dir=output_dir
        )

        # 测试结果
        self.results = {
            'load_time': 0,
            'run_time': 0,
            'total_transactions': 0,
            'successful_transactions': 0,
            'failed_transactions': 0,
            'tpmC': 0
        }

        print(f"🚀 TPC-C顺序性能测试初始化完成")
        print(f"  服务器: {host}:{port}")
        print(f"  仓库数量: {num_warehouses}")
        print(f"  输出目录: {output_dir}")
        print(f"  数据库名称: {dbname}")
        print(f"  执行模式: 顺序执行（无并发）")

    def cleanup_database(self):
        """清理数据库目录"""
        if os.path.exists(self.db_path):
            print(f"🗑️ 删除现有数据库目录: {self.db_path}")
            shutil.rmtree(self.db_path)
            print("✅ 数据库目录清理完成")
        else:
            print(f"📁 数据库目录不存在，无需清理: {self.db_path}")

    def start_database_server(self) -> bool:
        """启动数据库服务器"""
        try:
            print(f"🚀 启动数据库服务器: {self.db_binary} {self.dbname}")

            # 启动数据库服务器
            self.db_server_process = subprocess.Popen(
                [self.db_binary, self.dbname],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd="/home/nero/diff/db2025/build"
            )

            # 等待服务器启动
            print("⏳ 等待数据库服务器启动...")
            time.sleep(3)

            # 检查进程是否还在运行
            if self.db_server_process.poll() is None:
                print("✅ 数据库服务器启动成功")
                return True
            else:
                stdout, stderr = self.db_server_process.communicate()
                print(f"❌ 数据库服务器启动失败")
                print(f"stdout: {stdout.decode()}")
                print(f"stderr: {stderr.decode()}")
                return False

        except Exception as e:
            print(f"❌ 启动数据库服务器时出错: {e}")
            return False

    def start_perf_monitoring(self) -> bool:
        """启动perf监控"""
        try:
            if self.db_server_process is None:
                print("❌ 数据库服务器未启动，无法开始perf监控")
                return False

            db_pid = self.db_server_process.pid
            perf_output = os.path.join(self.perf_dir, f"perf_data_{self.run_id}.data")

            print(f"📊 开始perf监控，PID: {db_pid}")

            # 启动perf record
            self.perf_process = subprocess.Popen([
                "perf", "record", "-g", "-F", "4000", "-p", str(db_pid), "-o", perf_output
            ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

            # 等待perf启动
            time.sleep(1)

            if self.perf_process.poll() is None:
                print(f"✅ perf监控启动成功，输出文件: {perf_output}")
                return True
            else:
                stdout, stderr = self.perf_process.communicate()
                print(f"❌ perf监控启动失败")
                print(f"stderr: {stderr.decode()}")
                return False

        except Exception as e:
            print(f"❌ 启动perf监控时出错: {e}")
            return False

    def stop_perf_monitoring(self):
        """停止perf监控"""
        if self.perf_process and self.perf_process.poll() is None:
            print("🛑 停止perf监控...")
            self.perf_process.send_signal(signal.SIGINT)
            try:
                self.perf_process.wait(timeout=10)
                print("✅ perf监控已停止")
            except subprocess.TimeoutExpired:
                print("⚠️ perf进程超时，强制终止")
                self.perf_process.kill()

    def generate_flame_graph(self) -> bool:
        """生成火焰图"""
        try:
            perf_data = os.path.join(self.perf_dir, f"perf_data_{self.run_id}.data")
            flame_graph_svg = os.path.join(self.perf_dir, f"flame_graph_{self.run_id}.svg")

            if not os.path.exists(perf_data):
                print(f"❌ perf数据文件不存在: {perf_data}")
                return False

            print("🔥 生成火焰图...")

            # 使用perf script生成调用栈数据
            perf_script_output = os.path.join(self.perf_dir, f"perf_script_{self.run_id}.txt")

            with open(perf_script_output, 'w') as f:
                result = subprocess.run([
                    "perf", "script", "-i", perf_data
                ], stdout=f, stderr=subprocess.PIPE)

            if result.returncode != 0:
                print(f"❌ perf script执行失败: {result.stderr.decode()}")
                return False

            # 检查是否安装了FlameGraph工具
            flamegraph_path = None
            
            # 首先尝试使用which命令查找stackcollapse-perf.pl
            try:
                result = subprocess.run(["which", "stackcollapse-perf.pl"], 
                                       capture_output=True, text=True)
                if result.returncode == 0:
                    stackcollapse_full_path = result.stdout.strip()
                    flamegraph_path = os.path.dirname(stackcollapse_full_path)
                    print(f"✅ 在PATH中找到FlameGraph工具: {flamegraph_path}")
            except Exception:
                pass
            
            # 如果which命令没找到，尝试常见路径
            if flamegraph_path is None:
                possible_paths = [
                    "/home/nero/FlameGraph",  # 用户确认的绝对路径
                    "/opt/FlameGraph",
                    "/usr/local/FlameGraph", 
                    "./FlameGraph",
                    os.path.expanduser("~/FlameGraph")
                ]

                for path in possible_paths:
                    if os.path.exists(os.path.join(path, "stackcollapse-perf.pl")):
                        flamegraph_path = path
                        print(f"✅ 在固定路径找到FlameGraph工具: {flamegraph_path}")
                        break

            if flamegraph_path is None:
                print("⚠️ 未找到FlameGraph工具，尝试简单的文本报告...")
                print("💡 请确保FlameGraph工具在PATH中或安装在常见路径")
                print("💡 可以尝试: export PATH=\"$HOME/FlameGraph:$PATH\"")
                # 生成简单的perf报告
                report_file = os.path.join(self.perf_dir, f"perf_report_{self.run_id}.txt")
                with open(report_file, 'w') as f:
                    subprocess.run([
                        "perf", "report", "-i", perf_data, "--stdio"
                    ], stdout=f, stderr=subprocess.PIPE)
                print(f"📊 perf报告已生成: {report_file}")
                return True

            # 使用FlameGraph生成火焰图
            stackcollapse = os.path.join(flamegraph_path, "stackcollapse-perf.pl")
            flamegraph_pl = os.path.join(flamegraph_path, "flamegraph.pl")

            # 验证工具文件存在
            if not os.path.exists(stackcollapse):
                print(f"❌ 找不到stackcollapse-perf.pl: {stackcollapse}")
                return False
            if not os.path.exists(flamegraph_pl):
                print(f"❌ 找不到flamegraph.pl: {flamegraph_pl}")
                return False

            # 处理调用栈数据
            collapsed_file = os.path.join(self.perf_dir, f"collapsed_{self.run_id}.txt")

            with open(perf_script_output, 'r') as input_f, open(collapsed_file, 'w') as output_f:
                result = subprocess.run(["perl", stackcollapse], stdin=input_f, stdout=output_f, stderr=subprocess.PIPE)
                if result.returncode != 0:
                    print(f"❌ stackcollapse-perf.pl执行失败: {result.stderr.decode()}")
                    return False

            # 生成火焰图
            with open(collapsed_file, 'r') as input_f, open(flame_graph_svg, 'w') as output_f:
                result = subprocess.run(["perl", flamegraph_pl], stdin=input_f, stdout=output_f, stderr=subprocess.PIPE)
                if result.returncode != 0:
                    print(f"❌ flamegraph.pl执行失败: {result.stderr.decode()}")
                    return False

            print(f"🔥 火焰图已生成: {flame_graph_svg}")

            # 清理临时文件
            for temp_file in [perf_script_output, collapsed_file]:
                if os.path.exists(temp_file):
                    os.remove(temp_file)

            return True

        except Exception as e:
            print(f"❌ 生成火焰图时出错: {e}")
            return False

    def stop_database_server(self):
        """停止数据库服务器"""
        if self.db_server_process and self.db_server_process.poll() is None:
            print("🛑 停止数据库服务器...")
            self.db_server_process.terminate()
            try:
                self.db_server_process.wait(timeout=10)
                print("✅ 数据库服务器已停止")
            except subprocess.TimeoutExpired:
                print("⚠️ 数据库服务器超时，强制终止")
                self.db_server_process.kill()

    def cleanup(self):
        """清理资源"""
        print("🧹 清理资源...")
        self.stop_perf_monitoring()
        self.stop_database_server()

    def check_server_connection(self) -> bool:
        """检查服务器连接"""
        try:
            # 初始化客户端连接
            if self.client is None:
                self.client = SQLClient(self.host, self.port, run_id=self.run_id)

            success, response = self.client.send_sql("SELECT 1;")
            if success:
                print("✅ 服务器连接正常")
                return True
            else:
                print(f"❌ 服务器连接失败: {response}")
                return False
        except Exception as e:
            print(f"❌ 服务器连接异常: {e}")
            return False

    def create_tables(self) -> bool:
        """创建TPC-C表结构"""
        print("📝 创建表结构...")

        table_sqls = [
            """create table warehouse (w_id int, w_name char(10), w_street_1 char(20),w_street_2 char(20), w_city char(20), w_state char(2),
 w_zip char(9), w_tax float, w_ytd float);""",
            """create table district (d_id int, d_w_id int, d_name char(10), d_street_1 char(20), d_street_2 char(20),
 d_city char(20), d_state char(2), d_zip char(9),d_tax float, d_ytd float, d_next_o_id int);""",
            """create table customer (c_id int, c_d_id int, c_w_id int, c_first char(16),c_middle char(2),
 c_last char(16), c_street_1 char(20), c_street_2 char(20),c_city char(20), c_state char(2),
 c_zip char(9), c_phone char(16), c_since char(30), c_credit char(2),
 c_credit_lim int, c_discount float, c_balance float,c_ytd_payment float,
 c_payment_cnt int, c_delivery_cnt int, c_data char(50));""",
            """create table history (h_c_id int, h_c_d_id int, h_c_w_id int, h_d_id int,
 h_w_id int, h_date char(19), h_amount float, h_data char(24));""",
            """create table new_orders (no_o_id int, no_d_id int, no_w_id int);""",
            """create table orders (o_id int, o_d_id int, o_w_id int, o_c_id int, o_entry_d char(19),
 o_carrier_id int, o_ol_cnt int, o_all_local int);""",
            """create table order_line ( ol_o_id int, ol_d_id int, ol_w_id int, ol_number int,
ol_i_id int, ol_supply_w_id int, ol_delivery_d char(30), ol_quantity int, ol_amount float, ol_dist_info char(24));""",
            """create table item (i_id int, i_im_id int, i_name char(24), i_price float, i_data char(50));""",
            """create table stock (s_i_id int, s_w_id int, s_quantity int, s_dist_01 char(24),s_dist_02 char(24),
 s_dist_03 char(24), s_dist_04 char(24), s_dist_05 char(24),s_dist_06 char(24), s_dist_07 char(24),
 s_dist_08 char(24), s_dist_09 char(24),s_dist_10 char(24), s_ytd float, s_order_cnt int, s_remote_cnt int, s_data char(50));"""
        ]

        for sql in table_sqls:
            success, response = self.client.send_sql(sql)
            if not success:
                print(f"❌ 创建表失败: {response}")
                return False

        print("✅ 表结构创建完成")
        return True

    def execute_sql_file(self, file_path: str, description: str = "") -> Tuple[bool, float]:
        """执行SQL文件"""
        if description:
            print(f"📝 {description}...")

        start_time = time.time()
        success = self.client.execute_sql_file(file_path)
        end_time = time.time()

        execution_time = end_time - start_time

        if success:
            print(f"✅ {description or '文件执行'}完成，耗时: {execution_time:.2f}秒")
        else:
            print(f"❌ {description or '文件执行'}失败，耗时: {execution_time:.2f}秒")

        return success, execution_time

    def execute_load_phase(self, files: Dict[str, List[str]]) -> bool:
        """执行加载阶段"""
        print("\n" + "="*60)
        print("🚀 开始加载阶段")
        print("="*60)

        load_start_time = time.time()

        # 1. 执行设置
        for setup_file in files['setup']:
            success, _ = self.execute_sql_file(setup_file, "执行初始设置")
            if not success:
                return False

        # 2. 创建索引
        for index_file in files['indexes']:
            success, _ = self.execute_sql_file(index_file, "创建索引")
            if not success:
                return False

        # 3. 加载数据
        for load_file in files['load']:
            success, _ = self.execute_sql_file(load_file, "加载数据")
            if not success:
                return False

        load_end_time = time.time()
        self.results['load_time'] = load_end_time - load_start_time

        print(f"\n✅ 加载阶段完成，总耗时: {self.results['load_time']:.2f}秒")
        return True

    def execute_transaction_file(self, file_path: str, file_index: int) -> Tuple[int, int, float]:
        """执行单个事务文件"""
        start_time = time.time()

        success_count = 0
        total_count = 0

        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()

            # 解析事务
            transactions = []
            current_transaction = []
            in_transaction = False

            for line in content.split('\n'):
                line = line.strip()
                if not line or line.startswith('--'):
                    continue

                if line.upper() == 'BEGIN;':
                    in_transaction = True
                    current_transaction = [line]
                elif line.upper() == 'COMMIT;' and in_transaction:
                    current_transaction.append(line)
                    transactions.append(current_transaction)
                    current_transaction = []
                    in_transaction = False
                elif in_transaction:
                    current_transaction.append(line)

            # 逐条执行事务中的每个SQL语句
            for transaction_sqls in transactions:
                total_count += 1
                transaction_success = True

                # 逐条发送事务中的每个SQL语句
                for sql in transaction_sqls:
                    success, response = self.client.send_sql(sql, timeout=30)
                    if not success:
                        transaction_success = False
                        print(f"❌ SQL执行失败: {sql[:50]}... - {response}")
                        # 如果连接断开，尝试重连
                        if "timed out" in response.lower() or "connection" in response.lower():
                            print(f"🔄 尝试重新连接...")
                            if self.client.connect():
                                # 重试一次
                                success, response = self.client.send_sql(sql, timeout=120)
                                if success:
                                    transaction_success = True
                                    continue
                        break

                if transaction_success:
                    success_count += 1

        except Exception as e:
            print(f"❌ 执行事务文件 {file_index} 时出错: {e}")

        end_time = time.time()
        execution_time = end_time - start_time

        print(f"📊 事务文件 {file_index} 完成: {success_count}/{total_count} 成功，耗时: {execution_time:.2f}秒")

        return success_count, total_count, execution_time

    def execute_transaction_phase_sequential(self, files: Dict[str, List[str]]) -> bool:
        """执行事务阶段（顺序执行）"""
        print("\n" + "="*60)
        print("🚀 开始事务执行阶段（顺序执行）")
        print("="*60)

        transaction_files = files['transactions']
        total_files = len(transaction_files)

        print(f"📝 准备顺序执行 {total_files} 个事务文件")

        run_start_time = time.time()

        total_success = 0
        total_transactions = 0

        # 顺序执行每个事务文件
        for i, file_path in enumerate(transaction_files):
            print(f"\n📄 执行事务文件 {i+1}/{total_files}: {os.path.basename(file_path)}")
            
            try:
                success_count, total_count, execution_time = self.execute_transaction_file(file_path, i+1)
                total_success += success_count
                total_transactions += total_count
            except Exception as e:
                print(f"❌ 事务文件 {i+1} 执行异常: {e}")

        run_end_time = time.time()
        self.results['run_time'] = run_end_time - run_start_time
        self.results['total_transactions'] = total_transactions
        self.results['successful_transactions'] = total_success
        self.results['failed_transactions'] = total_transactions - total_success

        # 计算tpmC (每分钟事务数)
        if self.results['run_time'] > 0:
            self.results['tpmC'] = (total_success / self.results['run_time']) * 60

        print(f"\n✅ 事务执行阶段完成")
        print(f"  总事务数: {total_transactions}")
        print(f"  成功事务数: {total_success}")
        print(f"  失败事务数: {total_transactions - total_success}")
        print(f"  执行时间: {self.results['run_time']:.2f}秒")
        print(f"  吞吐量(tpmC): {self.results['tpmC']:.2f}")

        return total_success > 0

    def generate_test_report(self):
        """生成测试报告"""
        report_file = os.path.join(self.test_dir, "sequential_performance_report.txt")

        with open(report_file, 'w', encoding='utf-8') as f:
            f.write("TPC-C顺序性能测试报告\n")
            f.write("="*50 + "\n")
            f.write(f"测试时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"仓库数量: {self.num_warehouses}\n")
            f.write(f"服务器: {self.host}:{self.port}\n")
            f.write(f"数据库名称: {self.dbname}\n")
            f.write(f"执行模式: 顺序执行（无并发）\n")
            f.write("\n")

            f.write("测试结果:\n")
            f.write("-"*30 + "\n")
            f.write(f"加载时间 (load_time): {self.results['load_time']:.2f} 秒\n")
            f.write(f"运行时间 (run_time): {self.results['run_time']:.2f} 秒\n")
            f.write(f"总运行时间: {self.results['load_time'] + self.results['run_time']:.2f} 秒\n")
            f.write(f"总事务数: {self.results['total_transactions']}\n")
            f.write(f"成功事务数: {self.results['successful_transactions']}\n")
            f.write(f"失败事务数: {self.results['failed_transactions']}\n")
            if self.results['total_transactions'] > 0:
                f.write(f"成功率: {(self.results['successful_transactions']/self.results['total_transactions']*100):.1f}%\n")
            f.write(f"吞吐量 (tpmC): {self.results['tpmC']:.2f}\n")
            f.write(f"\nperf数据文件: {self.perf_dir}/perf_data_{self.run_id}.data\n")
            f.write(f"火焰图文件: {self.perf_dir}/flame_graph_{self.run_id}.svg\n")

        print(f"\n📊 测试报告已生成: {os.path.abspath(report_file)}")

    def run_performance_test(self, num_transaction_files: int = 10, transactions_per_file: int = 100) -> bool:
        """运行完整的顺序性能测试"""
        print("\n" + "="*80)
        print("🚀 TPC-C顺序性能测试开始")
        print("="*80)

        try:
            # 1. 清理数据库目录
            self.cleanup_database()

            # 2. 启动数据库服务器
            if not self.start_database_server():
                return False

            # 3. 检查服务器连接
            if not self.check_server_connection():
                return False

            # 4. 启动perf监控
            if not self.start_perf_monitoring():
                print("⚠️ perf监控启动失败，继续测试但不会生成火焰图")

            # 5. 创建表结构
            if not self.create_tables():
                return False

            # 6. 生成测试文件
            print("\n📝 生成测试文件...")
            files = self.generator.generate_all_files(num_transaction_files, transactions_per_file)

            # 7. 执行加载阶段
            if not self.execute_load_phase(files):
                return False

            # 8. 执行事务阶段（顺序执行）
            if not self.execute_transaction_phase_sequential(files):
                return False

            # 9. 停止perf监控
            self.stop_perf_monitoring()

            # 10. 生成火焰图
            if self.perf_process is not None:
                self.generate_flame_graph()

            # 11. 生成报告
            self.generate_test_report()

            # 12. 打印最终结果
            print("\n" + "="*80)
            print("🎉 TPC-C顺序性能测试完成")
            print("="*80)
            print(f"📊 最终结果:")
            print(f"  加载时间: {self.results['load_time']:.2f} 秒")
            print(f"  运行时间: {self.results['run_time']:.2f} 秒")
            print(f"  总时间: {self.results['load_time'] + self.results['run_time']:.2f} 秒")
            print(f"  吞吐量: {self.results['tpmC']:.2f} tpmC")
            print(f"  perf数据: {self.perf_dir}/perf_data_{self.run_id}.data")
            print(f"  火焰图: {self.perf_dir}/flame_graph_{self.run_id}.svg")

            return True

        finally:
            # 确保清理资源
            self.cleanup()

def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='TPC-C顺序性能测试')
    parser.add_argument('--host', default='localhost', help='数据库服务器地址')
    parser.add_argument('--port', type=int, default=8765, help='数据库服务器端口')
    parser.add_argument('--warehouses', '-w', type=int, default=10, help='仓库数量')
    parser.add_argument('--output-dir', '-o', default='sqls', help='输出目录')
    parser.add_argument('--transaction-files', '-f', type=int, default=10, help='事务文件数量')
    parser.add_argument('--transactions-per-file', '-t', type=int, default=100, help='每文件事务数')
    parser.add_argument('--dbname', '-d', default='testdb', help='数据库名称')
    parser.add_argument('--generate-only', action='store_true', help='只生成文件，不执行测试')

    args = parser.parse_args()

    print("TPC-C顺序性能测试")
    print("=" * 50)

    # 创建测试实例
    test = TPCCSequentialPerformanceTest(
        host=args.host,
        port=args.port,
        num_warehouses=args.warehouses,
        output_dir=args.output_dir,
        dbname=args.dbname
    )

    try:
        if args.generate_only:
            # 只生成文件
            test.generator.generate_all_files(
                num_transaction_files=args.transaction_files,
                transactions_per_file=args.transactions_per_file
            )
            print("✅ 文件生成完成!")
        else:
            # 执行完整测试
            test.run_performance_test(
                num_transaction_files=args.transaction_files,
                transactions_per_file=args.transactions_per_file
            )

        return 0
    except KeyboardInterrupt:
        print("\n❌ 用户中断测试")
        test.cleanup()
        return 1
    except Exception as e:
        print(f"\n❌ 测试过程中出错: {e}")
        test.cleanup()
        return 1

if __name__ == "__main__":
    sys.exit(main())