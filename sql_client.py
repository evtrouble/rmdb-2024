#!/usr/bin/env python3
"""
SQL客户端脚本
用于向RMDB服务器发送SQL命令并记录响应
"""

import os
import socket
import time
from datetime import datetime
from typing import Tuple, List

class SQLClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 8765, run_id: str = None):
        self.server_host = host
        self.server_port = port
        self.buffer_size = 8192
        
        # 添加缺失的属性初始化
        self.sock = None
        self.connected = False
        
        # 如果没有提供run_id，创建一个基于当前时间的run_id
        if run_id is None:
            run_id = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # 为每次运行创建专门的子目录
        self.log_dir = os.path.join("sql_logs", f"run_{run_id}")
        
        # 确保日志目录存在
        if not os.path.exists(self.log_dir):
            os.makedirs(self.log_dir)

        # 创建新的日志文件
        self.log_file = os.path.join(
            self.log_dir,
            f"sql_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        )
        self.init_log_file()

    def init_log_file(self):
        """初始化日志文件"""
        with open(self.log_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("SQL命令和响应日志\n")
            f.write(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")

    def log_interaction(self, sql: str, success: bool, response: str, context: str = ""):
        """记录SQL交互到日志文件"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

        with open(self.log_file, 'a', encoding='utf-8') as f:
            if context:
                f.write(f"[{timestamp}] {context}\n")
            f.write("-" * 50 + "\n")
            f.write(f"SQL命令: {sql}\n")
            f.write(f"执行状态: {'成功' if success else '失败'}\n")
            f.write(f"服务器响应: {response}\n")
            f.write("-" * 50 + "\n\n")

    def connect(self) -> bool:
        """建立持久连接"""
        try:
            if hasattr(self, 'sock') and self.sock:
                self.sock.close()
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(60)  # 增加超时时间
            self.sock.connect((self.server_host, self.server_port))
            self.connected = True
            return True
        except Exception as e:
            self.connected = False
            return False
    
    def disconnect(self):
        """关闭连接"""
        if hasattr(self, 'sock') and self.sock:
            try:
                self.sock.close()
            except:
                pass
            self.sock = None
        self.connected = False
    
    def send_sql(self, sql: str, timeout: int = 60) -> Tuple[bool, str]:
        """发送SQL命令（使用持久连接）"""
        if not self.connected:
            if not self.connect():
                return False, "连接失败"
        
        try:
            self.sock.sendall((sql + '\0').encode('utf-8'))
            data = self.sock.recv(self.buffer_size)
            response = data.decode('utf-8').strip()
            self.log_interaction(sql, True, response)
            return True, response
        except Exception as e:
            error_msg = str(e)
            self.log_interaction(sql, False, error_msg)
            self.connected = False
            return False, error_msg
    
    def __del__(self):
        self.disconnect()

    def execute_sql_file(self, sql_file: str) -> bool:
        """执行SQL文件中的所有命令"""
        print(f"\n执行SQL文件: {sql_file}")
    
        if not os.path.exists(sql_file):
            print(f"错误: SQL文件不存在 - {sql_file}")
            return False
    
        try:
            with open(sql_file, 'r') as f:
                content = f.read()
    
            # 定义特殊命令列表（不需要分号结尾）
            special_commands = {'set output_file on', 'set output_file off', 'restart', 'crash'}
            
            # 将内容按分号分割成单独的SQL语句
            # 过滤掉空语句和注释
            queries = []
            current_query = []
    
            for line in content.split('\n'):
                line = line.strip()
                if not line or line.startswith('--'):
                    continue
    
                # 检查是否是特殊命令
                if line.lower() in special_commands:
                    # 如果有未完成的查询，先处理它
                    if current_query:
                        query = ' '.join(current_query)
                        if query.strip():
                            queries.append(query)
                        current_query = []
                    # 添加特殊命令
                    queries.append(line)
                    continue
    
                current_query.append(line)
                if line.endswith(';'):
                    query = ' '.join(current_query)
                    if query.strip(';').strip():  # 确保去掉分号后还有内容
                        queries.append(query)
                    current_query = []
    
            # 检查是否有未完成的查询
            if current_query:
                remaining_query = ' '.join(current_query)
                if remaining_query.strip():
                    # 检查是否是特殊命令但没有被识别
                    if remaining_query.lower() in special_commands:
                        queries.append(remaining_query)
                    else:
                        print(f"警告: 发现未以分号结尾的SQL语句: {remaining_query}")
    
            total_queries = len(queries)
            print(f"准备执行 {total_queries} 个SQL命令")

            success_count = 0
            for i, query in enumerate(queries, 1):
                success, response = self.send_sql(query)
                if success:
                    success_count += 1

                if i % 50 == 0 or i == total_queries:
                    print(f"已执行 {i}/{total_queries} 个命令 (成功: {success_count})")

            success_rate = (success_count / total_queries) * 100 if total_queries > 0 else 0
            print(f"\n执行完成: 总计 {total_queries} 个命令, 成功率 {success_rate:.1f}%")
            return True

        except Exception as e:
            print(f"执行SQL文件时发生错误: {e}")
            return False

def main():
    import argparse

    parser = argparse.ArgumentParser(description='RMDB SQL客户端')
    parser.add_argument('--host', default='127.0.0.1', help='服务器主机名 (默认: 127.0.0.1)')
    parser.add_argument('--port', type=int, default=8765, help='服务器端口 (默认: 8765)')
    parser.add_argument('--file', help='要执行的SQL文件路径')
    parser.add_argument('--sql', help='要执行的SQL命令')

    args = parser.parse_args()

    client = SQLClient(args.host, args.port)

    if args.file:
        client.execute_sql_file(args.file)
    elif args.sql:
        success, response = client.send_sql(args.sql)
        if success:
            print("执行成功!")
            print("服务器响应:", response)
        else:
            print("执行失败!")
            print("错误信息:", response)
    else:
        print("请使用 --file 指定SQL文件或使用 --sql 指定SQL命令")

    # 输出日志信息
    print("\n日志信息:")
    print(f"日志文件: {os.path.abspath(client.log_file)}")

if __name__ == "__main__":
    main()