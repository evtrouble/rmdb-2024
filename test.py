#!/usr/bin/env python3
import subprocess
import time
import re
import os
from typing import List, Dict, Tuple, Optional, Union

class DatabaseTester:
    def __init__(self, client_path: str, test_output_file: str = "test_results.txt", timeout: int = 300):
        """初始化测试器"""
        self.client_path = client_path
        self.test_output_file = test_output_file
        self.timeout = timeout
        self.results = []
        self.debug_mode = True  # 启用调试模式
    
    def run_test_case(self, test_name: str, sql_commands: List[str]) -> Tuple[bool, str]:
        """执行单个测试用例并返回结果"""
        print(f"\n=== 执行测试: {test_name} ===")
        
        # 构建完整的命令
        cmd = [self.client_path]
        
        try:
            # 启动客户端进程
            process = subprocess.Popen(
                cmd,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # 发送SQL命令并捕获输出
            output = ""
            for i, sql in enumerate(sql_commands):
                if self.debug_mode:
                    print(f"发送命令 {i+1}/{len(sql_commands)}: {sql[:80]}" + ("..." if len(sql) > 80 else ""))
                
                try:
                    # 检查进程是否已终止
                    if process.poll() is not None:
                        error_msg = f"错误: 进程在执行命令 {i+1} 前已退出，返回码: {process.returncode}"
                        if self.debug_mode:
                            print(error_msg)
                            print(f"进程输出: {process.stdout.read()}")
                            print(f"进程错误: {process.stderr.read()}")
                        raise Exception(error_msg)
                    
                    # 确保SQL命令以分号和换行符结尾
                    if not sql.endswith(';'):
                        sql += ';'
                    sql += '\n'  # 确保有换行符
                    
                    # 发送SQL命令
                    process.stdin.write(sql)
                    process.stdin.flush()
                    
                    # 读取命令执行结果，设置超时防止卡死
                    cmd_output = ""
                    start_time = time.time()
                    
                    while True:
                        # 检查是否超时
                        if time.time() - start_time > self.timeout:
                            raise TimeoutError(f"命令执行超时 ({self.timeout}秒)")
                        
                        # 尝试读取输出
                        line = process.stdout.readline()
                        if not line:
                            # 如果没有输出且进程已退出，退出循环
                            if process.poll() is not None:
                                break
                            # 否则等待一小段时间继续尝试
                            time.sleep(0.1)
                            continue
                        
                        cmd_output += line
                        output += line
                        
                        # 打印非空输出
                        if self.debug_mode and len(line.strip()) > 0:
                            print(f"输出: {line.strip()}")
                        
                        # 检测命令提示符，判断命令是否执行完毕
                        if "Rucbase>" in line:  # 根据实际提示符调整
                            break
                    
                    # 检查命令执行是否成功
                    if "ERROR" in cmd_output.upper() or "FAILED" in cmd_output.upper():
                        print(f"警告: 命令执行失败 - {sql.strip()}")
                    
                except TimeoutError as e:
                    print(f"错误: 命令 {i+1} 执行超时: {str(e)}")
                    raise
                except Exception as e:
                    # 尝试获取进程状态
                    return_code = process.poll()
                    if return_code is not None:
                        error_output = process.stderr.read()
                        error_detail = f"进程已退出，返回码: {return_code}, 错误输出: {error_output}"
                    else:
                        error_detail = "进程仍在运行"
                    
                    error_msg = f"发送命令 {i+1} 时发生异常: {str(e)}, 详情: {error_detail}"
                    print(error_msg)
                    
                    # 尝试清理进程
                    try:
                        process.terminate()
                        time.sleep(1)
                        if process.poll() is None:
                            process.kill()
                    except:
                        pass
                    
                    raise Exception(error_msg)
            
            # 关闭输入并等待进程完成
            process.stdin.close()
            stdout, stderr = process.communicate(timeout=self.timeout)
            output += stdout
            
            # 检查返回码
            return_code = process.returncode
            
            # 判断测试是否成功
            success = return_code == 0 and "ERROR" not in output.upper()
            
            # 记录结果
            result = {
                "test_name": test_name,
                "success": success,
                "return_code": return_code,
                "output": output,
                "error": stderr
            }
            self.results.append(result)
            
            # 打印简要结果
            status = "通过" if success else "失败"
            print(f"测试结果: {status} (返回码: {return_code})")
            
            return success, output
            
        except subprocess.TimeoutExpired:
            print(f"错误: 测试 {test_name} 整体超时")
            try:
                process.kill()
            except:
                pass
            return False, "测试整体超时"
        except TimeoutError as e:
            print(f"错误: 测试 {test_name} 因命令超时失败: {str(e)}")
            try:
                process.kill()
            except:
                pass
            return False, str(e)
        except Exception as e:
            print(f"错误: 执行测试 {test_name} 时发生异常: {str(e)}")
            return False, str(e)
    
    def generate_report(self) -> None:
        """生成测试报告"""
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results if r["success"])
        failed_tests = total_tests - passed_tests
        
        print(f"\n=== 测试总结 ===")
        print(f"总测试数: {total_tests}")
        print(f"通过: {passed_tests}")
        print(f"失败: {failed_tests}")
        
        # 保存详细结果到文件
        with open(self.test_output_file, "w") as f:
            f.write("=== 测试报告 ===\n\n")
            f.write(f"总测试数: {total_tests}\n")
            f.write(f"通过: {passed_tests}\n")
            f.write(f"失败: {failed_tests}\n\n")
            
            for result in self.results:
                f.write(f"=== 测试: {result['test_name']} ===\n")
                f.write(f"状态: {'通过' if result['success'] else '失败'}\n")
                f.write(f"返回码: {result['return_code']}\n")
                f.write(f"输出:\n{result['output']}\n")
                if result["error"]:
                    f.write(f"错误:\n{result['error']}\n")
                f.write("\n")
        
        print(f"详细测试结果已保存到: {self.test_output_file}")
    
    def run_test_from_file(self, test_name: str, file_path: str) -> bool:
        """从文件读取SQL命令并执行测试"""
        if not os.path.exists(file_path):
            print(f"错误: 测试文件不存在 - {file_path}")
            return False
        
        try:
            with open(file_path, 'r') as f:
                sql_content = f.read()
            
            # 过滤掉SQL注释
            sql_lines = []
            for line in sql_content.split('\n'):
                line = line.strip()
                if not line or line.startswith('--'):
                    continue
                sql_lines.append(line)
            
            # 重新组合并按分号分割
            sql_content = '\n'.join(sql_lines)
            
            # 按分号分割SQL命令，但保留分号在每个命令末尾
            # 使用正则表达式确保分号不在引号内
            sql_commands = []
            current_cmd = ""
            in_quote = False
            quote_char = None
            
            for char in sql_content:
                if char in ["'", '"']:
                    if not in_quote:
                        in_quote = True
                        quote_char = char
                    elif in_quote and char == quote_char:
                        in_quote = False
                        quote_char = None
                
                current_cmd += char
                
                if char == ';' and not in_quote:
                    sql_commands.append(current_cmd.strip())
                    current_cmd = ""
            
            # 添加最后一个命令（如果有）
            if current_cmd.strip():
                sql_commands.append(current_cmd.strip())
            
            if not sql_commands:
                print(f"错误: 文件 {file_path} 不包含有效SQL命令")
                return False
            
            # 执行测试
            success, output = self.run_test_case(test_name, sql_commands)
            return success
            
        except Exception as e:
            print(f"错误: 读取测试文件 {file_path} 时发生异常: {str(e)}")
            return False
    
    def run_all_tests(self, test_files: Dict[str, str]) -> None:
        """执行所有测试用例"""
        for test_name, file_path in test_files.items():
            self.run_test_from_file(test_name, file_path)
        
        # 生成测试报告
        self.generate_report()

if __name__ == "__main__":
    # 客户端程序路径
    client_path = "rmdb_client/build/rmdb_client"
    
    # 测试文件配置 - 格式: {测试名称: 测试文件路径}
    test_files = {
        "测试": "warehouse_test.sql"
    }
    
    # 检查客户端程序是否存在
    if not os.path.exists(client_path):
        print(f"错误: 客户端程序不存在 - {client_path}")
        print("请确保路径正确，或者修改脚本中的client_path变量")
    else:
        # 创建测试器实例并运行测试
        tester = DatabaseTester(client_path)
        tester.run_all_tests(test_files)