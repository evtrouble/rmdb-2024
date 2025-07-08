#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TPC-C万级CSV数据生成器
生成与查询语句紧密关联的测试数据，确保性能测试的有效性
"""

import csv
import random
import os
from datetime import datetime, timedelta
from typing import List, Dict
import time

class TPCCCSVGenerator:
    def __init__(self, num_warehouses: int = 10, output_dir: str = "csv_data"):  # 从100改为10
        """
        初始化TPC-C CSV数据生成器
        
        Args:
            num_warehouses: 仓库数量 (建议10以上，产生千级数据)
            output_dir: CSV文件输出目录
        """
        self.num_warehouses = num_warehouses
        self.output_dir = output_dir
        
        # TPC-C标准配置 - 调整为千级数据
        self.num_districts_per_warehouse = 10
        self.num_customers_per_district = 300  # 从3000改为300
        self.num_items = 10000  # 从100000改为10000
        self.num_orders_per_district = 300  # 从3000改为300
        self.num_new_orders_per_district = 90  # 从900改为90
        self.num_order_lines_per_order = random.randint(5, 15)  # 保持不变
        
        # 计算总数据量
        self.total_customers = self.num_warehouses * self.num_districts_per_warehouse * self.num_customers_per_district
        self.total_orders = self.num_warehouses * self.num_districts_per_warehouse * self.num_orders_per_district
        
        print(f"📊 数据规模预估:")
        print(f"  仓库数: {self.num_warehouses:,}")
        print(f"  客户数: {self.total_customers:,}")
        print(f"  订单数: {self.total_orders:,}")
        print(f"  商品数: {self.num_items:,}")
        
        # 常用姓氏列表 - 确保查询能找到数据
        self.last_names = [
            'SMITH', 'JOHNSON', 'WILLIAMS', 'BROWN', 'JONES', 'GARCIA', 'MILLER', 'DAVIS',
            'RODRIGUEZ', 'MARTINEZ', 'HERNANDEZ', 'LOPEZ', 'GONZALEZ', 'WILSON', 'ANDERSON',
            'THOMAS', 'TAYLOR', 'MOORE', 'JACKSON', 'MARTIN', 'LEE', 'PEREZ', 'THOMPSON',
            'WHITE', 'HARRIS', 'SANCHEZ', 'CLARK', 'RAMIREZ', 'LEWIS', 'ROBINSON'
        ]
        
        # 创建输出目录
        os.makedirs(self.output_dir, exist_ok=True)
    
    def generate_random_string(self, length: int, numeric: bool = False) -> str:
        """生成随机字符串"""
        if numeric:
            chars = '0123456789'
        else:
            chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        return ''.join(random.choice(chars) for _ in range(length))
    
    def generate_random_phone(self) -> str:
        """生成随机电话号码"""
        return f"{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
    
    def generate_zip_code(self) -> str:
        """生成邮编"""
        return f"{random.randint(10000, 99999)}"
    
    def generate_timestamp(self) -> str:
        """生成时间戳"""
        base_date = datetime(2024, 1, 1)
        random_days = random.randint(0, 365)
        random_seconds = random.randint(0, 86400)
        timestamp = base_date + timedelta(days=random_days, seconds=random_seconds)
        return timestamp.strftime('%Y-%m-%d %H:%M:%S')
    
    def write_csv_file(self, filename: str, headers: List[str], data_generator, batch_size: int = 10000):
        """批量写入CSV文件"""
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(headers)  # 写入表头
            
            batch = []
            count = 0
            
            for row in data_generator:
                batch.append(row)
                count += 1
                
                if len(batch) >= batch_size:
                    writer.writerows(batch)
                    batch = []
                    if count % 50000 == 0:
                        print(f"    已写入 {count:,} 行...")
            
            # 写入剩余数据
            if batch:
                writer.writerows(batch)
            
            print(f"  ✅ {filename} 完成，共 {count:,} 行")
    
    def generate_warehouse_data(self):
        """生成warehouse表数据"""
        print("🏢 生成warehouse数据...")
        
        def warehouse_generator():
            for w_id in range(1, self.num_warehouses + 1):
                yield [
                    w_id,
                    f"Warehouse{w_id:04d}",
                    f"{random.randint(100, 999)} Main St",
                    f"Suite {random.randint(1, 999)}",
                    f"City{random.randint(1, 100)}",
                    random.choice(['CA', 'NY', 'TX', 'FL', 'IL']),
                    self.generate_zip_code(),
                    round(random.uniform(0.05, 0.15), 4),  # w_tax
                    round(random.uniform(100000, 500000), 2)  # w_ytd
                ]
        
        headers = ['w_id', 'w_name', 'w_street_1', 'w_street_2', 'w_city', 'w_state', 'w_zip', 'w_tax', 'w_ytd']
        self.write_csv_file('warehouse.csv', headers, warehouse_generator())
    
    def generate_district_data(self):
        """生成district表数据"""
        print("🏘️ 生成district数据...")
        
        def district_generator():
            for w_id in range(1, self.num_warehouses + 1):
                for d_id in range(1, self.num_districts_per_warehouse + 1):
                    yield [
                        d_id,
                        w_id,
                        f"District{d_id:02d}",
                        f"{random.randint(100, 999)} District Ave",
                        f"Floor {random.randint(1, 10)}",
                        f"City{random.randint(1, 100)}",
                        random.choice(['CA', 'NY', 'TX', 'FL', 'IL']),
                        self.generate_zip_code(),
                        round(random.uniform(0.05, 0.15), 4),  # d_tax
                        round(random.uniform(10000, 50000), 2),  # d_ytd
                        self.num_orders_per_district + 1  # d_next_o_id
                    ]
        
        headers = ['d_id', 'd_w_id', 'd_name', 'd_street_1', 'd_street_2', 'd_city', 'd_state', 'd_zip', 'd_tax', 'd_ytd', 'd_next_o_id']
        self.write_csv_file('district.csv', headers, district_generator())
    
    def generate_customer_data(self):
        """生成customer表数据"""
        print("👥 生成customer数据...")
        
        def customer_generator():
            for w_id in range(1, self.num_warehouses + 1):
                for d_id in range(1, self.num_districts_per_warehouse + 1):
                    for c_id in range(1, self.num_customers_per_district + 1):
                        # 确保姓氏分布合理，便于按姓氏查询
                        last_name = random.choice(self.last_names)
                        
                        yield [
                            c_id,
                            d_id,
                            w_id,
                            f"First{c_id:04d}",
                            "OE",  # c_middle
                            last_name,
                            f"{random.randint(100, 999)} Customer St",
                            f"Apt {random.randint(1, 999)}",
                            f"City{random.randint(1, 100)}",
                            random.choice(['CA', 'NY', 'TX', 'FL', 'IL']),
                            self.generate_zip_code(),
                            self.generate_random_phone(),
                            self.generate_timestamp(),
                            random.choice(['GC', 'BC']),  # c_credit
                            50000,  # c_credit_lim
                            round(random.uniform(0.0, 0.5), 4),  # c_discount
                            round(random.uniform(-1000, 5000), 2),  # c_balance
                            round(random.uniform(0, 50000), 2),  # c_ytd_payment
                            random.randint(0, 100),  # c_payment_cnt
                            random.randint(0, 50),  # c_delivery_cnt
                            f"Customer data for {c_id}"
                        ]
        
        headers = ['c_id', 'c_d_id', 'c_w_id', 'c_first', 'c_middle', 'c_last', 'c_street_1', 'c_street_2', 
                  'c_city', 'c_state', 'c_zip', 'c_phone', 'c_since', 'c_credit', 'c_credit_lim', 
                  'c_discount', 'c_balance', 'c_ytd_payment', 'c_payment_cnt', 'c_delivery_cnt', 'c_data']
        self.write_csv_file('customer.csv', headers, customer_generator())
    
    def generate_item_data(self):
        """生成item表数据"""
        print("📦 生成item数据...")
        
        def item_generator():
            for i_id in range(1, self.num_items + 1):
                yield [
                    i_id,
                    random.randint(1, 10000),  # i_im_id
                    f"Item{i_id:06d}",
                    round(random.uniform(1.0, 100.0), 2),  # i_price
                    f"Item data for {i_id}"
                ]
        
        headers = ['i_id', 'i_im_id', 'i_name', 'i_price', 'i_data']
        self.write_csv_file('item.csv', headers, item_generator())
    
    def generate_stock_data(self):
        """生成stock表数据"""
        print("📊 生成stock数据...")
        
        def stock_generator():
            for w_id in range(1, self.num_warehouses + 1):
                for i_id in range(1, self.num_items + 1):
                    yield [
                        i_id,
                        w_id,
                        random.randint(10, 100),  # s_quantity
                        f"Dist01-{i_id:06d}",  # s_dist_01
                        f"Dist02-{i_id:06d}",  # s_dist_02
                        f"Dist03-{i_id:06d}",  # s_dist_03
                        f"Dist04-{i_id:06d}",  # s_dist_04
                        f"Dist05-{i_id:06d}",  # s_dist_05
                        f"Dist06-{i_id:06d}",  # s_dist_06
                        f"Dist07-{i_id:06d}",  # s_dist_07
                        f"Dist08-{i_id:06d}",  # s_dist_08
                        f"Dist09-{i_id:06d}",  # s_dist_09
                        f"Dist10-{i_id:06d}",  # s_dist_10
                        round(random.uniform(0, 10000), 2),  # s_ytd
                        random.randint(0, 100),  # s_order_cnt
                        random.randint(0, 10),  # s_remote_cnt
                        f"Stock data for item {i_id}"
                    ]
        
        headers = ['s_i_id', 's_w_id', 's_quantity', 's_dist_01', 's_dist_02', 's_dist_03', 's_dist_04', 
                  's_dist_05', 's_dist_06', 's_dist_07', 's_dist_08', 's_dist_09', 's_dist_10', 
                  's_ytd', 's_order_cnt', 's_remote_cnt', 's_data']
        self.write_csv_file('stock.csv', headers, stock_generator())
    
    def generate_orders_data(self):
        """生成orders表数据"""
        print("📋 生成orders数据...")
        
        def orders_generator():
            for w_id in range(1, self.num_warehouses + 1):
                for d_id in range(1, self.num_districts_per_warehouse + 1):
                    for o_id in range(1, self.num_orders_per_district + 1):
                        # 确保客户ID在有效范围内
                        c_id = random.randint(1, self.num_customers_per_district)
                        
                        yield [
                            o_id,
                            d_id,
                            w_id,
                            c_id,
                            self.generate_timestamp(),
                            random.randint(1, 10) if o_id <= (self.num_orders_per_district - self.num_new_orders_per_district) else None,  # o_carrier_id
                            random.randint(5, 15),  # o_ol_cnt
                            1  # o_all_local
                        ]
        
        headers = ['o_id', 'o_d_id', 'o_w_id', 'o_c_id', 'o_entry_d', 'o_carrier_id', 'o_ol_cnt', 'o_all_local']
        self.write_csv_file('orders.csv', headers, orders_generator())
    
    def generate_new_orders_data(self):
        """生成new_orders表数据"""
        print("🆕 生成new_orders数据...")
        
        def new_orders_generator():
            for w_id in range(1, self.num_warehouses + 1):
                for d_id in range(1, self.num_districts_per_warehouse + 1):
                    # 最后900个订单作为新订单
                    start_o_id = self.num_orders_per_district - self.num_new_orders_per_district + 1
                    for o_id in range(start_o_id, self.num_orders_per_district + 1):
                        yield [o_id, d_id, w_id]
        
        headers = ['no_o_id', 'no_d_id', 'no_w_id']
        self.write_csv_file('new_orders.csv', headers, new_orders_generator())
    
    def generate_order_line_data(self):
        """生成order_line表数据"""
        print("📝 生成order_line数据...")
        
        def order_line_generator():
            for w_id in range(1, self.num_warehouses + 1):
                for d_id in range(1, self.num_districts_per_warehouse + 1):
                    for o_id in range(1, self.num_orders_per_district + 1):
                        ol_cnt = random.randint(5, 15)
                        for ol_number in range(1, ol_cnt + 1):
                            # 确保商品ID在有效范围内
                            i_id = random.randint(1, self.num_items)
                            
                            yield [
                                o_id,
                                d_id,
                                w_id,
                                ol_number,
                                i_id,
                                w_id,  # ol_supply_w_id
                                self.generate_timestamp() if o_id <= (self.num_orders_per_district - self.num_new_orders_per_district) else None,  # ol_delivery_d
                                random.randint(1, 10),  # ol_quantity
                                round(random.uniform(1.0, 100.0), 2),  # ol_amount
                                f"Dist info for {ol_number:02d}"
                            ]
        
        headers = ['ol_o_id', 'ol_d_id', 'ol_w_id', 'ol_number', 'ol_i_id', 'ol_supply_w_id', 
                  'ol_delivery_d', 'ol_quantity', 'ol_amount', 'ol_dist_info']
        self.write_csv_file('order_line.csv', headers, order_line_generator())
    
    def generate_history_data(self):
        """生成history表数据"""
        print("📚 生成history数据...")
        
        def history_generator():
            # 生成历史记录，数量约为客户数的2-5倍
            num_history_records = self.total_customers * random.randint(2, 5)
            
            for h_id in range(1, num_history_records + 1):
                w_id = random.randint(1, self.num_warehouses)
                d_id = random.randint(1, self.num_districts_per_warehouse)
                c_id = random.randint(1, self.num_customers_per_district)
                
                yield [
                    c_id,
                    d_id,
                    w_id,
                    d_id,
                    w_id,
                    self.generate_timestamp(),
                    round(random.uniform(1.0, 5000.0), 2),  # h_amount
                    f"Payment history {h_id}"
                ]
        
        headers = ['h_c_id', 'h_c_d_id', 'h_c_w_id', 'h_d_id', 'h_w_id', 'h_date', 'h_amount', 'h_data']
        self.write_csv_file('history.csv', headers, history_generator())
    
    def generate_all_csv_data(self):
        """生成所有表的CSV数据"""
        print("🚀 开始生成TPC-C万级CSV测试数据...")
        print("=" * 60)
        
        start_time = time.time()
        
        # 按依赖关系顺序生成数据
        self.generate_warehouse_data()
        self.generate_district_data()
        self.generate_customer_data()
        self.generate_item_data()
        self.generate_stock_data()
        self.generate_orders_data()
        self.generate_new_orders_data()
        self.generate_order_line_data()
        self.generate_history_data()
        
        end_time = time.time()
        
        print("\n" + "=" * 60)
        print(f"✅ 所有CSV数据生成完成！")
        print(f"📁 输出目录: {self.output_dir}")
        print(f"⏱️ 总耗时: {end_time - start_time:.2f}秒")
        print("\n📊 生成的文件:")
        
        # 显示生成的文件信息
        for filename in os.listdir(self.output_dir):
            if filename.endswith('.csv'):
                filepath = os.path.join(self.output_dir, filename)
                file_size = os.path.getsize(filepath) / (1024 * 1024)  # MB
                print(f"  📄 {filename}: {file_size:.1f} MB")
        
        print("\n💡 使用说明:")
        print("  1. 这些CSV文件包含万级数据，与TPC-C查询语句紧密关联")
        print("  2. 数据ID范围与查询范围完全匹配，确保查询能找到对应数据")
        print("  3. 可以使用LOAD DATA INFILE或INSERT语句将数据导入数据库")
        print("  4. 建议先创建索引再导入数据以提高性能")

def main():
    """主函数"""
    # 调整仓库数量来控制数据规模
    # 10个仓库 ≈ 3万客户 + 3万订单 + 100万订单行
    generator = TPCCCSVGenerator(num_warehouses=10, output_dir="csv_data")  # 从100改为10
    generator.generate_all_csv_data()

if __name__ == "__main__":
    main()