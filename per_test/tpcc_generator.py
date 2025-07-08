#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TPC-C事务语句生成器
用于生成TPC-C性能测试所需的各种事务SQL语句
包含五种标准TPC-C事务的随机生成
支持生成多个独立的事务文件
支持数据分区策略以减少UPDATE冲突
"""

import random
import datetime
import string
import os
from typing import List, Dict, Tuple

class TPCCTransactionGenerator:
    def __init__(self, num_warehouses: int = 10, output_dir: str = "sqls", output_file: str = None):
        """
        初始化TPC-C事务生成器

        Args:
            num_warehouses: 仓库数量
            output_dir: 输出目录
            output_file: 输出文件名（用于兼容性）
        """
        self.num_warehouses = num_warehouses
        self.output_dir = output_dir
        self.output_file = output_file or os.path.join(output_dir, "tpcc_transactions.sql")

        # 确保输出目录存在
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        # TPC-C标准配置
        self.num_districts_per_warehouse = 10
        self.num_customers_per_district = 3000
        self.num_items = 100000
        self.num_orders_per_district = 3000
        self.num_new_orders_per_district = 900

        # 事务权重配置（符合TPC-C标准）
        self.transaction_weights = {
            'new_order': 45,      # 45%
            'payment': 43,        # 43%
            'order_status': 4,    # 4%
            'delivery': 4,        # 4%
            'stock_level': 4      # 4%
        }

        # 常用姓氏列表（TPC-C标准）
        self.last_names = [
            'BARBARBAR', 'BARBAROUGHT', 'BARBARABLE', 'BARBARPRI', 'BARBARPRES',
            'BARBARESE', 'BARBARANTI', 'BARBARCALLY', 'BARBARATION', 'BARBAREING',
            'OUGHTBAR', 'OUGHTOUGHT', 'OUGHTABLE', 'OUGHTPRI', 'OUGHTPRES'
        ]
        
        # 分区配置
        self.partition_config = None
        self.current_partition_id = 0

        print(f"🚀 TPC-C事务生成器初始化完成")
        print(f"  仓库数量: {self.num_warehouses}")
        print(f"  输出目录: {self.output_dir}")

    def set_partition_config(self, partition_id: int, total_partitions: int):
        """
        设置当前分区配置
        
        Args:
            partition_id: 当前分区ID (0-based)
            total_partitions: 总分区数
        """
        self.current_partition_id = partition_id
        
        # 计算warehouse分区范围
        warehouses_per_partition = max(1, self.num_warehouses // total_partitions)
        warehouse_start = partition_id * warehouses_per_partition + 1
        warehouse_end = min((partition_id + 1) * warehouses_per_partition, self.num_warehouses)
        
        # 计算item分区范围
        items_per_partition = max(1000, self.num_items // total_partitions)
        item_start = partition_id * items_per_partition + 1
        item_end = min((partition_id + 1) * items_per_partition, self.num_items)
        
        self.partition_config = {
            'warehouse_range': (warehouse_start, warehouse_end),
            'item_range': (item_start, item_end),
            'partition_id': partition_id,
            'total_partitions': total_partitions
        }
        
        print(f"  分区 {partition_id+1}/{total_partitions}: 仓库 {warehouse_start}-{warehouse_end}, 商品 {item_start}-{item_end}")

    def get_partition_warehouse_id(self) -> int:
        """
        获取当前分区的warehouse_id
        """
        if self.partition_config:
            start, end = self.partition_config['warehouse_range']
            return random.randint(start, end)
        else:
            return random.randint(1, self.num_warehouses)

    def get_partition_item_id(self) -> int:
        """
        获取当前分区的item_id
        """
        if self.partition_config:
            start, end = self.partition_config['item_range']
            return random.randint(start, end)
        else:
            return random.randint(1, self.num_items)

    def generate_setup_sql(self) -> List[str]:
        """生成初始设置SQL语句"""
        return [
            "-- ========================================",
            "-- TPC-C 性能测试初始化",
            "-- ========================================",
            "",
            "-- 关闭输出文件以提升性能",
            "set output_file off",
            ""
        ]

    def generate_index_creation_sql(self) -> List[str]:
        """生成索引创建SQL语句"""
        return [
            "-- ========================================",
            "-- 索引创建",
            "-- ========================================",
            "",
            "-- warehouse表索引",
            "create index warehouse(w_id);",
            "",
            "-- item表索引",
            "create index item(i_id);",
            "create index item(i_price);",
            "",
            "-- district表索引",
            "create index district(d_w_id, d_id);",
            "create index district(d_w_id, d_id, d_next_o_id);",
            "",
            "-- customer表索引",
            "create index customer(c_w_id, c_d_id, c_id);",
            "create index customer(c_w_id, c_d_id, c_last, c_first);",
            "create index customer(c_last);",
            "",
            "-- stock表索引",
            "create index stock(s_w_id, s_i_id);",
            "create index stock(s_w_id, s_quantity);",
            "create index stock(s_i_id);",
            "",
            "-- orders表索引",
            "create index orders(o_w_id, o_d_id, o_id);",
            "create index orders(o_w_id, o_d_id, o_c_id, o_id);",
            "create index orders(o_w_id, o_d_id, o_entry_d);",
            "",
            "-- new_orders表索引",
            "create index new_orders(no_w_id, no_d_id, no_o_id);",
            "",
            "-- order_line表索引",
            "create index order_line(ol_w_id, ol_d_id, ol_o_id, ol_number);",
            "create index order_line(ol_w_id, ol_d_id, ol_i_id);",
            "create index order_line(ol_w_id, ol_d_id, ol_o_id);",
            "",
            "-- history表索引",
            "create index history(h_c_w_id, h_c_d_id, h_c_id);",
            "create index history(h_date);",
            ""
        ]

    def generate_data_load_sql(self) -> List[str]:
        """生成数据加载SQL语句"""
        return [
            "-- ========================================",
            "-- 数据加载",
            "-- ========================================",
            "",
            "load ../../src/test/performance_test/table_data1/warehouse.csv into warehouse;",
            "load ../../src/test/performance_test/table_data1/item.csv into item;",
            "load ../../src/test/performance_test/table_data1/district.csv into district;",
            "load ../../src/test/performance_test/table_data1/customer.csv into customer;",
            "load ../../src/test/performance_test/table_data1/stock.csv into stock;",
            "load ../../src/test/performance_test/table_data1/orders.csv into orders;",
            "load ../../src/test/performance_test/table_data1/new_orders.csv into new_orders;",
            "load ../../src/test/performance_test/table_data1/order_line.csv into order_line;",
            "load ../../src/test/performance_test/table_data1/history.csv into history;",
            ""
        ]

    def generate_random_timestamp(self) -> str:
        """生成随机时间戳"""
        base_time = datetime.datetime(2025, 1, 1)
        random_days = random.randint(0, 180)  # 半年内的随机时间
        random_time = base_time + datetime.timedelta(
            days=random_days,
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        return random_time.strftime('%Y-%m-%d %H:%M:%S')

    def generate_random_string(self, length: int) -> str:
        """生成随机字符串"""
        chars = string.ascii_uppercase + string.digits + ' '
        return ''.join(random.choice(chars) for _ in range(length)).strip()

    def generate_new_order_transaction(self) -> List[str]:
        """生成NewOrder事务（使用分区策略）"""
        w_id = self.get_partition_warehouse_id()
        d_id = random.randint(1, self.num_districts_per_warehouse)
        c_id = random.randint(1, self.num_customers_per_district)
        o_id = random.randint(1, self.num_orders_per_district)
        ol_cnt = random.randint(5, 15)
        timestamp = self.generate_random_timestamp()
    
        sql_statements = [
            f"-- NewOrder事务 (W:{w_id}, D:{d_id}, C:{c_id}, O:{o_id}) [分区{self.current_partition_id+1 if self.partition_config else 'N/A'}]",
            "BEGIN;",
            "",
            f"SELECT w_tax FROM warehouse WHERE w_id = {w_id};",
            f"SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = {w_id} AND d_id = {d_id};",
            f"UPDATE district SET d_next_o_id=d_next_o_id+1 WHERE d_w_id = {w_id} AND d_id = {d_id};",
            f"SELECT c_discount, c_last, c_credit FROM customer WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};",
            "",
            f"INSERT INTO orders VALUES ({o_id}, {d_id}, {w_id}, {c_id}, '{timestamp}', 0, {ol_cnt}, 1);",
            f"INSERT INTO new_orders VALUES ({o_id}, {d_id}, {w_id});",
            ""
        ]
    
        # 生成订单行（使用分区item_id）
        for ol_number in range(1, ol_cnt + 1):
            i_id = self.get_partition_item_id()
            supply_w_id = w_id if random.random() < 0.9 else self.get_partition_warehouse_id()
            quantity = random.randint(1, 10)
            amount = round(random.uniform(0.01, 99.99), 2)
            dist_info = self.generate_random_string(24)
    
            sql_statements.extend([
                f"-- 订单行 {ol_number} (Item:{i_id})",
                f"SELECT i_price, i_name, i_data FROM item WHERE i_id = {i_id};",
                f"SELECT s_quantity, s_data, s_dist_{d_id:02d} FROM stock WHERE s_w_id = {supply_w_id} AND s_i_id = {i_id};",
                f"UPDATE stock SET s_quantity=s_quantity-{quantity}, s_order_cnt=s_order_cnt+1 WHERE s_w_id = {supply_w_id} AND s_i_id = {i_id};",
                f"INSERT INTO order_line VALUES ({o_id}, {d_id}, {w_id}, {ol_number}, {i_id}, {supply_w_id}, '', {quantity}, {amount}, '{dist_info}');",
                ""
            ])
    
        sql_statements.extend(["COMMIT;", ""])
        return sql_statements

    def generate_payment_transaction(self) -> List[str]:
        """生成Payment事务（使用分区策略）"""
        w_id = self.get_partition_warehouse_id()
        d_id = random.randint(1, self.num_districts_per_warehouse)
        amount = round(random.uniform(1.0, 5000.0), 2)
        timestamp = self.generate_random_timestamp()

        # 60%按客户ID查询，40%按姓氏查询
        if random.random() < 0.6:
            c_id = random.randint(1, self.num_customers_per_district)
            customer_query = f"SELECT c_first, c_middle, c_last, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};"
            customer_update = f"UPDATE customer SET c_balance=c_balance-{amount}, c_ytd_payment=c_ytd_payment+{amount}, c_payment_cnt=c_payment_cnt+1 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};"
            history_insert = f"INSERT INTO history VALUES ({c_id}, {d_id}, {w_id}, {d_id}, {w_id}, '{timestamp}', {amount}, 'Payment by ID');"
        else:
            last_name = random.choice(self.last_names)
            customer_query = f"SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since FROM customer WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_last = '{last_name}' ORDER BY c_first LIMIT 1;"
            c_id_for_update = random.randint(1, self.num_customers_per_district)
            customer_update = f"UPDATE customer SET c_balance=c_balance-{amount}, c_ytd_payment=c_ytd_payment+{amount}, c_payment_cnt=c_payment_cnt+1 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_last = '{last_name}';"
            history_insert = f"INSERT INTO history VALUES ({c_id_for_update}, {d_id}, {w_id}, {d_id}, {w_id}, '{timestamp}', {amount}, 'Payment by Last Name');"

        return [
            f"-- Payment事务 (W:{w_id}, D:{d_id}, Amount:{amount}) [分区{self.current_partition_id+1 if self.partition_config else 'N/A'}]",
            "BEGIN;",
            "",
            f"SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name FROM warehouse WHERE w_id = {w_id};",
            f"UPDATE warehouse SET w_ytd=w_ytd+{amount} WHERE w_id = {w_id};",
            "",
            f"SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name FROM district WHERE d_w_id = {w_id} AND d_id = {d_id};",
            f"UPDATE district SET d_ytd=d_ytd+{amount} WHERE d_w_id = {w_id} AND d_id = {d_id};",
            "",
            customer_query,
            customer_update,
            "",
            history_insert,
            "",
            "COMMIT;",
            ""
        ]

    def generate_order_status_transaction(self) -> List[str]:
        """生成OrderStatus事务"""
        w_id = random.randint(1, self.num_warehouses)
        d_id = random.randint(1, self.num_districts_per_warehouse)

        # 60%按客户ID查询，40%按姓氏查询
        if random.random() < 0.6:
            c_id = random.randint(1, self.num_customers_per_district)
            customer_query = f"SELECT c_first, c_middle, c_last, c_balance FROM customer WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};"
            order_query = f"SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_c_id = {c_id} ORDER BY o_id DESC LIMIT 1;"
        else:
            last_name = random.choice(self.last_names)
            customer_query = f"SELECT c_first, c_middle, c_id, c_balance FROM customer WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_last = '{last_name}' ORDER BY c_first LIMIT 1;"
            # 使用具体的客户ID进行订单查询
            c_id_for_query = random.randint(1, self.num_customers_per_district)
            order_query = f"SELECT o_id, o_entry_d, o_carrier_id FROM orders WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_c_id = {c_id_for_query} ORDER BY o_id DESC LIMIT 1;"

        # 假设查询最近的订单ID
        recent_o_id = random.randint(1, self.num_orders_per_district)

        return [
            f"-- OrderStatus事务 (W:{w_id}, D:{d_id})",
            "BEGIN;",
            "",
            customer_query,
            "",
            order_query,
            "",
            f"SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d FROM order_line WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} AND ol_o_id = {recent_o_id};",
            "",
            "COMMIT;",
            ""
        ]

    def generate_delivery_transaction(self) -> List[str]:
        """生成Delivery事务"""
        w_id = random.randint(1, self.num_warehouses)
        carrier_id = random.randint(1, 10)
        timestamp = self.generate_random_timestamp()

        sql_statements = [
            f"-- Delivery事务 (W:{w_id}, Carrier:{carrier_id})",
            "BEGIN;",
            ""
        ]

        # 为每个地区处理一个新订单
        for d_id in range(1, self.num_districts_per_warehouse + 1):
            no_o_id = random.randint(self.num_orders_per_district - self.num_new_orders_per_district + 1,
                                   self.num_orders_per_district)
            c_id = random.randint(1, self.num_customers_per_district)
            total_amount = round(random.uniform(50.0, 500.0), 2)

            sql_statements.extend([
                f"-- 处理地区 {d_id}",
                f"SELECT no_o_id FROM new_orders WHERE no_w_id = {w_id} AND no_d_id = {d_id} ORDER BY no_o_id LIMIT 1;",
                f"DELETE FROM new_orders WHERE no_w_id = {w_id} AND no_d_id = {d_id} AND no_o_id = {no_o_id};",
                f"SELECT o_c_id FROM orders WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id = {no_o_id};",
                f"UPDATE orders SET o_carrier_id = {carrier_id} WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id = {no_o_id};",
                f"UPDATE order_line SET ol_delivery_d = '{timestamp}' WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} AND ol_o_id = {no_o_id};",
                f"SELECT SUM(ol_amount) FROM order_line WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} AND ol_o_id = {no_o_id};",
                f"UPDATE customer SET c_balance=c_balance+{total_amount}, c_delivery_cnt=c_delivery_cnt+1 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};",
                ""
            ])

        sql_statements.extend(["COMMIT;", ""])
        return sql_statements

    def generate_stock_level_transaction(self) -> List[str]:
        """生成StockLevel事务"""
        w_id = random.randint(1, self.num_warehouses)
        d_id = random.randint(1, self.num_districts_per_warehouse)
        threshold = random.randint(10, 20)

        # 查询最近20个订单 - 将BETWEEN改为>= AND <=
        recent_orders_start = max(1, self.num_orders_per_district - 20)
        recent_orders_end = recent_orders_start + 19

        return [
            f"-- StockLevel事务 (W:{w_id}, D:{d_id}, Threshold:{threshold})",
            "BEGIN;",
            "",
            f"SELECT d_next_o_id FROM district WHERE d_w_id = {w_id} AND d_id = {d_id};",
            "",
            f"SELECT ol_i_id FROM order_line WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} AND ol_o_id >= {recent_orders_start} AND ol_o_id <= {recent_orders_end};",
            "",
            f"SELECT COUNT(*) AS low_stock_count FROM stock WHERE s_w_id = {w_id} AND s_quantity < {threshold};",
            "",
            "COMMIT;",
            ""
        ]

    def generate_random_transaction(self) -> List[str]:
        """根据权重随机生成一个事务"""
        rand = random.randint(1, 100)

        if rand <= 45:  # 45% NewOrder
            return self.generate_new_order_transaction()
        elif rand <= 88:  # 43% Payment
            return self.generate_payment_transaction()
        elif rand <= 92:  # 4% OrderStatus
            return self.generate_order_status_transaction()
        elif rand <= 96:  # 4% Delivery
            return self.generate_delivery_transaction()
        else:  # 4% StockLevel
            return self.generate_stock_level_transaction()

    def generate_mixed_workload(self, num_transactions: int = 1000) -> List[str]:
        """生成混合工作负载"""
        sql_statements = [
            "-- ========================================",
            f"-- TPC-C混合工作负载 ({num_transactions}个事务)",
            "-- ========================================",
            ""
        ]

        transaction_count = {
            'new_order': 0,
            'payment': 0,
            'order_status': 0,
            'delivery': 0,
            'stock_level': 0
        }

        for i in range(num_transactions):
            # 确定事务类型
            rand = random.randint(1, 100)
            if rand <= 45:
                transaction_type = 'new_order'
                transaction_sql = self.generate_new_order_transaction()
            elif rand <= 88:
                transaction_type = 'payment'
                transaction_sql = self.generate_payment_transaction()
            elif rand <= 92:
                transaction_type = 'order_status'
                transaction_sql = self.generate_order_status_transaction()
            elif rand <= 96:
                transaction_type = 'delivery'
                transaction_sql = self.generate_delivery_transaction()
            else:
                transaction_type = 'stock_level'
                transaction_sql = self.generate_stock_level_transaction()

            transaction_count[transaction_type] += 1

            sql_statements.extend([f"-- 事务 {i+1}: {transaction_type.upper()}"])
            sql_statements.extend(transaction_sql)

            if (i + 1) % 100 == 0:
                print(f"  已生成 {i+1} 个事务...")

        # 添加统计信息
        sql_statements.extend([
            "-- ========================================",
            "-- 事务统计",
            "-- ========================================",
            f"-- 总事务数: {num_transactions}",
            f"-- NewOrder: {transaction_count['new_order']} ({transaction_count['new_order']/num_transactions*100:.1f}%)",
            f"-- Payment: {transaction_count['payment']} ({transaction_count['payment']/num_transactions*100:.1f}%)",
            f"-- OrderStatus: {transaction_count['order_status']} ({transaction_count['order_status']/num_transactions*100:.1f}%)",
            f"-- Delivery: {transaction_count['delivery']} ({transaction_count['delivery']/num_transactions*100:.1f}%)",
            f"-- StockLevel: {transaction_count['stock_level']} ({transaction_count['stock_level']/num_transactions*100:.1f}%)",
            ""
        ])

        return sql_statements

    def generate_performance_test_queries(self) -> List[str]:
        """生成性能测试查询"""
        w_id = random.randint(1, self.num_warehouses)
        d_id = random.randint(1, self.num_districts_per_warehouse)
        c_id = random.randint(1, self.num_customers_per_district)
        last_name = random.choice(self.last_names)

        return [
            "-- ========================================",
            "-- 性能测试查询",
            "-- ========================================",
            "",
            "-- 单点查询测试",
            f"SELECT * FROM warehouse WHERE w_id = {w_id};",
            f"SELECT * FROM customer WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id};",
            "",
            "-- 范围查询测试",
            f"SELECT * FROM orders WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id >= 1000 AND o_id <= 2000;",
            f"SELECT * FROM stock WHERE s_w_id = {w_id} AND s_quantity < 20;",
            "",
            "-- 非主键字段查询测试",
            f"SELECT * FROM customer WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_last = '{last_name}';",
            "SELECT * FROM item WHERE i_price >= 10.00 AND i_price <= 50.00;",
            "",
            "-- 连接查询测试",
            f"SELECT c.c_first, c.c_last, o.o_id, o.o_entry_d FROM customer c, orders o WHERE c.c_w_id = o.o_w_id AND c.c_d_id = o.o_d_id AND c.c_id = o.o_c_id AND c.c_w_id = {w_id} AND c.c_d_id = {d_id};",
            "",
            "-- 聚合查询测试",
            f"SELECT o_w_id, o_d_id, COUNT(*) as order_count FROM orders WHERE o_w_id = {w_id} GROUP BY o_w_id, o_d_id;",
            ""
        ]

    def generate_setup_file(self, filename: str = "setup.sql") -> str:
        """生成设置文件"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            for line in self.generate_setup_sql():
                f.write(line + '\n')
        return filepath
    
    def generate_index_file(self, filename: str = "indexes.sql") -> str:
        """生成索引文件"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            for line in self.generate_index_creation_sql():
                f.write(line + '\n')
        return filepath
    
    def generate_load_file(self, filename: str = "load.sql") -> str:
        """生成加载文件"""
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            for line in self.generate_data_load_sql():
                f.write(line + '\n')
        return filepath
    
    def generate_transaction_files(self, num_files: int = 10, transactions_per_file: int = 100) -> List[str]:
        """生成多个事务文件（使用分区策略）"""
        files = []
        print(f"📝 使用数据分区策略生成 {num_files} 个事务文件...")
        
        for i in range(num_files):
            # 为每个文件设置不同的分区
            self.set_partition_config(i, num_files)
            
            filename = f"transactions_{i+1:03d}.sql"
            filepath = os.path.join(self.output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                # 添加分区信息注释
                f.write(f"-- ========================================\n")
                f.write(f"-- TPC-C事务文件 {i+1}/{num_files} (分区策略)\n")
                f.write(f"-- 仓库范围: {self.partition_config['warehouse_range'][0]}-{self.partition_config['warehouse_range'][1]}\n")
                f.write(f"-- 商品范围: {self.partition_config['item_range'][0]}-{self.partition_config['item_range'][1]}\n")
                f.write(f"-- ========================================\n\n")
                
                for line in self.generate_mixed_workload(transactions_per_file):
                    f.write(line + '\n')
            
            files.append(filepath)
            print(f"  生成事务文件: {filename}")
        
        # 重置分区配置
        self.partition_config = None
        self.current_partition_id = 0
        
        return files
    
    def generate_all_files(self, num_transaction_files: int = 10, transactions_per_file: int = 100) -> Dict[str, List[str]]:
        """生成所有文件"""
        print(f"🚀 开始生成TPC-C测试文件...")
        print(f"  事务文件数: {num_transaction_files}")
        print(f"  每文件事务数: {transactions_per_file}")
        
        files = {
            'setup': [self.generate_setup_file()],
            'indexes': [self.generate_index_file()],
            'load': [self.generate_load_file()],
            'transactions': self.generate_transaction_files(num_transaction_files, transactions_per_file)
        }
        
        print(f"✅ 所有文件生成完成!")
        print(f"  输出目录: {os.path.abspath(self.output_dir)}")
        
        return files

    def generate_all_sql(self, num_transactions: int = 1000, include_setup: bool = True):
        """生成完整的SQL文件"""
        print(f"🚀 开始生成TPC-C事务SQL文件...")
        print(f"  目标事务数: {num_transactions}")
        print(f"  包含初始化: {include_setup}")

        all_sql = []

        if include_setup:
            # 1. 初始设置
            all_sql.extend(self.generate_setup_sql())

            # 2. 索引
            all_sql.extend(self.generate_index_creation_sql())

            # 3. 数据加载
            all_sql.extend(self.generate_data_load_sql())

        # 4. 混合工作负载
        print("📝 生成混合工作负载事务...")
        all_sql.extend(self.generate_mixed_workload(num_transactions))

        # 5. 性能测试查询
        all_sql.extend(self.generate_performance_test_queries())

        # 写入文件
        print(f"💾 写入SQL文件: {self.output_file}")
        with open(self.output_file, 'w', encoding='utf-8') as f:
            for line in all_sql:
                f.write(line + '\n')

        file_size = os.path.getsize(self.output_file) / 1024  # KB
        print(f"✅ SQL文件生成完成!")
        print(f"  文件大小: {file_size:.1f} KB")
        print(f"  文件路径: {os.path.abspath(self.output_file)}")


def main():
    """主函数"""
    print("TPC-C事务语句生成器")
    print("=" * 50)

    # 用户输入配置
    try:
        num_warehouses = int(input("请输入仓库数量 (默认10): ") or "10")
        if num_warehouses < 1:
            print("⚠️ 仓库数量必须大于0，使用默认值10")
            num_warehouses = 10
    except ValueError:
        print("⚠️ 输入无效，使用默认值10")
        num_warehouses = 10

    try:
        num_transactions = int(input("请输入要生成的事务数量 (默认1000): ") or "1000")
        if num_transactions < 1:
            print("⚠️ 事务数量必须大于0，使用默认值1000")
            num_transactions = 1000
    except ValueError:
        print("⚠️ 输入无效，使用默认值1000")
        num_transactions = 1000

    output_file = input("请输入输出SQL文件名 (默认tpcc_transactions.sql): ") or "tpcc_transactions.sql"

    include_setup_input = input("是否包含索引和数据加载语句? (y/n, 默认y): ") or "y"
    include_setup = include_setup_input.lower() == 'y'

    # 创建生成器
    generator = TPCCTransactionGenerator(
        num_warehouses=num_warehouses,
        output_file=output_file
    )

    try:
        generator.generate_all_sql(
            num_transactions=num_transactions,
            include_setup=include_setup
        )
        return 0
    except KeyboardInterrupt:
        print("\n❌ 用户中断生成")
        return 1
    except Exception as e:
        print(f"\n❌ 生成过程中出错: {e}")
        return 1


if __name__ == "__main__":
    import sys
    sys.exit(main())