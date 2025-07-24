#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
生成themes表SQL插入语句的脚本
根据Satgwong_processed.csv文件中的category_1、category_2、category_3字段
生成相应的SQL INSERT语句来构建分类树结构
"""

import pandas as pd
import re
from typing import Dict, List, Tuple, Set

class ThemesSQLGenerator:
    def __init__(self, csv_file: str):
        self.csv_file = csv_file
        self.themes = {}  # 存储主题信息: {name: {id, level, parent_id, sort_order}}
        self.id_counter = 1
        
    def load_data(self) -> pd.DataFrame:
        """加载CSV数据"""
        print(f"正在读取文件: {self.csv_file}")
        df = pd.read_csv(self.csv_file, encoding='utf-8')
        print(f"共读取 {len(df)} 条记录")
        return df
    
    def clean_category_name(self, name: str) -> str:
        """清理分类名称，去除多余的符号和空格"""
        if pd.isna(name) or name == '':
            return ''
        # 去除首尾空格
        name = str(name).strip()
        # 可以添加更多清理规则
        return name
    
    def extract_unique_categories(self, df: pd.DataFrame) -> Tuple[List[str], Dict[str, List[str]], Dict[str, List[str]]]:
        """提取所有唯一的分类"""
        # 一级分类
        category_1_list = []
        category_1_to_2 = {}  # category_1 -> [category_2, ...]
        category_2_to_3 = {}  # category_2 -> [category_3, ...]
        
        seen_1 = set()
        seen_2 = set()
        seen_3 = set()
        
        for _, row in df.iterrows():
            cat1 = self.clean_category_name(row['category_1'])
            cat2 = self.clean_category_name(row['category_2'])
            cat3 = self.clean_category_name(row['category_3'])
            
            # 处理一级分类
            if cat1 and cat1 not in seen_1:
                category_1_list.append(cat1)
                seen_1.add(cat1)
                category_1_to_2[cat1] = []
            
            # 处理二级分类
            if cat2 and cat1 and cat2 not in seen_2:
                if cat2 not in category_1_to_2[cat1]:
                    category_1_to_2[cat1].append(cat2)
                    seen_2.add(cat2)
                    category_2_to_3[cat2] = []
            
            # 处理三级分类
            if cat3 and cat2 and cat3 not in seen_3:
                if cat2 in category_2_to_3 and cat3 not in category_2_to_3[cat2]:
                    category_2_to_3[cat2].append(cat3)
                    seen_3.add(cat3)
        
        return category_1_list, category_1_to_2, category_2_to_3
    
    def build_themes_structure(self, category_1_list: List[str], 
                             category_1_to_2: Dict[str, List[str]], 
                             category_2_to_3: Dict[str, List[str]]) -> None:
        """构建主题结构"""
        # 处理一级分类
        for i, cat1 in enumerate(category_1_list):
            self.themes[cat1] = {
                'id': self.id_counter,
                'name': cat1,
                'parent_id': None,
                'level': 1,
                'sort_order': i + 1
            }
            self.id_counter += 1
        
        # 处理二级分类
        for cat1, cat2_list in category_1_to_2.items():
            parent_id = self.themes[cat1]['id']
            for i, cat2 in enumerate(cat2_list):
                self.themes[cat2] = {
                    'id': self.id_counter,
                    'name': cat2,
                    'parent_id': parent_id,
                    'level': 2,
                    'sort_order': i + 1
                }
                self.id_counter += 1
        
        # 处理三级分类
        for cat2, cat3_list in category_2_to_3.items():
            if cat2 in self.themes:
                parent_id = self.themes[cat2]['id']
                for i, cat3 in enumerate(cat3_list):
                    self.themes[cat3] = {
                        'id': self.id_counter,
                        'name': cat3,
                        'parent_id': parent_id,
                        'level': 3,
                        'sort_order': i + 1
                    }
                    self.id_counter += 1
    
    def generate_sql(self) -> str:
        """生成SQL INSERT语句"""
        sql_lines = []
        sql_lines.append("-- 自动生成的themes表INSERT语句")
        sql_lines.append("-- 基于Satgwong_processed.csv文件的分类数据")
        sql_lines.append("")
        sql_lines.append("-- 清空现有数据（可选）")
        sql_lines.append("-- DELETE FROM themes;")
        sql_lines.append("-- ALTER SEQUENCE themes_id_seq RESTART WITH 1;")
        sql_lines.append("")
        sql_lines.append("-- 插入主题分类数据")
        
        # 按照级别和排序顺序生成INSERT语句
        themes_by_level = {1: [], 2: [], 3: []}
        for theme_name, theme_info in self.themes.items():
            themes_by_level[theme_info['level']].append((theme_name, theme_info))
        
        # 对每个级别按sort_order排序
        for level in [1, 2, 3]:
            themes_by_level[level].sort(key=lambda x: (x[1]['parent_id'] or 0, x[1]['sort_order']))
        
        # 生成INSERT语句
        for level in [1, 2, 3]:
            if themes_by_level[level]:
                sql_lines.append(f"\n-- {level}级分类")
                for theme_name, theme_info in themes_by_level[level]:
                    parent_id_str = str(theme_info['parent_id']) if theme_info['parent_id'] is not None else 'NULL'
                    
                    # 转义单引号
                    escaped_name = theme_name.replace("'", "''")
                    
                    sql = (f"INSERT INTO themes (id, name, parent_id, level, sort_order, is_active) "
                          f"VALUES ({theme_info['id']}, '{escaped_name}', {parent_id_str}, "
                          f"{theme_info['level']}, {theme_info['sort_order']}, true);")
                    sql_lines.append(sql)
        
        sql_lines.append("")
        sql_lines.append("-- 重置序列（如果需要）")
        sql_lines.append(f"SELECT setval('themes_id_seq', {self.id_counter - 1});")
        
        return '\n'.join(sql_lines)
    
    def count_expressions_by_theme(self, df: pd.DataFrame) -> Dict[str, int]:
        """统计每个主题下的词条数量"""
        theme_counts = {}
        
        # 初始化计数
        for theme_name in self.themes.keys():
            theme_counts[theme_name] = 0
        
        # 统计词条数量
        for _, row in df.iterrows():
            for col in ['category_1', 'category_2', 'category_3']:
                cat = self.clean_category_name(row[col])
                if cat and cat in theme_counts:
                    theme_counts[cat] += 1
        
        return theme_counts
    
    def generate_update_counts_sql(self, theme_counts: Dict[str, int]) -> str:
        """生成更新expression_count的SQL语句"""
        sql_lines = []
        sql_lines.append("\n-- 更新每个主题的词条数量")
        
        for theme_name, count in theme_counts.items():
            if count > 0:
                escaped_name = theme_name.replace("'", "''")
                sql = f"UPDATE themes SET expression_count = {count} WHERE name = '{escaped_name}';"
                sql_lines.append(sql)
        
        return '\n'.join(sql_lines)
    
    def generate_report(self, category_1_list: List[str], 
                       category_1_to_2: Dict[str, List[str]], 
                       category_2_to_3: Dict[str, List[str]]) -> str:
        """生成分析报告"""
        report_lines = []
        report_lines.append("=== 分类结构分析报告 ===")
        report_lines.append(f"一级分类数量: {len(category_1_list)}")
        report_lines.append(f"二级分类数量: {sum(len(cats) for cats in category_1_to_2.values())}")
        report_lines.append(f"三级分类数量: {sum(len(cats) for cats in category_2_to_3.values())}")
        report_lines.append(f"总主题数量: {len(self.themes)}")
        report_lines.append("")
        
        report_lines.append("=== 分类层次结构 ===")
        for cat1 in category_1_list:
            report_lines.append(f"├─ {cat1}")
            if cat1 in category_1_to_2:
                cat2_list = category_1_to_2[cat1]
                for j, cat2 in enumerate(cat2_list):
                    prefix = "├─" if j < len(cat2_list) - 1 else "└─"
                    report_lines.append(f"│  {prefix} {cat2}")
                    if cat2 in category_2_to_3:
                        cat3_list = category_2_to_3[cat2]
                        for k, cat3 in enumerate(cat3_list):
                            sub_prefix = "├─" if k < len(cat3_list) - 1 else "└─"
                            indent = "│  │  " if j < len(cat2_list) - 1 else "   │  "
                            report_lines.append(f"{indent}{sub_prefix} {cat3}")
        
        return '\n'.join(report_lines)
    
    def run(self) -> None:
        """运行主程序"""
        try:
            # 加载数据
            df = self.load_data()
            
            # 提取分类
            print("正在分析分类结构...")
            category_1_list, category_1_to_2, category_2_to_3 = self.extract_unique_categories(df)
            
            # 构建主题结构
            print("正在构建主题结构...")
            self.build_themes_structure(category_1_list, category_1_to_2, category_2_to_3)
            
            # 统计词条数量
            print("正在统计词条数量...")
            theme_counts = self.count_expressions_by_theme(df)
            
            # 生成SQL
            print("正在生成SQL语句...")
            sql_content = self.generate_sql()
            sql_content += self.generate_update_counts_sql(theme_counts)
            
            # 保存SQL文件
            sql_filename = 'themes_insert.sql'
            with open(sql_filename, 'w', encoding='utf-8') as f:
                f.write(sql_content)
            print(f"SQL文件已保存: {sql_filename}")
            
            # 生成报告
            report_content = self.generate_report(category_1_list, category_1_to_2, category_2_to_3)
            report_filename = 'themes_analysis_report.txt'
            with open(report_filename, 'w', encoding='utf-8') as f:
                f.write(report_content)
            print(f"分析报告已保存: {report_filename}")
            
            # 输出统计信息
            print("\n=== 生成完成 ===")
            print(f"一级分类: {len(category_1_list)} 个")
            print(f"二级分类: {sum(len(cats) for cats in category_1_to_2.values())} 个")
            print(f"三级分类: {sum(len(cats) for cats in category_2_to_3.values())} 个")
            print(f"总主题数: {len(self.themes)} 个")
            
        except Exception as e:
            print(f"错误: {e}")
            raise

def main():
    # 设置CSV文件路径
    csv_file = 'Satgwong_processed.csv'
    
    # 创建生成器并运行
    generator = ThemesSQLGenerator(csv_file)
    generator.run()

if __name__ == '__main__':
    main() 