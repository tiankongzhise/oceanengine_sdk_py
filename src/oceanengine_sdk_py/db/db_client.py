# -*- coding: utf-8 -*-
import os
import pymysql
import tomllib
from typing import Literal
from dotenv import load_dotenv



class DbClient(object):

    cursors_class_dict = {
        'dict': pymysql.cursors.DictCursor,
        'tuple': pymysql.cursors.Cursor
    }

    def __init__(self, db_name: str):
        # 加载 .env 文件
        if not load_dotenv():
            raise FileNotFoundError('未找到 .env 文件')
        with open('db_config.toml', 'rb') as f:
            all_db_config = tomllib.load(f)
        if db_name not in all_db_config:
            self.config = {
                'charset': all_db_config['charset'],
                'cursorclass': all_db_config['cursorclass']
            }
        else:
            self.config = all_db_config.get(db_name)
        self.conn = self.get_db_connect(db_name)

    def get_db_connect(self, db_name: str) -> pymysql.connections.Connection:
        """
        获取数据库连接
        :param db_name: 数据库名称
        :return: 数据库连接
        """
        try:
            conn = pymysql.connect(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT')),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                db=db_name,
                charset=self.config['charset'],
                cursorclass=self.cursors_class_dict[self.config['cursorclass']]
            )
            return conn
        except Exception as e:
            raise e

    def select(self, sql_text: str):
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql_text)
                result = cursor.fetchall()
                return result
        except Exception as e:
            raise e

    def update(self, sql_text: str,data:tuple|list):
        try:
            with self.conn.cursor() as cursor:
                cursor.executemany(sql_text, data)
                self.conn.commit()
                return cursor.rowcount
        except Exception as e:
            raise e

    def insert(self, sql_text: str,data:tuple|list):
        try:
            with self.conn.cursor() as cursor:
                cursor.executemany(sql_text, data)
                self.conn.commit()
                return cursor.rowcount
        except Exception as e:
            raise e

    def close(self):
        try:
            self.conn.close()
        except Exception as e:
            raise e

    def update_conn(self,db_name):
        self.conn = self.get_db_connect(db_name)

    def generate_sql(self,
                     table_name: str,
                     operation:str = Literal['insert', 'update', 'select'],
                     columns: list|None = None,
                     data: dict | list[dict|list|tuple] | tuple[dict|list|tuple] = None,
                     conditions: str|None = None):
        """
        生成 SQL 查询模板和数据
        :param operation: 要执行的操作，可选为insert，update，select
        :param table_name: 表名
        :param columns: 列名列表（可选）
        :param data: 数据字典（可选）
        :param conditions: 约束条件字典（可选）
        :return: SQL 查询模板和数据
        """
        columns_str = ''
        conditions_str = ''
        placeholders = ''


        if operation != 'select':
            if not data  or (isinstance(data,list) and not columns):
                raise ValueError("必须提供列名列表或数据字典")
        else:
            if not columns and not data:
                raise ValueError("必须提供列名列表或数据字典")

        if columns:
            if operation != 'select' and (not isinstance(data,list)  or not isinstance(data,tuple)):
                raise ValueError("列名只能和list或tuple类型一起使用")
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
        else:
            if isinstance(data,dict):
                columns_str = ', '.join(data.keys())
                placeholders = ', '.join(['%s'] * len(data))
            elif isinstance(data[0],dict):
                columns_str = ', '.join(data[0].keys())
                placeholders = ', '.join(['%s'] * len(data[0]))
            else:
                raise ValueError("数据类型错误,data仅支持dict或dict列表")


        if conditions:
            conditions_str = f" WHERE {conditions.replace('{table}', table_name)}"

        if operation == 'insert':
            sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders}){conditions_str}"
        elif operation == 'update':
            sql = f"UPDATE {table_name} SET {', '.join([f'{column} = %s' for column in data.keys()])} {conditions_str}"
        elif operation == 'select':
            sql = f"SELECT {columns_str} from {table_name} {conditions_str}"
        else:
            raise ValueError("操作类型错误")

        if data:
            if isinstance(data,dict):
                values = list(data.values())
            else:
                values = [list(item.values()) for item in data]
        else:
            values = ()
        return sql, values



