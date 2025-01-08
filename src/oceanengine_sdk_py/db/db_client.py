# -*- coding: utf-8 -*-
import sys,os
import pymysql
import tomllib
from typing import Literal,Iterator
from dotenv import load_dotenv
import re


class DbClient(object):

    cursors_class_dict = {
        'dict': pymysql.cursors.DictCursor,
        'tuple': pymysql.cursors.Cursor
    }

    @staticmethod
    def _find_dbconfig_toml(
            filename: str = 'db_config.toml',
            raise_error_if_not_found: bool = False,
            usecwd: bool = False,
    ) -> str:
        """
        Search in increasingly higher folders for the given file

        Returns path to the file if found, or an empty string otherwise
        """

        def _is_interactive():
            """ Decide whether this is running in a REPL or IPython notebook """
            main = __import__('__main__', None, None, fromlist=['__file__'])
            return not hasattr(main, '__file__')

        if usecwd or _is_interactive() or getattr(sys, 'frozen', False):
            # Should work without __file__, e.g. in REPL or IPython notebook.
            path = os.getcwd()
        else:
            # will work for .py files
            frame = sys._getframe()
            current_file = __file__

            while frame.f_code.co_filename == current_file:
                assert frame.f_back is not None
                frame = frame.f_back
            frame_filename = frame.f_code.co_filename
            path = os.path.dirname(os.path.abspath(frame_filename))

        def _walk_to_root(path: str) -> Iterator[str]:
            """
            Yield directories starting from the given directory up to the root
            """
            if not os.path.exists(path):
                raise IOError('Starting path not found')

            if os.path.isfile(path):
                path = os.path.dirname(path)

            last_dir = None
            current_dir = os.path.abspath(path)
            while last_dir != current_dir:
                yield current_dir
                parent_dir = os.path.abspath(os.path.join(current_dir, os.path.pardir))
                last_dir, current_dir = current_dir, parent_dir

        for dirname in _walk_to_root(path):
            check_path = os.path.join(dirname, filename)
            if os.path.isfile(check_path):
                return check_path

        if raise_error_if_not_found:
            raise IOError('File not found')

        return ''

    def __init__(self, db_name: str):
        # 加载 .env 文件
        if not load_dotenv():
            raise FileNotFoundError('未找到 .env 文件')

        # 加在数据库配置文件
        db_config_path = self._find_dbconfig_toml()
        if not db_config_path:
            raise FileNotFoundError('未找到 db_config.toml 文件')
        with open(db_config_path, 'rb') as f:
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
    @staticmethod
    def _check_generate_sql_params(
                                   operation:str = Literal['insert', 'update', 'select'],
                                   columns: list|None = None,
                                   data: dict | list[dict|list|tuple] | tuple[dict|list|tuple] = None,
                                   ):
        if operation == 'update':
            if not isinstance(data,dict):
                raise ValueError("模式为update时，data参数必须为dict")
            else:
                if not columns:
                    return True
                if all([column in data.keys() for column in columns]):
                    return True
                else:
                    raise ValueError("columns参数中存在data参数中不存在的列")



        if columns:
            if data is None:
                if operation == 'select':
                    return True
                else:
                    raise ValueError("模式为非select时，columns与data参数不能同时为非None")
            else:
                if isinstance(data,dict):
                    raise ValueError("data参数为dict时，columns参数必须为None")
                elif isinstance(data,list) or isinstance(data,tuple):
                    if isinstance(data[0],list) or isinstance(data[0],tuple):
                        return True
                    elif isinstance(data[0],dict):
                        if all([column in data[0].keys() for column in columns]):
                            return True
                        else:
                            raise ValueError("columns参数中存在data参数中不存在的列")
                    else:
                        raise ValueError(f"data参数为list或tuple时，元素只能为list,tuple,dict，此类型尚不支持{type(data[0])}")
                else:
                    raise ValueError("data参数类型错误,data必须为dict或者由dict组成的list或tuple")
        else:
            if isinstance(data,dict):
                return True
            if isinstance(data,list) or isinstance(data,tuple):
                if  isinstance(data[0],dict):
                    return True
                else:
                    raise ValueError("data参数为list或tuple时，每个元素必须为dict")
        raise ValueError("未知错误，发生在_check_generate_sql_params，"
                         f"columns:{columns} "
                         f"data:{data}")
    @staticmethod
    def _parse_update_conditions(conditions: str) -> tuple:
        # 使用正则表达式匹配 {} = %s 中的 {}，
        matches = re.findall(r"\{(\w+)\}\s*=\s*%s", conditions)
        new_conditions = conditions
        for match in matches:
            new_conditions = new_conditions.replace(f"{{{match}}}", f'`{match}`')
        # 使用正则表达式匹配 new_conditions 中的 {}，
        sec_matches = re.findall(r"\{(\w+)\}", new_conditions)
        for match in sec_matches:
            new_conditions = new_conditions.replace(f"{{{match}}}", f'`{match}`')

        conditions_str = new_conditions


        return conditions_str, matches

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
        update_columns = []

        self._check_generate_sql_params(operation,columns,data)

        if operation == 'update':
            if columns:
                columns_str = ', '.join([f'`{column}` = %s' for column in columns])
            else:
                if isinstance(data, dict):
                    columns_str = ', '.join([f'`{column}` = %s' for column in data.keys()])
                elif isinstance(data[0], dict):
                    columns_str = ', '.join([f'`{column}` = %s' for column in data[0].keys()])
                else:
                    raise ValueError("update的sql生成失败,data仅支持dict或dict列表")
        else:
            if columns:
                columns_str = '`, `'.join(columns)
                placeholders = ', '.join(['%s'] * len(columns))
            else:
                if isinstance(data,dict):
                    columns_str = '`, `'.join(data.keys())
                    placeholders = ', '.join(['%s'] * len(data))
                elif isinstance(data[0],dict):
                    columns_str = '`, `'.join(data[0].keys())
                    placeholders = ', '.join(['%s'] * len(data[0]))
                else:
                    raise ValueError("数据类型错误,data仅支持dict或dict列表")



        if conditions:
            if operation == 'update':
                conditions_str, update_columns = self._parse_update_conditions(conditions)
                conditions_str = f" WHERE {conditions_str}"
            else:
                conditions_str = f" WHERE {conditions.replace('{table}', table_name)}"

        if operation == 'insert':
            sql = f"INSERT INTO {table_name} (`{columns_str}`) VALUES ({placeholders}){conditions_str}"
        elif operation == 'update':
            sql = f"UPDATE {table_name} SET {columns_str} {conditions_str}"
        elif operation == 'select':
            sql = f"SELECT `{columns_str}` from {table_name} {conditions_str}"
        else:
            raise ValueError("操作类型错误")
        if data:
            if isinstance(data,dict):
                if columns:
                    values = [[data[column] for column in columns]]
                else:
                    values = [list(data.values())]

            elif isinstance(data[0],dict):
                if columns:
                    values = [[item[column] for column in columns] for item in data]
                else:
                    values = [list(item.values()) for item in data]
            elif isinstance(data[0],tuple):
                values = [list(item) for item in data]
            elif isinstance(data[0],list):
                values = [item for item in data]
            else:
                raise ValueError(f"values的list转化失败，data:{data}")

        else:
            values = []
        if update_columns:
            values[0].extend([data[key] for key in update_columns])

        if operation == 'select':
            if '`*`' in sql:
                sql = sql.replace('`*`', '*')

        return sql, values



