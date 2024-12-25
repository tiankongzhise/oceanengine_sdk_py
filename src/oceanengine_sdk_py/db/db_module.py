import sys

import pymysql
import os
import copy
from datetime import datetime
from typing import (Optional,
                    overload,
                    Union,
                    NoReturn,
                    TYPE_CHECKING,
                    Literal,
                    List
                    )
from configobj import ConfigObj
from inspect import currentframe, getouterframes
from dotenv import load_dotenv

# 加载 .env 文件
load_dotenv()

import pandas as pd
from ..local_log.DAP_logging import DAPLogging



from pathlib import Path
def find_project_root(start_dir=None, marker_files=('LICENSE', '.gitignore', '.git')):
    if start_dir is None:
        start_dir = Path.cwd()

    current_dir = Path(start_dir).resolve()

    while True:
        for marker in marker_files:
            if (current_dir / marker).exists():
                return current_dir
        parent_dir = current_dir.parent
        if parent_dir == current_dir:
            # Reached the root of the filesystem without finding a marker
            return None
        current_dir = parent_dir


BASE_DIR = find_project_root()
DEFINE_CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')







class DbModule(object):
    """
    数据库模块
    """

    def __init__(self):
        # self.db_config_file = DEFINE_CONFIG_PATH
        self.db_config = ConfigObj(DEFINE_CONFIG_PATH)

    @overload
    def get_db_connect(self,
                       item: Optional[str] = None
                       ) -> Optional[pymysql.connections.Connection]:
        ...

    @overload
    def get_db_connect(self,
                       item: Optional[dict] = None) -> Optional[pymysql.connections.Connection]:
        ...

    @DAPLogging
    def get_db_connect(self,
                       item: Optional[Union[str, dict]] = None
                       ) -> Union[pymysql.connections.Connection, NoReturn]:
        """
        获取数据库连接
        :param item: 配置文件中的数据库连接配置项
        :return: 数据库连接
        """
        # 将配置中的字符串转为pymysql的合格pymysql.cursorsde参数
        cursors_class_dict = {
            'pymysql.cursors.DictCursor': pymysql.cursors.DictCursor
        }

        # cfg = ConfigObj(self.db_config_file)
        cfg = copy.deepcopy(self.db_config)
        # 如果未指定连接哪个数据库，则默认连接配置表中第一个数据库
        if item is None:
            db_config = cfg['db'][cfg['db'].keys()[0]]
        # 若输入为字典，则将输入字典作为数据库配置文件
        elif isinstance(item, dict):
            db_config = item
        # 若输入为字符串，则连接相对于的配置文件设置
        elif isinstance(item, str):
            db_config = cfg['db'].get(item)
        else:
            raise TypeError('item must be str or dict')
            sys.exit('数据库链接建立失败！')
        # 防止item输入了错误的值
        if db_config is None:
            raise KeyError(f'{item}没有对应的配置文件，请检查输入！')
            sys.exit('数据库链接建立失败！')
        try:
            db_connect = pymysql.connect(
                host=os.getenv('DB_HOST'),
                port=int(os.getenv('DB_PORT')),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASSWORD'),
                db=db_config['db'],
                charset=db_config['charset'],
                cursorclass=cursors_class_dict[db_config['cursorclass']]
            )
            return db_connect
        except Exception as e:
            raise e
            sys.exit('数据库链接建立失败！')

    @DAPLogging
    def get_bd_auth(self,
                    pymysql_connect: pymysql.connections.Connection,
                    username: str,
                    ) -> dict:
        """
        获取百度授权信息
        :return: dict
        """
        try:
            cur = pymysql_connect.cursor()
            # cfg = ConfigObj(self.db_config_file)
            cfg = copy.deepcopy(self.db_config)
            sql = f'select * from {cfg["db"]["bd_auth"]["table_name"]} where userName="{username}"'
            DAPLogging.debug(f'debug|get_bd_auth sql:{sql}')
            cur.execute(sql)
            pymysql_connect.commit()
            return cur.fetchall()
        except Exception as e:
            pymysql_connect.rollback()
            raise e
            sys.exit(f'从数据库读取百度auth认证信息失败，错误原因为:{e}')

    @DAPLogging
    def update_bd_auth(self,
                       pymysql_connect: pymysql.connections.Connection,
                       bd_auth: Union['BaiduAdsClient', dict],
                       user_flag: Optional[Union[str, int]] = None,
                       ) -> dict:
        """
        更新百度授权信息
        :return: NoReturn
        """
        try:
            cur = pymysql_connect.cursor()
            # cfg = ConfigObj(self.db_config_file)
            cfg = copy.deepcopy(self.db_config)

            if not TYPE_CHECKING:
                if __package__:
                    from .get_bd_sem_data import BaiduAdsClient
                else:
                    from get_bd_sem_data import BaiduAdsClient
                if isinstance(bd_auth, BaiduAdsClient):
                    update_operation = f'accessToken ="{bd_auth.access_token}",' \
                                       f'refreshToken ="{bd_auth.refresh_token}",' \
                                       f' expiresTime ="{bd_auth.expires_time}", ' \
                                       f'refreshExpiresTime ="{bd_auth.refresh_expires_time}"'
                    user_flag = bd_auth.user_id
                elif isinstance(bd_auth, dict):
                    update_operation = ','.join(
                        [f'{key}={values}' if isinstance(values, int) else f'{key}="{values}"' for key, values in
                         bd_auth.items()])
                else:
                    raise TypeError('bd_auth must be dict or BaiduAdsClient')
            else:
                raise TypeError(f'bd_auth not TYPE_CHECKING,type is {type(bd_auth)}')

            if isinstance(user_flag, str):
                sql = f'update {cfg["db"]["bd_auth"]["table_name"]} set {update_operation} where userName="{user_flag}"'
            elif isinstance(user_flag, int):
                sql = f'update {cfg["db"]["bd_auth"]["table_name"]} set {update_operation} where userId={user_flag}'
            else:
                raise TypeError('user_flag must be str or int')
            DAPLogging.debug(f'debug|update_operation:{update_operation}')
            DAPLogging.debug(f'debug|sql={sql}')
            cur.execute(sql)
            pymysql_connect.commit()
            return cur.rowcount
        except Exception as e:
            pymysql_connect.rollback()
            raise e
            sys.exit(f'更新百度auth认证信息失败，错误原因为:{e}')

    @DAPLogging
    def insert_bd_report_data(self,
                              pymysql_connect: pymysql.connections.Connection,
                              bd_report_data: dict,
                              table_name: str,
                              batch_size: int = 100000,
                              insert_module: Literal['inser', 'ignore', 'replace'] = 'insert',
                              ) -> int:
        """
        插入百度报告数据
        :param bd_report_data: dict
        :param table_name: str
        :return: int
        """
        try:
            with pymysql_connect.cursor() as cursor:
                column_name = ','.join(bd_report_data['table_header'])
                placeholder = ','.join(['%s' for i in bd_report_data['table_header']])
                match insert_module:
                    case 'insert':
                        sql_operation = 'insert into'
                    case 'ignore':
                        sql_operation = 'insert ignore into'
                    case 'replace':
                        sql_operation = 'replace into'
                    case _:
                        raise ValueError('insert_module must be insert or ignore or replace')

                sql = f"{sql_operation} {table_name}({column_name}) VALUES({placeholder})"
                data = bd_report_data['data']
                # 分批次处理数据
                for i in range(0, len(data), batch_size):
                    # 获取当前批次的数据，确保不超过实际数据长度
                    end_index = min(i + batch_size, len(data))
                    # 获取当前批次的数据
                    batch_data = data[i:end_index]

                    # 执行批量插入
                    cursor.executemany(sql, batch_data)
                # 提交事务
                pymysql_connect.commit()
                return cursor.rowcount
        except Exception as e:
            # 如果出现异常，则回滚事务
            pymysql_connect.rollback()
            raise e

    @DAPLogging
    def etl_select(self,
                   conn: Optional[Union[pymysql.connections.Connection, str]] = None,
                   query: Optional[str] = None):
        if query is None:
            return f'query is None,nothing is done'

        match conn:
            case None:
                t_conn = self.get_db_connect()
            case str():
                t_conn = self.get_db_connect(conn)
            case conn if isinstance(conn, pymysql.connections.Connection):
                t_conn = conn
            case _:
                raise TypeError('conn must be str or pymysql.connections.Connection')

        frame = currentframe()
        # 获取外部帧，即调用者的帧
        outer_frames = getouterframes(frame)

        call_by = outer_frames[2].function

        try:
            with t_conn.cursor() as cursor:
                try:
                    cursor.execute(query)
                    t_conn.commit()
                    return cursor.rowcount
                except Exception as e:
                    # Handle cursor-level exceptions here
                    error_info = f"{call_by} error: {repr(e)}"
                    DAPLogging.error(error_info)
                    # Attempt to rollback the transaction if possible
                    try:
                        t_conn.rollback()
                    except Exception as rollback_e:
                        DAPLogging.error(f"{call_by} error "
                                         f"Rollback failed: {repr(rollback_e)}")
                    raise e
        except Exception as outer_e:
            # Handle connection or other outer-level exceptions here
            DAPLogging.error(f"{call_by} "
                             f"Connection or outer-level error: {repr(outer_e)}")
            raise outer_e

    @DAPLogging
    def insert_df(self,
                  df: pd.DataFrame,
                  pymysql_connect: pymysql.connections.Connection,
                  table_name: str,
                  insert_module: Literal['insert', 'ignore', 'replace'] = 'insert',
                  ) -> int:
        try:
            # 获取数据库连接
            t_conn = pymysql_connect

            # 保持兼容性知道在那个函数
            call_by = 'insert_df'

            match insert_module:
                case 'insert':
                    sql_operation = 'insert into'
                case 'ignore':
                    sql_operation = 'insert ignore into'
                case 'replace':
                    sql_operation = 'replace into'
                case _:
                    raise ValueError('insert_module must be insert or ignore or replace')

            # 获取DataFrame的列名
            columns = df.columns.tolist()
            # 构建SQL语句
            sql_columns = ', '.join(columns)
            sql_values = ', '.join(['%s'] * len(columns))
            sql = f"{sql_operation} {table_name} ({sql_columns}) VALUES ({sql_values});"

            data = [tuple(x) for x in df.to_numpy()]

            with t_conn.cursor() as cursor:
                try:
                    cursor.executemany(sql, data)
                    t_conn.commit()
                    return cursor.rowcount
                except Exception as e:
                    # Handle cursor-level exceptions here
                    error_info = f"{call_by} error: {repr(e)}"
                    DAPLogging.error(error_info)
                    # Attempt to rollback the transaction if possible
                    try:
                        t_conn.rollback()
                    except Exception as rollback_e:
                        DAPLogging.error(f"{call_by} error "
                                         f"Rollback failed: {repr(rollback_e)}")
                    raise e
        except Exception as outer_e:
            # Handle connection or other outer-level exceptions here
            DAPLogging.error(f"{call_by} "
                             f"Connection or outer-level error: {repr(outer_e)}")
            raise outer_e
    @DAPLogging
    def get_table_name(self,
                       db_conn_status: str,
                       table_status: str
                       ) -> str:
        return self.db_config['db'][db_conn_status][table_status]

    @DAPLogging
    def select_data(self,
               pymysql_connect: pymysql.connections.Connection,
               sql_text: str) -> List[dict]:
        try:
            with pymysql_connect.cursor() as cursor:
                cursor.execute(sql_text)
                pymysql_connect.commit()
                return cursor.fetchall()
        except Exception as e:
            # 如果出现异常，则回滚事务
            pymysql_connect.rollback()
            raise e

    def update_auth_sql(self,
                        pymysql_connect: pymysql.connections.Connection,
                        auth_info: dict,
                        user_flag:Optional[Union[str,int]] = None
                       ) -> str:
        try:
            cfg = copy.deepcopy(self.db_config)
            cur = pymysql_connect.cursor()
            update_operation = ','.join(
                        [f'{key}={values}' if isinstance(values, int) else f'{key}="{values}"' for key, values in
                         auth_info.items()])
            if user_flag is None:
                sql = f'update {cfg["db"]["bd_auth"]["table_name"]} set {update_operation} where userName="{auth_info["userName"]}"'
            elif isinstance(user_flag, str):
                sql = f'update {cfg["db"]["bd_auth"]["table_name"]} set {update_operation} where userName="{user_flag}"'
            elif isinstance(user_flag, int):
                sql = f'update {cfg["db"]["bd_auth"]["table_name"]} set {update_operation} where userId={user_flag}'
            else:
                raise TypeError('user_flag must be str or int')
            DAPLogging.debug(f'debug|update_operation:{update_operation}')
            DAPLogging.debug(f'debug|sql={sql}')
            cur.execute(sql)
            pymysql_connect.commit()
            return cur.rowcount
        except Exception as e:
            pymysql_connect.rollback()
            raise e
            sys.exit(f'更新百度auth认证信息失败，错误原因为:{e}')
