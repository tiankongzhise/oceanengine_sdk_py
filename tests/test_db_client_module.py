# -*- coding: utf-8 -*-
from src.oceanengine_sdk_py.db.db_client import DbClient
import unittest




class test_db_client(unittest.TestCase):
    # def test_init(self):
    #     db_client = DbClient('baidudb')
    #     self.assertIsInstance(db_client,DbClient)
    #     print("test_init down!")
    #
    # def test_generate_sql_select(self):
    #     db_client = DbClient('baidudb')
    #     sql_text,values = db_client.generate_sql(table_name='bd_auth_token',
    #                                      operation='select',
    #                                      columns=['*'],
    #                                      )
    #     print(f"test_generate_sql_select,sql:{sql_text}")
    #     print(f"test_generate_sql_select,values:{values}")
    #     self.assertIsInstance(sql_text,str)
    #     self.assertIsInstance(values,list)
    #
    # def test_create_table(self):
    #     db_client = DbClient('test_db')
    #     sql = """
    #             CREATE TABLE test_db.test_table (
    #         id INT AUTO_INCREMENT PRIMARY KEY,
    #         name VARCHAR(255) NOT NULL,
    #         age INT,
    #         created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    #     );
    #     """
    #     result = db_client.select(sql)
    #     print(f"test_create_table:{result}")
    #     self.assertIsNotNone(result)
    def test_generate_sql_insert_dict(self):
        db_client = DbClient('test_db')
        sql_text,values = db_client.generate_sql(table_name='test_table',
                                          operation='insert',
                                          # columns=['name','age'],
                                          data={'name':'test_name_a','age':18}
                                          )
        print(f"test_generate_sql_insert,sql:{sql_text}")
        print(f"test_generate_sql_insert,values:{values}")

    def test_generate_sql_insert_dict_list(self):
        db_client = DbClient('test_db')
        sql_text,values = db_client.generate_sql(table_name='test_table',
                                          operation='insert',
                                          # columns=['name','age'],
                                          data=[{'name':'test_name_a','age':18},
                                                {'name':'test_name_b','age':19}]
                                          )
        print(f"test_generate_sql_insert,sql:{sql_text}")
        print(f"test_generate_sql_insert,values:{values}")
    def test_generate_sql_insert_dict_truple(self):
        db_client = DbClient('test_db')
        sql_text,values = db_client.generate_sql(table_name='test_table',
                                          operation='insert',
                                          # columns=['name','age'],
                                          data=({'name':'test_name_a','age':18},
                                                {'name':'test_name_b','age':19})
                                          )
        print(f"test_generate_sql_insert,sql:{sql_text}")
        print(f"test_generate_sql_insert,values:{values}")

    def test_generate_sql_insert_list_truple(self):
        db_client = DbClient('test_db')
        sql_text,values = db_client.generate_sql(table_name='test_table',
                                          operation='insert',
                                          columns=['name','age'],
                                          data=(['test_name_a',18],
                                                ['test_name_b',19])
                                          )
        print(f"test_generate_sql_insert,sql:{sql_text}")
        print(f"test_generate_sql_insert,values:{values}")

    def test_generate_sql_insert_list_list(self):
        db_client = DbClient('test_db')
        sql_text,values = db_client.generate_sql(table_name='test_table',
                                          operation='insert',
                                          # columns=['name','age'],
                                          data=[{'name':'test_name_a','age':18},
                                                {'name':'test_name_b','age':19}]
                                          )
        print(f"test_generate_sql_insert,sql:{sql_text}")
        print(f"test_generate_sql_insert,values:{values}")
