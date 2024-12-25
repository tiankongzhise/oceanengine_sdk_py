# -*- coding: utf-8 -*-
import re
import pandas as pd
import unittest
import datetime

from src.oceanengine_sdk_py import OceanengineSdkClient
from src.oceanengine_sdk_py.db.db_module import DbModule

def camel_to_snake(key):
    """
    将驼峰命名法的字符串转换为下划线命名法。

    :param key: 驼峰命名法的字符串
    :return: 下划线命名法的字符串
    """
    # 使用正则表达式将驼峰命名法转换为下划线命名法
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', key)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def snake_to_small_camel(snake_str):
    """
    将下划线命名法的字符串转换为驼峰命名法。

    :param snake_str: 下划线命名法的字符串
    :return: 驼峰命名法的字符串
    """
    components = snake_str.split('_')
    # 将第一个组件保持原样，其余组件首字母大写，然后连接起来
    return components[0] + ''.join(x.capitalize() or '_' for x in components[1:])


def snake_to_big_camel(snake_str):
    """
    将下划线命名法的字符串转换为驼峰命名法。

    :param snake_str: 下划线命名法的字符串
    :return: 驼峰命名法的字符串
    """
    components = snake_str.split('_')
    # 将每个组件的首字母大写，然后连接起来
    return ''.join(x.capitalize() or '_' for x in components)


def convert_dict_keys(d, conventions):
    """
    将字典的键从驼峰命名法转换为下划线命名法。

    :param d: 输入的字典
    :return: 转换后的字典
    """
    if conventions == 'snake':
        return {camel_to_snake(k): v for k, v in d.items()}
    if conventions == 'small_camel':
        return {snake_to_small_camel(k): v for k, v in d.items()}
    if conventions == 'big_camel':
        return {snake_to_big_camel(k): v for k, v in d.items()}


def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + a + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out


class TestServiceOauth2(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db_service = DbModule()
        cls.define_db_connect = cls.db_service.get_db_connect()
        oceanengine_auth_old = cls.db_service.get_bd_auth(cls.define_db_connect, '创想策划汇-代理商')[0]
        oceanengine_auth_old = convert_dict_keys(oceanengine_auth_old, "snake")
        cls.client = OceanengineSdkClient(oceanengine_auth_old)

    def tearDown(self):
        rsp_data = self.rsp
        current_time = datetime.datetime.now()
        if rsp_data['code'] != 0:
            raise Exception(rsp_data['message'])
        self.client.access_token = rsp_data['data']['access_token']
        self.client.refresh_token = rsp_data['data']['refresh_token']
        self.client.expires_time = current_time + datetime.timedelta(seconds=rsp_data['data'].get('expires_in', 0))
        self.client.refresh_expires_time = current_time + datetime.timedelta(
                           seconds=rsp_data['data'].get('refresh_token_expires_in', 0))
        rsp = convert_dict_keys(vars(self.client), "small_camel")
        update_result = self.db_service.update_auth_sql(self.define_db_connect, rsp, rsp['userName'])

    def test_oauth2_access__token_adApi(self):
        query = {
                "app_id": self.client.app_id,
                "secret": self.client.secret_key,
                "auth_code": self.client.auth_code
            }
        self.rsp = self.client.oauth2_access__token_adApi(query)
        self.assertEqual(self.rsp['code'], 0)
        self.assertIn('access_token', self.rsp['data'])
        self.assertIn('refresh_token', self.rsp['data'])

    def test_oauth2_refresh__token(self):
        query = {
                "app_id": self.client.app_id,
                "secret": self.client.secret_key,
                "refresh_token": self.client.refresh_token
            }
        self.rsp = self.client.oauth2_refresh__token(query)
        self.assertEqual(self.rsp['code'], 0)
        self.assertIn('access_token', self.rsp['data'])
        self.assertIn('refresh_token', self.rsp['data'])