import re
import pandas as pd


# from py_data_analytics_prep.src.oceanengine_sdk_py import OceanengineSdkClient
# from py_data_analytics_prep.src.data_analytics_prep.db_module import DbModule

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


class TestOceanengineClient(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(TestOceanengineClient, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    @classmethod
    def get_instance(cls):
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        db_service = DbModule()
        define_db_connect = db_service.get_db_connect()
        oceanengine_auth_old = db_service.get_bd_auth(define_db_connect, '创想策划汇-代理商')[0]
        oceanengine_auth_old = convert_dict_keys(oceanengine_auth_old, "snake")
        print(f"old auth token: {oceanengine_auth_old}")
        self.client = OceanengineSdkClient(oceanengine_auth_old)
        rsp = self.client.oauth_sign()
        rsp = convert_dict_keys(rsp, "small_camel")
        print(f'new auth token: {rsp}')
        update_result = db_service.update_auth_sql(define_db_connect, rsp, oceanengine_auth_old['user_name'])
        print(f"mysql oauth update: {update_result}")

    def test_customer__center_advertiser_list_adApi(self):
        query = {
            "cc_account_id": '1800168496063497',
        }
        _data = self.client.customer__center_advertiser_list_adApi(query)
        print(_data)

    def test_business__platform_partner__organization_list_adApi(self):
        partner_organization_query = {
            "organization_id": 1800168496063497
        }
        _data = self.client.business__platform_partner__organization_list_adApi(partner_organization_query)
        print(_data)

    def test_v3___0_business__platform_company__info_get(self):
        query = {
            "organization_id": 1800168496063497
        }
        _data = self.client.v3___0_business__platform_company__info_get(query)
        print(_data)

    def test_v3___0_business__platform_company__account_get(self):
        company_account_query = {
            "organization_id": 1800168496063497,
            "company_id": 1000995637,
            "account_type": ['AD', 'QIANCHUAN'],
            'page_size': 100
        }
        _data = self.client.v3___0_business__platform_company__account_get(company_account_query)
        print(_data)

    def test_advertiser_info_adApi(self):
        advertiser_info_query = {
            "advertiser_ids": [1801616765879323]
        }
        _data = self.client.advertiser_info_adApi(advertiser_info_query)
        print(_data)

    def test_advertiser_public__info_adApi(self):
        advertiser_info_query = {
            "advertiser_ids": [1801616765879323]
        }
        _data = self.client.advertiser_public__info_adApi(advertiser_info_query)
        print(_data)

    def test_advertiser_avatar_upload(self):
        advertiser_avatar_upload_query = {
            "advertiser_id": 1801616765879323,
            "image_file": "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQV"
                          "QImWNgYGBgAAAABQABh6FO1AAAAABJRU5Erk Jggg=="
        }
        _data = self.client.advertiser_avatar_upload(advertiser_avatar_upload_query)
        print(_data)

    def test_v3___0_report_custom_config_get(self):
        customer_config_get_query = {
            "advertiser_id": 1801616765879323,
            'data_topics': ['BASIC_DATA']
        }
        _data = self.client.v3___0_report_custom_config_get(customer_config_get_query)
        dimensions_list = _data['data']['list'][0]['dimensions']
        metrics_list = _data['data']['list'][0]['metrics']
        df_dimensions = pd.DataFrame(dimensions_list)
        df_metrics = pd.DataFrame(metrics_list)
        df_dimensions.to_csv('dimensions.csv', index=False, encoding='utf-8_sig')
        df_metrics.to_csv('metrics.csv', index=False, encoding='utf-8_sig')

    def test_v3___0_report_custom_get(self):
        query = {
            "advertiser_id": 1802369766232155,
            "data_topics": 'BASIC_DATA',
            "dimensions": ['stat_time_day', 'cdp_project_id', 'cdp_project_name',
                           'cdp_promotion_id', 'cdp_promotion_name', 'app_code',
                           'gender', 'ac', 'platform', 'image_mode'],
            "metrics": ['stat_cost', 'show_cnt', 'click_cnt'],
            "filters": [],
            "start_time": '2024-07-01',
            "end_time": '2024-08-31',
            "order_by": [{'field': "stat_time_day", "type": 'DESC'},
                         {'field': "stat_cost", "type": 'DESC'}],
            "page_size": 100
        }
        _data = self.client.v3___0_report_custom_get(query)
        print(_data)

    def test_report_advertiser_get_adApi(self):
        query = {
            "advertiser_id": 1802369766232155,
            "start_date": '2024-07-01',
            "end_date": '2024-07-31',
            "page_size": 100
        }
        _data = self.client.report_advertiser_get_adApi(query)
        print(_data)

    def test_report_agent_get__v2_adApi(self):
        query = {
            "agent_id": 1800168496063497,
            "start_date": '2024-07-01',
            "end_date": '2024-07-31',
            "cursor_size": 1000
        }
        _data = self.client.report_agent_get__v2_adApi(query)
        print(_data)

    def test_agent_adv_cost__report_list_query(self):
        query = {
            "agent_id": 1800168496063497,
            "start_date": '2024-07-01',
            "end_date": '2024-07-31',
            "page": 1000,
            "filtering": [{"advertiser_ids": 1800168496063497}]
        }
        _data = self.client.agent_adv_cost__report_list_query(query)
        print(_data)

    def test_agent_advertiser_select(self):
        query = {
            "advertiser_id ":92403628013,
        }
        _data = self.client.agent_advertiser_select(query)
        print(_data)

    def test_agent_info_adApi(self):
        query = {
            "advertiser_ids ": [1817570459492448],
        }
        _data = self.client.agent_info_adApi(query)
        print(_data)



if __name__ == '__main__':
    test_client = TestOceanengineClient.get_instance()
    test_client.test_v3___0_report_custom_get()
