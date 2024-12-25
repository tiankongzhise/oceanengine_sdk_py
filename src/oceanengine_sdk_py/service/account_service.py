import inspect

from collections.abc import Mapping

from src.oceanengine_sdk_py.service.oceanengine_sdk_base import OceanengineSdkBase


class AccountService(OceanengineSdkBase):
    """
    由巨量引擎API自动生成的sdk接口，实现与API完全相同的入参要求与返回结果。方法名称为url按名称转化。
    转化规则:
    所有访问https://ad.oceanengine.com/open_api/的API，按以下规则转化为方法名称，部分API存在特殊规则，见特殊规则说明
    ad_api_url_prefix = "https://ad.oceanengine.com/open_api/"
    api_url.replace(ad_api_url_prefix,'').replace('_','__').replace('.','___').replace('/','_')[:-1]+'_adApi'

    所有访问https://api.oceanengine.com/open_api/的API，按以下规则转化为方法名称
    open_api_url_prefix = "https://api.oceanengine.com/open_api/"
    api_url.replace(open_api_url_prefix,'').replace('_','__').replace('.','___').replace('/','_')[:-1]

    特殊规则说明:
    部分API，在去除url_prefix后以纯数字为开头，无法作为方法名称，修改方法名称，将开头的数字与_全部去除，正则规则如下:
    re.match(r'^((\\d+)_*)*', s)
    如果出现特殊情况，在方法中func_name添加额外修正，
    .replace("func_name = func_name",f'func_name = "{del_char}" + func_name ')

    参数:
    params为巨量引擎要求的请求参数，参数类型参见巨量引擎对应API说明，以dict形式传入。默认为None。

    返回值:
    如果http返回200，则按照API接口返回数据转换为JSON后返回。
    如果http结果不为200，raise一个关于相关错误的DICT
    包括func_name，请求url，http_methods，请求参数params，rsp的text，以及rsp的status_code
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    # 工作台相关API
    def customer__center_advertiser_list_adApi(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = "2_" + func_name
        http_method = 'get'
        return self.make_request(func_name, params, http_method)

    def business__platform_partner__organization_list_adApi(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = "2_" + func_name
        http_method = 'get'
        return self.make_request(func_name, params, http_method)

    def v3___0_business__platform_company__info_get(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = func_name
        http_method = 'get'
        return self.make_request(func_name, params, http_method)

    def v3___0_business__platform_company__account_get(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = func_name
        http_method = 'get'
        return self.make_request(func_name, params, http_method)

    # 广告主相关API
    def advertiser_info_adApi(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = "2_" + func_name
        http_method = 'get'
        return self.make_request(func_name, params, http_method)

    def advertiser_public__info_adApi(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = "2_" + func_name
        http_method = 'get'
        return self.make_request(func_name, params, http_method)

    def advertiser_avatar_upload(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = "2_" + func_name
        http_method = 'post'
        special_headers = {
            "Content-Type": "multipart/form-data"
        }
        return self.make_request(func_name, params, http_method, special_headers)

    # 代理商相关API
    def agent_advertiser_select(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = "2_" + func_name
        http_method = 'get'
        return self.make_request(func_name, params, http_method)

    def agent_advertiser__info_query(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = "2_" + func_name
        http_method = 'get'
        return self.make_request(func_name, params, http_method)

    def agent_info_adApi(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = "2_" + func_name
        http_method = 'get'
        return self.make_request(func_name, params, http_method)
