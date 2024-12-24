from typing import Optional, Literal, Iterable
from collections.abc import Mapping
import json
import requests


class OceanengineSdkBase(object):
    __open_api_url_prefix = "https://api.oceanengine.com/open_api/"
    __ad_api_url_prefix = "https://ad.oceanengine.com/open_api/"

    def from_dict(self, config: Mapping):
        self.app_id = config.get('app_id')
        self.secret_key = config.get('secret_key')
        self.access_token = config.get('access_token')
        self.refresh_token = config.get('refresh_token')
        self.expires_time = config.get('expires_time')
        self.refresh_expires_time = config.get('refresh_expires_time')
        self.auth_code = config.get('auth_code')
        self.user_id = config.get('user_id')
        self.user_name = config.get('user_name')

    def from_args(self, *args, **kwargs):
        args_keys = ['app_id',
                     'secret_key',
                     'access_token',
                     'refresh_token',
                     'expires_time',
                     'refresh_expires_time']
        for i in range(len(args)):
            setattr(self, args_keys[i], args[i])

        for k, v in kwargs.items():
            setattr(self, k, v)

    def make_request(self,
                     call_func_name: str,
                     params: Mapping | None,
                     http_methods: Literal['get', 'put'] = 'put',
                     special_headers: Mapping | None = None
                     ) -> Mapping[str, any]:
        # print("call common method")
        api_uri = call_func_name.replace('___', '.').replace('__', '~').replace('_', '/').replace('~', '_') + '/'
        if "adApi" in api_uri:
            pre_url = self.__ad_api_url_prefix
            api_uri = api_uri.replace("adApi/", "")
        else:
            pre_url = self.__open_api_url_prefix

        api_url = pre_url + api_uri
        # print(f"The url is: {api_url}")
        headers = {
            "Access-Token": self.access_token
        }
        std_params = {k: json.dumps(v) if isinstance(v, Iterable) and not isinstance(v, (str, bytes)) else v for k, v in
                      params.items()}

        if special_headers:
            headers.update(special_headers)
        try:
            if params is None:
                rsp = requests.request(method=http_methods, url=api_url, headers=headers)
            else:
                rsp = requests.request(method=http_methods, url=api_url, params=std_params, headers=headers)
        except Exception as e:
            raise e

        if rsp.status_code != 200:
            raise Exception(f"request failed!,func_name: {call_func_name}, "
                            f"url: {api_url}, http_methods={http_methods}, "
                            f"params={params}, rsp_text={rsp.text}, "
                            f"rsp_status_code={rsp.status_code}")
        return rsp.json()

    def __init__(self, *args, **kwargs):
        if args and isinstance(args[0], dict):
            self.from_dict(args[0])
        else:
            self.from_args(*args, **kwargs)
