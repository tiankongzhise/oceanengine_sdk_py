# -*- coding: utf-8 -*-
import datetime
from collections.abc import Mapping

from .service.oauth2 import Oauth2
from .service.account_service import AccountService
from .service.report_service import ReportService
from .service.ad_service_plus import AdServicePlus




class OceanengineSdkClient(Oauth2, AccountService, ReportService, AdServicePlus):

    # 自实现的客户端实用功能，比如保持登录状态
    def __update_auth_info(self, rsp):
        self.access_token = rsp['accessToken']
        self.refresh_token = rsp['refreshToken']
        self.expires_time = rsp['expiresTime']
        self.refresh_expires_time = rsp['refreshExpiresTime']

    @staticmethod
    def __process_auth_response(rsp):
        """
        处理 API 响应，提取 access_token 和 refresh_token 并计算它们的过期时间。

        :param rsp: API 响应对象 (requests.Response)
        :return: 包含 access_token、refresh_token 及其过期时间的字典
        """
        rsp_data = rsp
        current_time = datetime.datetime.now()
        if rsp_data['code'] != 0:
            raise Exception(rsp_data['message'])

        return_data = {'accessToken': rsp_data['data']['access_token'],
                       'refreshToken': rsp_data['data']['refresh_token'],
                       'expiresTime': current_time + datetime.timedelta(seconds=rsp_data['data'].get('expires_in', 0)),
                       'refreshExpiresTime': current_time + datetime.timedelta(
                           seconds=rsp_data['data'].get('refresh_token_expires_in', 0))}
        return return_data

    def oauth_sign(self, params: Mapping | None = None) -> Mapping[str, str | int]:
        if params is not None:
            self.from_dict(params)

        if hasattr(self, 'access_token') and self.access_token is not None:

            if self.expires_time > datetime.datetime.now() + datetime.timedelta(hours=3):
                return vars(self)

            else:
                query_param = {
                    "app_id": self.app_id,
                    "secret": self.secret_key,
                    "refresh_token": self.refresh_token
                }
                rsp = self.oauth2_refresh__token(query_param)
                rsp = self.__process_auth_response(rsp)
                self.__update_auth_info(rsp)
                return vars(self)
        else:
            query_param = {
                "app_id": self.app_id,
                "secret": self.secret_key,
                "auth_code": self.auth_code
            }
            rsp = self.oauth2_access__token_adApi(query_param)
            rsp = self.__process_auth_response(rsp)
            self.__update_auth_info(rsp)
            return vars(self)
