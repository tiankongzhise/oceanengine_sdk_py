# -*- coding: utf-8 -*-
import re
from typing import Optional
def remove_leading_digits_and_underscore(s):
    # 使用正则表达式匹配以数字开头并跟随下划线的字符串
    match = re.match(r'^((\d+)_*)*', s)
    if match:
        del_char = match.group(0)  # 获取匹配的部分
        remaining_str = s[len(del_char):]  # 去除匹配的部分
        return remaining_str, del_char
    else:
        return s, ''  # 如果没有匹配的部分，返回原字符串和空字符串
def create_method(api_url):
    open_api_url_prefix = 'https://api.oceanengine.com/open_api/'
    ad_api_url_prefix = 'https://ad.oceanengine.com/open_api/'


    if open_api_url_prefix in api_url:
        replace_special_chars = api_url.replace(open_api_url_prefix,'').replace('_','__').replace('.','___').replace('/','_')[:-1]
    elif ad_api_url_prefix in api_url:
        replace_special_chars = api_url.replace(ad_api_url_prefix,'').replace('_','__').replace('.','___').replace('/','_')[:-1]+'_adApi'
    else:
        raise ValueError('Invalid API URL')

    print(replace_special_chars)

    replace_first_int,del_char = remove_leading_digits_and_underscore(replace_special_chars)

    func_name = replace_first_int

    func_method = 'get'

    print(func_name)
    create_method_tempate= '''def {func_name}(self, params: Mapping | None = None):
        func_name = inspect.currentframe().f_code.co_name
        func_name = func_name
        http_method = '{func_method}'
        return self.make_request(func_name, params, http_method)
    '''

    result = create_method_tempate.format(func_name=func_name,func_method=func_method)
    print(result)

    if del_char:
        print('in')
        result = result.replace("func_name = func_name",f'func_name = "{del_char}" + func_name ')




    print(result)
    print('\n')
    print(replace_first_int,del_char)

if __name__ == '__main__':
    api_url = 'https://ad.oceanengine.com/open_api/2/agent/info/'
    create_method(api_url)

