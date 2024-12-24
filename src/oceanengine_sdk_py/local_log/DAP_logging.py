import inspect
import time
import os
import sys
from typing import Callable, Optional, Any
from inspect import currentframe

import functools
from loguru import logger
from configobj import ConfigObj

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
DEFINE_CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'config.ini')


class DAPLogging(object):
    # 初始化判断
    __call_flag = False

    # __instance = None
    #
    # def __new__(cls, *args, **kwargs):
    #     if cls.__instance is None:
    #         cls.__instance = super().__new__(cls)
    #     return cls.__instance

    @staticmethod
    def __loguru_config_item_to_bool(item):
        if item is None:
            return False
        if item.lower() == 'true':
            return True
        else:
            return False

    def __init_loguru(self, cfg: ConfigObj) -> None:
        # 将字符串形式的配置属性转化为对应的loguru要求的格式
        # 默认日志名称
        if os.path.isdir(cfg['local_log']['log_path']):
            log_save_path = os.path.join(cfg['local_log']['log_path'], cfg['local_log']['define_log_name'])
        else:
            log_save_path = os.path.join(BASE_DIR, cfg['local_log']['log_path'], cfg['local_log']['define_log_name'])

            # 默认日志格式
            loguru_format = cfg['local_log']['loguru_format']

            # 默认日志等级
            if cfg['local_log'].get('loguru_logging_level') is None:
                logger_level = 'DEBUG'
            else:
                logger_level = cfg['local_log']['loguru_logging_level']

            # 默认队列情况
            loguru_enqueue = self.__loguru_config_item_to_bool(cfg['local_log'].get('loguru_enqueue'))
            # 是否向上追踪
            loguru_trace = self.__loguru_config_item_to_bool(cfg['local_log'].get('loguru_trace'))
            # 是否开启崩溃追踪
            loguru_catch = self.__loguru_config_item_to_bool(cfg['local_log'].get('loguru_catch'))
            # 日志轮转设定
            loguru_rotation = cfg['local_log'].get('loguru_rotation')
            # 日志压缩方式
            loguru_compression = cfg['local_log'].get('loguru_compression')
        # 清除loguru默认输出
        logger.remove()
        # 设置一路日志输出到日志文件
        logger.add(
            log_save_path + "_{time:YYYY-MM-DD_HH-mm}.local_log",
            format=loguru_format,
            level=logger_level,
            enqueue=loguru_enqueue,
            backtrace=loguru_trace,
            catch=loguru_catch,
            rotation=loguru_rotation,
            compression=loguru_compression,
            encoding="utf-8"
        )

        # 设置自定义的日志输出到控制台
        logger.add(
            sys.stdout,
            format=loguru_format,
            level=logger_level,
            enqueue=loguru_enqueue,
            backtrace=loguru_trace,
            catch=loguru_catch,
            # rotation=loguru_rotation,
            # compression=loguru_compression
        )

    def __init__(
            self,
            func: Optional[Callable] = None,
    ) -> None:
        if func is not None:
            self.func = func
        # 判断是否需要初始化，call_flag为true则直接跳过。
        if not self.__call_flag:
            # 判断是否有指定配置文件地址，没有则采用默认配置文件地址
            if getattr(self, 'config_path', None) is None:
                local_config_path = DEFINE_CONFIG_PATH
            else:
                local_config_path = self.config_path
            # 读取默认配置文件
            cfg = ConfigObj(local_config_path, encoding='utf-8', )
            # 初始化loguru
            self.__init_loguru(cfg)
            # 初始化DAP日志器的工作模式
            self.logger_level = cfg['local_log']['DAP_logging_module']
            # 标记init已经完成初始化，后续将跳过
            self.__call_flag = True

            self.__big_result_limit = int(cfg['local_log']['big_result_limit'])
            self.__big_args_limit = int(cfg['local_log']['big_args_limit'])

    def __call__(self, *args, **kwargs) -> Any:
        # 确保这里的functools.wraps装饰器正确接收了self.func作为参数
        functools.wraps(self.__call__)(self.func)

        frame = currentframe()
        start_time = time.time()
        formatted_start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
        ret = self.func(*args, **kwargs)
        func_name = self.func.__name__
        end_time = time.time()
        formatted_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

        call_by = frame.f_back.f_code.co_name
        call_file = frame.f_back.f_code.co_filename
        call_line = frame.f_back.f_lineno
        ret_size = self.__get_total_size(ret)

        if self.__big_result_limit != 0 and ret_size > self.__big_result_limit:
            result_limit = ret_size
        else:
            result_limit = ret

        if self.__big_args_limit != 0:
            args_limit, kwargs_limit = self.__replace_big_elements(*args, **kwargs)
        else:
            args_limit = args
            kwargs_limit = kwargs

        DAP_debug_format = f"call_by:{call_by},func_name:{func_name},start_time:{formatted_start_time},end_time:{formatted_end_time},cost_time:{round(end_time - start_time, 2)}s,args:{args_limit},kwargs:{kwargs_limit},result:{result_limit}--call_file:{call_file},call_line:{call_line}"
        DAP_logging_format = f"call_by:{call_by},func_name:{func_name},cost_time:{round(end_time - start_time, 2)}s,result:{result_limit}--call_file:{call_file},call_line:{call_line}"

        if self.logger_level == "debug":
            logger.info(DAP_debug_format)
        elif self.logger_level == "logging":
            logger.info(DAP_logging_format)
        else:
            logger.warning('未正确设置DAP日志器等级，DAP日志工作在默认Logging模式下')
            logger.warning(DAP_logging_format)
        return ret

    def __get__(self, instance, owner):
        @functools.wraps(self.func)
        def wapper(*args, **kwargs):
            frame = currentframe()
            start_time = time.time()
            formatted_start_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(start_time))
            ret = self.func(instance, *args, **kwargs)
            end_time = time.time()
            formatted_end_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            func_name = self.func.__name__
            call_by = frame.f_back.f_code.co_name
            call_file = frame.f_back.f_code.co_filename
            call_line = frame.f_back.f_lineno
            ret_size = self.__get_total_size(ret)

            if self.__big_result_limit != 0 and ret_size > self.__big_result_limit:
                result_limit = ret_size
            else:
                result_limit = ret

            if self.__big_args_limit != 0:
                args_limit, kwargs_limit = self.__replace_big_elements(*args, **kwargs)
            else:
                args_limit = args
                kwargs_limit = kwargs

            DAP_debug_format = f"call_by:{call_by},func_name:{func_name},start_time:{formatted_start_time},end_time:{formatted_end_time},cost_time:{round(end_time - start_time, 2)}s,args:{args_limit},kwargs:{kwargs_limit},result:{result_limit}--call_file:{call_file},call_line:{call_line}"
            DAP_logging_format = f"call_by:{call_by},func_name:{func_name},cost_time:{round(end_time - start_time, 2)}s,result:{result_limit}--call_file:{call_file},call_line:{call_line}"

            if self.logger_level == "debug":
                logger.info(DAP_debug_format)
            elif self.logger_level == "logging":
                logger.info(DAP_logging_format)
            else:
                logger.warning('未正确设置DAP日志器等级，DAP日志工作在默认Logging模式下')
                logger.warning(DAP_logging_format)
            return ret

        return wapper

    @staticmethod
    def trace(log_message, *args, **kwargs):
        logger.trace(log_message, args, kwargs)

    @staticmethod
    def debug(log_message, *args, **kwargs):
        logger.debug(log_message, args, kwargs)

    @staticmethod
    def info(log_message, *args, **kwargs):
        logger.info(log_message, args, kwargs)

    @staticmethod
    def success(log_message, *args, **kwargs):
        logger.success(log_message, args, kwargs)

    @staticmethod
    def warning(log_message, *args, **kwargs):
        logger.warning(log_message, args, kwargs)

    @staticmethod
    def error(log_message, *args, **kwargs):
        logger.error(log_message, args, kwargs)

    @staticmethod
    def critical(log_message, *args, **kwargs):
        logger.critical(log_message, args, kwargs)

    def __get_total_size(self, obj, seen=None):
        """Recursively finds size of objects"""
        size = sys.getsizeof(obj)
        if seen is None:
            seen = set()
        obj_id = id(obj)
        if obj_id in seen:
            return 0
        # Mark as seen
        seen.add(obj_id)
        if isinstance(obj, dict):
            size += sum([self.__get_total_size(v, seen) for v in obj.values()])
            size += sum([self.__get_total_size(k, seen) for k in obj.keys()])
        elif hasattr(obj, '__dict__'):
            size += self.__get_total_size(obj.__dict__, seen)
        elif hasattr(obj, '__iter__') and not isinstance(obj, (str, bytes, bytearray)):
            size += sum([self.__get_total_size(i, seen) for i in obj])
        return size

    def __replace_big_elements(self, *args, **kwargs):
        new_args = []
        for arg in args:
            size = self.__get_total_size(arg)
            if size > self.__big_args_limit:
                new_args.append(f'big_args_limit:{size}')
            else:
                new_args.append(arg)

        new_kwargs = {}
        for key, value in kwargs.items():
            size = self.__get_total_size(value)
            if size > self.__big_args_limit:
                new_kwargs[key] = f'big_kwargs_limit:{size}'
            else:
                new_kwargs[key] = value

        return tuple(new_args), new_kwargs
