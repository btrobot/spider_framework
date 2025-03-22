#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志管理器模块
负责配置和管理爬虫框架的日志系统
"""

import os
import sys
import logging
import traceback
from typing import Optional, Dict, Any, Union
from pathlib import Path

from loguru import logger


class LogFormatter:
    """日志格式化器，用于格式化日志输出"""
    
    @staticmethod
    def format(record: Dict[str, Any]) -> str:
        """
        格式化日志记录
        
        Args:
            record: 日志记录
            
        Returns:
            格式化后的日志字符串
        """
        if record["level"].name == "ERROR" or record["level"].name == "CRITICAL":
            log_format = "<red>{time:YYYY-MM-DD HH:mm:ss.SSS}</red> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
        else:
            log_format = "{time:YYYY-MM-DD HH:mm:ss.SSS} | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
            
        return log_format


class LoguruHandler(logging.Handler):
    """Loguru Handler for standard logging compatibility"""
    
    def emit(self, record: logging.LogRecord) -> None:
        """
        输出日志记录
        
        Args:
            record: 日志记录
        """
        # 获取对应的loguru级别
        level = logger.level(record.levelname).name
        
        # 提取堆栈信息
        frame = sys._getframe(6)
        depth = 6
        
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1
            
        # 输出日志
        logger.opt(depth=depth, exception=record.exc_info).log(
            level, record.getMessage()
        )


class LogManager:
    """日志管理器，负责配置和管理爬虫框架的日志系统"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化日志管理器
        
        Args:
            config: 日志配置
        """
        self.config = config or {}
        self._configure_loguru()
        self._configure_standard_logging()
    
    def _configure_loguru(self) -> None:
        """配置loguru日志系统"""
        # 移除默认处理器
        logger.remove()
        
        # 获取日志配置
        log_level = self.config.get("level", "INFO")
        log_format = self.config.get("format", "{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
        log_file = self.config.get("file", "./logs/spider.log")
        rotation = self.config.get("rotation", "500 MB")
        retention = self.config.get("retention", "10 days")
        compression = self.config.get("compression", "zip")
        
        # 确保日志目录存在
        if log_file:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir, exist_ok=True)
        
        # 添加控制台处理器
        logger.add(
            sys.stderr,
            level=log_level,
            format=log_format,
            colorize=True,
            backtrace=True,
            diagnose=True,
        )
        
        # 添加文件处理器
        if log_file:
            logger.add(
                log_file,
                level=log_level,
                format=log_format,
                rotation=rotation,
                retention=retention,
                compression=compression,
                encoding="utf-8",
                backtrace=True,
                diagnose=True,
            )
    
    def _configure_standard_logging(self) -> None:
        """配置标准日志系统以兼容第三方库"""
        # 移除所有现有处理器
        for handler in logging.root.handlers[:]:
            logging.root.removeHandler(handler)
        
        # 设置日志级别
        log_level = self.config.get("level", "INFO")
        logging.root.setLevel(getattr(logging, log_level))
        
        # 添加loguru处理器
        handler = LoguruHandler()
        logging.root.addHandler(handler)
    
    @staticmethod
    def get_logger(name: str = None) -> logger:
        """
        获取logger实例
        
        Args:
            name: 日志器名称
            
        Returns:
            logger实例
        """
        if name:
            return logger.bind(name=name)
        return logger
    
    @staticmethod
    def exception_handler(exc_type, exc_value, exc_traceback):
        """
        全局异常处理器
        
        Args:
            exc_type: 异常类型
            exc_value: 异常值
            exc_traceback: 异常堆栈
        """
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
            return
            
        logger.opt(exception=(exc_type, exc_value, exc_traceback)).critical("未捕获的异常:")


# 设置全局异常处理器
sys.excepthook = LogManager.exception_handler


# 单例模式
_log_manager_instance = None


def init_logging(config: Dict[str, Any] = None) -> LogManager:
    """
    初始化日志系统
    
    Args:
        config: 日志配置
        
    Returns:
        日志管理器实例
    """
    global _log_manager_instance
    if _log_manager_instance is None:
        _log_manager_instance = LogManager(config)
    return _log_manager_instance


def get_logger(name: str = None) -> logger:
    """
    获取logger实例
    
    Args:
        name: 日志器名称
        
    Returns:
        logger实例
    """
    if _log_manager_instance is None:
        init_logging()
    return LogManager.get_logger(name) 