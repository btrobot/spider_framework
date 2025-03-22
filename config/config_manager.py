#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
配置管理器模块
负责加载和管理爬虫框架的配置
"""

import os
import yaml
import json
import logging
from typing import Dict, Any, Optional
from pathlib import Path


class ConfigManager:
    """配置管理器类，负责加载和管理爬虫框架的配置"""

    def __init__(self, config_path: Optional[str] = None):
        """
        初始化配置管理器
        
        Args:
            config_path: 配置文件路径，如果为None则使用默认配置
        """
        self.logger = logging.getLogger("ConfigManager")
        self.config: Dict[str, Any] = {}
        
        # 默认配置文件路径
        self.default_config_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "config",
            "default_config.yaml"
        )
        
        # 加载默认配置
        self._load_default_config()
        
        # 如果提供了自定义配置路径，加载自定义配置
        if config_path:
            self._load_custom_config(config_path)
            
        # 加载环境变量覆盖
        self._load_env_vars()
            
        self.logger.info("配置加载完成")

    def _load_default_config(self) -> None:
        """加载默认配置"""
        try:
            with open(self.default_config_path, 'r', encoding='utf-8') as f:
                self.config = yaml.safe_load(f)
            self.logger.debug(f"从 {self.default_config_path} 加载默认配置成功")
        except Exception as e:
            self.logger.error(f"加载默认配置失败: {str(e)}")
            raise

    def _load_custom_config(self, config_path: str) -> None:
        """
        加载自定义配置并覆盖默认配置
        
        Args:
            config_path: 自定义配置文件路径
        """
        try:
            if not os.path.exists(config_path):
                self.logger.warning(f"配置文件 {config_path} 不存在，使用默认配置")
                return
                
            _, ext = os.path.splitext(config_path)
            
            if ext.lower() in ['.yaml', '.yml']:
                with open(config_path, 'r', encoding='utf-8') as f:
                    custom_config = yaml.safe_load(f)
            elif ext.lower() == '.json':
                with open(config_path, 'r', encoding='utf-8') as f:
                    custom_config = json.load(f)
            else:
                self.logger.warning(f"不支持的配置文件格式: {ext}，使用默认配置")
                return
                
            # 递归更新配置
            self._update_config(self.config, custom_config)
            self.logger.debug(f"从 {config_path} 加载自定义配置成功")
        except Exception as e:
            self.logger.error(f"加载自定义配置失败: {str(e)}")
            raise

    def _update_config(self, default_config: Dict[str, Any], custom_config: Dict[str, Any]) -> None:
        """
        递归更新配置
        
        Args:
            default_config: 默认配置
            custom_config: 自定义配置
        """
        for key, value in custom_config.items():
            if key in default_config and isinstance(default_config[key], dict) and isinstance(value, dict):
                self._update_config(default_config[key], value)
            else:
                default_config[key] = value

    def _load_env_vars(self) -> None:
        """从环境变量加载配置覆盖"""
        # 检查是否有环境变量配置文件
        env_file = self.config.get('system', {}).get('env_file', '.env')
        if env_file and os.path.exists(env_file):
            try:
                from dotenv import load_dotenv
                load_dotenv(env_file)
                self.logger.debug(f"从 {env_file} 加载环境变量成功")
            except ImportError:
                self.logger.warning("python-dotenv 未安装，无法加载 .env 文件")
            except Exception as e:
                self.logger.error(f"加载环境变量文件失败: {str(e)}")
        
        # 遍历环境变量，寻找以 SPIDER_ 开头的环境变量
        for key, value in os.environ.items():
            if key.startswith("SPIDER_"):
                # 将环境变量名转换为配置键路径
                # 例如：SPIDER_SCHEDULER_MAX_WORKERS -> ['scheduler', 'max_workers']
                config_path = key[7:].lower().split('_')
                self._set_config_by_path(config_path, value)

    def _set_config_by_path(self, path: list, value: str) -> None:
        """
        根据路径设置配置值
        
        Args:
            path: 配置路径列表
            value: 配置值
        """
        try:
            # 转换值类型
            if value.lower() == 'true':
                value = True
            elif value.lower() == 'false':
                value = False
            elif value.isdigit():
                value = int(value)
            elif self._is_float(value):
                value = float(value)
                
            # 遍历路径设置值
            config = self.config
            for i, key in enumerate(path):
                if i == len(path) - 1:
                    config[key] = value
                else:
                    if key not in config:
                        config[key] = {}
                    config = config[key]
        except Exception as e:
            self.logger.error(f"从环境变量设置配置失败: {str(e)}")

    @staticmethod
    def _is_float(value: str) -> bool:
        """
        检查字符串是否为浮点数
        
        Args:
            value: 要检查的字符串
            
        Returns:
            是否为浮点数
        """
        try:
            float(value)
            return '.' in value
        except ValueError:
            return False

    def get(self, key: str, default: Any = None) -> Any:
        """
        获取配置项
        
        Args:
            key: 配置键，支持点号分隔的路径，如 'scheduler.max_workers'
            default: 默认值，如果配置不存在则返回此值
            
        Returns:
            配置值
        """
        keys = key.split('.')
        value = self.config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default

    def set(self, key: str, value: Any) -> None:
        """
        设置配置项
        
        Args:
            key: 配置键，支持点号分隔的路径，如 'scheduler.max_workers'
            value: 配置值
        """
        keys = key.split('.')
        config = self.config
        
        for i, k in enumerate(keys):
            if i == len(keys) - 1:
                config[k] = value
            else:
                if k not in config:
                    config[k] = {}
                config = config[k]
        
        self.logger.debug(f"设置配置: {key} = {value}")

    def dump(self, file_path: str) -> None:
        """
        将当前配置导出到文件
        
        Args:
            file_path: 文件路径
        """
        try:
            directory = os.path.dirname(file_path)
            if directory and not os.path.exists(directory):
                os.makedirs(directory, exist_ok=True)
                
            _, ext = os.path.splitext(file_path)
            
            if ext.lower() in ['.yaml', '.yml']:
                with open(file_path, 'w', encoding='utf-8') as f:
                    yaml.dump(self.config, f, default_flow_style=False, sort_keys=False)
            elif ext.lower() == '.json':
                with open(file_path, 'w', encoding='utf-8') as f:
                    json.dump(self.config, f, indent=2, ensure_ascii=False)
            else:
                raise ValueError(f"不支持的配置文件格式: {ext}")
                
            self.logger.info(f"配置已导出到 {file_path}")
        except Exception as e:
            self.logger.error(f"导出配置失败: {str(e)}")
            raise

    def get_all(self) -> Dict[str, Any]:
        """
        获取所有配置
        
        Returns:
            完整配置字典
        """
        return self.config.copy()


# 单例模式
_config_instance = None


def get_config(config_path: Optional[str] = None) -> ConfigManager:
    """
    获取配置管理器实例
    
    Args:
        config_path: 配置文件路径
        
    Returns:
        配置管理器实例
    """
    global _config_instance
    if _config_instance is None:
        _config_instance = ConfigManager(config_path)
    return _config_instance 