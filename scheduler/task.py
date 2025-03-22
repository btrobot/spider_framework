#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
任务模块
定义爬虫任务的数据结构
"""

import uuid
import time
import json
from typing import Dict, Any, List, Optional, Union
from enum import Enum, auto
from dataclasses import dataclass, field, asdict


class TaskStatus(Enum):
    """任务状态枚举"""
    PENDING = auto()     # 等待执行
    RUNNING = auto()     # 正在执行
    SUCCESS = auto()     # 执行成功
    FAILED = auto()      # 执行失败
    RETRY = auto()       # 重试
    CANCELLED = auto()   # 取消


class TaskPriority(Enum):
    """任务优先级枚举"""
    LOW = 0
    NORMAL = 1
    HIGH = 2
    CRITICAL = 3


@dataclass
class TaskResult:
    """任务结果数据类"""
    success: bool = False                    # 是否成功
    data: Optional[Dict[str, Any]] = None    # 结果数据
    error: Optional[str] = None              # 错误信息
    status_code: Optional[int] = None        # 状态码
    execution_time: float = 0.0              # 执行时间（秒）
    screenshot_path: Optional[str] = None    # 截图路径

    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典
        
        Returns:
            字典表示
        """
        return asdict(self)

    def to_json(self) -> str:
        """
        转换为JSON字符串
        
        Returns:
            JSON字符串
        """
        return json.dumps(self.to_dict(), ensure_ascii=False)


@dataclass
class Task:
    """爬虫任务数据类"""
    url: str                                         # 爬取URL
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))  # 任务ID
    name: str = ""                                   # 任务名称
    priority: TaskPriority = TaskPriority.NORMAL     # 优先级
    status: TaskStatus = TaskStatus.PENDING          # 状态
    retry_count: int = 0                             # 重试次数
    max_retries: int = 3                             # 最大重试次数
    timeout: int = 60                                # 超时时间（秒）
    created_at: float = field(default_factory=time.time)  # 创建时间
    started_at: Optional[float] = None               # 开始时间
    finished_at: Optional[float] = None              # 完成时间
    worker_id: Optional[str] = None                  # 执行工作进程ID
    parent_id: Optional[str] = None                  # 父任务ID
    result: Optional[TaskResult] = None              # 任务结果
    headers: Dict[str, str] = field(default_factory=dict)  # 请求头
    cookies: Dict[str, str] = field(default_factory=dict)  # Cookie
    metadata: Dict[str, Any] = field(default_factory=dict)  # 元数据
    action_rules: List[Dict[str, Any]] = field(default_factory=list)  # 页面操作规则
    extraction_rules: List[Dict[str, Any]] = field(default_factory=list)  # 数据提取规则
    
    def to_dict(self) -> Dict[str, Any]:
        """
        转换为字典
        
        Returns:
            字典表示
        """
        task_dict = asdict(self)
        # 将枚举值转换为字符串
        task_dict["priority"] = self.priority.name
        task_dict["status"] = self.status.name
        return task_dict
        
    def to_json(self) -> str:
        """
        转换为JSON字符串
        
        Returns:
            JSON字符串
        """
        return json.dumps(self.to_dict(), ensure_ascii=False)
        
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Task':
        """
        从字典创建任务
        
        Args:
            data: 任务字典
            
        Returns:
            任务实例
        """
        # 复制字典，避免修改原始数据
        task_data = data.copy()
        
        # 转换枚举值
        if "priority" in task_data and isinstance(task_data["priority"], str):
            task_data["priority"] = TaskPriority[task_data["priority"]]
            
        if "status" in task_data and isinstance(task_data["status"], str):
            task_data["status"] = TaskStatus[task_data["status"]]
            
        # 处理结果字段
        if "result" in task_data and task_data["result"] is not None:
            task_data["result"] = TaskResult(**task_data["result"])
            
        return cls(**task_data)
        
    @classmethod
    def from_json(cls, json_str: str) -> 'Task':
        """
        从JSON字符串创建任务
        
        Args:
            json_str: JSON字符串
            
        Returns:
            任务实例
        """
        data = json.loads(json_str)
        return cls.from_dict(data)
        
    def start(self, worker_id: str) -> None:
        """
        标记任务开始
        
        Args:
            worker_id: 工作进程ID
        """
        self.status = TaskStatus.RUNNING
        self.started_at = time.time()
        self.worker_id = worker_id
        
    def complete(self, result: TaskResult) -> None:
        """
        标记任务完成
        
        Args:
            result: 任务结果
        """
        self.result = result
        self.finished_at = time.time()
        
        if result.success:
            self.status = TaskStatus.SUCCESS
        else:
            if self.retry_count < self.max_retries:
                self.status = TaskStatus.RETRY
                self.retry_count += 1
            else:
                self.status = TaskStatus.FAILED
                
    def cancel(self) -> None:
        """取消任务"""
        self.status = TaskStatus.CANCELLED
        self.finished_at = time.time()
        
    def is_complete(self) -> bool:
        """
        检查任务是否已完成
        
        Returns:
            是否已完成
        """
        return self.status in (TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED)
        
    def get_execution_time(self) -> float:
        """
        获取任务执行时间
        
        Returns:
            执行时间（秒）
        """
        if self.started_at is None:
            return 0
            
        end_time = self.finished_at or time.time()
        return end_time - self.started_at 