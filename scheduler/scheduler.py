#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调度器模块
负责任务的调度、分配和管理
"""

import os
import time
import json
import uuid
import signal
import logging
import threading
import multiprocessing as mp
from typing import Dict, List, Set, Any, Optional, Callable, Union, Tuple
from queue import PriorityQueue, Empty
from collections import defaultdict

from spider_framework.utils.logger import get_logger
from spider_framework.utils.exceptions import SchedulerError, QueueError, TaskError
from spider_framework.scheduler.task import Task, TaskStatus, TaskPriority, TaskResult


logger = get_logger("Scheduler")


class Scheduler:
    """调度器，负责任务的调度、分配和管理"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化调度器
        
        Args:
            config: 调度器配置
        """
        self.config = config or {}
        
        # 任务队列 (优先级, 创建时间, 任务ID), 任务
        self.task_queue = PriorityQueue()
        
        # 任务映射表，用于快速查找任务
        self.tasks: Dict[str, Task] = {}
        
        # 正在执行的任务
        self.running_tasks: Dict[str, Task] = {}
        
        # 工作进程心跳
        self.worker_heartbeats: Dict[str, float] = {}
        
        # 工作进程状态
        self.worker_status: Dict[str, Dict[str, Any]] = {}
        
        # 工作进程集合
        self.workers: Set[mp.Process] = set()
        
        # 任务完成回调
        self.task_callbacks: Dict[str, List[Callable[[Task], None]]] = defaultdict(list)
        
        # 内部共享变量
        self._stop_event = threading.Event()
        self._monitor_thread = None
        self._heartbeat_thread = None
        
        # 读取配置
        self.max_workers = self.config.get("max_workers", 4)
        self.check_interval = self.config.get("check_interval", 5)
        self.heartbeat_timeout = self.config.get("heartbeat_timeout", 30)
        self.retry_count = self.config.get("retry_count", 3)
        self.retry_interval = self.config.get("retry_interval", 10)
        
        # 任务队列文件路径
        self.queue_file = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "data",
            "task_queue.json"
        )
        
        # 确保数据目录存在
        os.makedirs(os.path.dirname(self.queue_file), exist_ok=True)
        
        # 加载持久化的任务队列
        self._load_queue()
        
    def start(self) -> None:
        """启动调度器"""
        logger.info("启动调度器")
        
        # 启动监控线程
        self._monitor_thread = threading.Thread(target=self._monitor_workers, daemon=True)
        self._monitor_thread.start()
        
        # 启动心跳检查线程
        self._heartbeat_thread = threading.Thread(target=self._check_heartbeats, daemon=True)
        self._heartbeat_thread.start()
        
        # 启动工作进程
        self._start_workers()
        
        logger.info(f"调度器已启动，工作进程数: {self.max_workers}")
        
    def stop(self) -> None:
        """停止调度器"""
        if self._stop_event.is_set():
            return
            
        logger.info("停止调度器")
        self._stop_event.set()
        
        # 等待监控线程结束
        if self._monitor_thread and self._monitor_thread.is_alive():
            self._monitor_thread.join(timeout=10)
            
        # 等待心跳线程结束
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            self._heartbeat_thread.join(timeout=10)
            
        # 结束所有工作进程
        for worker in self.workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
                
        self.workers.clear()
        
        # 保存任务队列
        self._save_queue()
        
        logger.info("调度器已停止")
        
    def add_task(self, task: Task) -> str:
        """
        添加任务到队列
        
        Args:
            task: 任务对象
            
        Returns:
            任务ID
        """
        # 如果任务已存在，抛出异常
        if task.task_id in self.tasks:
            raise TaskError(f"任务已存在: {task.task_id}", task_id=task.task_id)
            
        # 添加到任务映射表
        self.tasks[task.task_id] = task
        
        # 添加到优先级队列
        priority_tuple = (
            -task.priority.value,  # 负值，使高优先级在队列前面
            task.created_at,       # 创建时间
            task.task_id           # 任务ID
        )
        self.task_queue.put((priority_tuple, task))
        
        logger.info(f"添加任务: {task.task_id}, 优先级: {task.priority.name}, URL: {task.url}")
        return task.task_id
        
    def add_tasks(self, tasks: List[Task]) -> List[str]:
        """
        批量添加任务
        
        Args:
            tasks: 任务列表
            
        Returns:
            任务ID列表
        """
        task_ids = []
        for task in tasks:
            task_id = self.add_task(task)
            task_ids.append(task_id)
        return task_ids
        
    def get_task(self, task_id: str) -> Optional[Task]:
        """
        获取任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            任务对象，如果不存在则返回None
        """
        return self.tasks.get(task_id)
        
    def cancel_task(self, task_id: str) -> bool:
        """
        取消任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            是否成功取消
        """
        task = self.tasks.get(task_id)
        if not task:
            logger.warning(f"尝试取消不存在的任务: {task_id}")
            return False
            
        # 如果任务已经完成，无法取消
        if task.is_complete():
            logger.warning(f"无法取消已完成的任务: {task_id}, 状态: {task.status.name}")
            return False
            
        # 如果任务正在执行，标记为取消
        if task.status == TaskStatus.RUNNING:
            # 这里只标记状态，工作进程会检查并处理
            task.cancel()
            logger.info(f"标记正在执行的任务为取消: {task_id}")
            return True
            
        # 如果任务在队列中，需要重建队列（Python的PriorityQueue没有直接的移除方法）
        if task.status == TaskStatus.PENDING or task.status == TaskStatus.RETRY:
            # 标记为取消
            task.cancel()
            logger.info(f"取消排队中的任务: {task_id}")
            # 队列会在下次获取任务时跳过此任务
            return True
            
        return False
        
    def get_next_task(self) -> Optional[Task]:
        """
        获取下一个要执行的任务
        
        Returns:
            任务对象，如果队列为空则返回None
        """
        try:
            while not self.task_queue.empty():
                # 获取任务，但不从队列中移除
                _, task = self.task_queue.get()
                
                # 如果任务已取消或已完成，跳过
                if task.is_complete():
                    continue
                    
                # 如果是重试任务，检查是否到达重试时间
                if task.status == TaskStatus.RETRY:
                    # 计算重试时间
                    retry_time = task.finished_at + self.retry_interval
                    if time.time() < retry_time:
                        # 重新放入队列
                        priority_tuple = (
                            -task.priority.value,
                            task.created_at,
                            task.task_id
                        )
                        self.task_queue.put((priority_tuple, task))
                        continue
                    
                    # 重置为等待状态
                    task.status = TaskStatus.PENDING
                
                # 返回有效任务
                return task
                
            return None
        except Empty:
            return None
        except Exception as e:
            logger.error(f"获取下一个任务失败: {str(e)}")
            return None
            
    def register_worker(self, worker_id: str, status: Dict[str, Any] = None) -> None:
        """
        注册工作进程
        
        Args:
            worker_id: 工作进程ID
            status: 工作进程状态
        """
        self.worker_heartbeats[worker_id] = time.time()
        if status:
            self.worker_status[worker_id] = status
        logger.debug(f"工作进程注册: {worker_id}")
        
    def update_worker_heartbeat(self, worker_id: str, status: Dict[str, Any] = None) -> None:
        """
        更新工作进程心跳
        
        Args:
            worker_id: 工作进程ID
            status: 工作进程状态
        """
        self.worker_heartbeats[worker_id] = time.time()
        if status:
            self.worker_status[worker_id] = status
            
    def update_task_status(self, task_id: str, status: TaskStatus, 
                          result: TaskResult = None, worker_id: str = None) -> None:
        """
        更新任务状态
        
        Args:
            task_id: 任务ID
            status: 新状态
            result: 任务结果
            worker_id: 工作进程ID
        """
        task = self.tasks.get(task_id)
        if not task:
            logger.warning(f"尝试更新不存在的任务状态: {task_id}")
            return
            
        old_status = task.status
        task.status = status
        
        # 如果提供了结果，则更新
        if result:
            task.result = result
            task.finished_at = time.time()
            
        # 如果提供了工作进程ID，则更新
        if worker_id:
            task.worker_id = worker_id
            
        # 如果任务已完成，从运行中任务列表移除
        if task.is_complete() and task_id in self.running_tasks:
            del self.running_tasks[task_id]
            
        # 如果任务需要重试，重新加入队列
        if status == TaskStatus.RETRY:
            priority_tuple = (
                -task.priority.value,
                task.created_at,
                task.task_id
            )
            self.task_queue.put((priority_tuple, task))
            
        logger.info(f"任务 {task_id} 状态从 {old_status.name} 更新为 {status.name}")
        
        # 触发任务完成回调
        if task.is_complete() and task_id in self.task_callbacks:
            for callback in self.task_callbacks[task_id]:
                try:
                    callback(task)
                except Exception as e:
                    logger.error(f"任务回调执行失败: {str(e)}")
            
            # 移除回调
            del self.task_callbacks[task_id]
            
    def add_task_callback(self, task_id: str, callback: Callable[[Task], None]) -> None:
        """
        添加任务完成回调
        
        Args:
            task_id: 任务ID
            callback: 回调函数
        """
        if task_id in self.tasks:
            self.task_callbacks[task_id].append(callback)
            
            # 如果任务已完成，立即执行回调
            task = self.tasks[task_id]
            if task.is_complete():
                try:
                    callback(task)
                except Exception as e:
                    logger.error(f"任务回调执行失败: {str(e)}")
                # 移除回调
                self.task_callbacks[task_id].remove(callback)
        else:
            logger.warning(f"尝试为不存在的任务添加回调: {task_id}")
            
    def get_stats(self) -> Dict[str, Any]:
        """
        获取调度器统计信息
        
        Returns:
            统计信息
        """
        # 计算任务状态分布
        status_counts = defaultdict(int)
        for task in self.tasks.values():
            status_counts[task.status.name] += 1
            
        # 计算优先级分布
        priority_counts = defaultdict(int)
        for task in self.tasks.values():
            priority_counts[task.priority.name] += 1
            
        return {
            "total_tasks": len(self.tasks),
            "queue_size": self.task_queue.qsize(),
            "running_tasks": len(self.running_tasks),
            "workers": len(self.worker_heartbeats),
            "status_distribution": dict(status_counts),
            "priority_distribution": dict(priority_counts),
        }
        
    def _start_workers(self) -> None:
        """启动工作进程"""
        from spider_framework.worker.worker_process import worker_process_main
        
        for _ in range(self.max_workers):
            # 创建工作进程
            worker = mp.Process(
                target=worker_process_main,
                args=(self.config,),
                daemon=True
            )
            worker.start()
            self.workers.add(worker)
            logger.debug(f"启动工作进程: {worker.pid}")
            
    def _monitor_workers(self) -> None:
        """监控工作进程"""
        while not self._stop_event.is_set():
            # 检查工作进程是否存活
            for worker in list(self.workers):
                if not worker.is_alive():
                    logger.warning(f"工作进程已终止: {worker.pid}")
                    self.workers.remove(worker)
                    
                    # 如果调度器仍在运行，启动新工作进程
                    if not self._stop_event.is_set() and len(self.workers) < self.max_workers:
                        self._start_workers()
                        
            # 休眠
            time.sleep(self.check_interval)
            
    def _check_heartbeats(self) -> None:
        """检查工作进程心跳"""
        while not self._stop_event.is_set():
            current_time = time.time()
            
            # 检查每个工作进程的心跳
            for worker_id, last_heartbeat in list(self.worker_heartbeats.items()):
                # 如果工作进程心跳超时
                if current_time - last_heartbeat > self.heartbeat_timeout:
                    logger.warning(f"工作进程心跳超时: {worker_id}")
                    
                    # 查找该工作进程的任务
                    for task_id, task in list(self.running_tasks.items()):
                        if task.worker_id == worker_id:
                            # 将任务标记为重试
                            logger.warning(f"心跳超时，将任务 {task_id} 标记为重试")
                            
                            # 创建错误结果
                            result = TaskResult(
                                success=False,
                                error="工作进程心跳超时",
                                execution_time=current_time - (task.started_at or task.created_at)
                            )
                            
                            # 更新任务状态
                            self.update_task_status(task_id, TaskStatus.RETRY, result)
                            
                    # 移除工作进程记录
                    del self.worker_heartbeats[worker_id]
                    if worker_id in self.worker_status:
                        del self.worker_status[worker_id]
                        
            # 休眠
            time.sleep(self.check_interval)
            
    def _load_queue(self) -> None:
        """从文件加载任务队列"""
        if not os.path.exists(self.queue_file):
            logger.debug("任务队列文件不存在，使用空队列")
            return
            
        try:
            with open(self.queue_file, 'r', encoding='utf-8') as f:
                tasks_data = json.load(f)
                
            # 重新创建任务对象
            for task_data in tasks_data:
                task = Task.from_dict(task_data)
                
                # 跳过已完成的任务
                if task.is_complete():
                    continue
                    
                # 重置运行中的任务为等待状态
                if task.status == TaskStatus.RUNNING:
                    task.status = TaskStatus.PENDING
                    
                # 添加到任务映射表
                self.tasks[task.task_id] = task
                
                # 添加到优先级队列
                priority_tuple = (
                    -task.priority.value,
                    task.created_at,
                    task.task_id
                )
                self.task_queue.put((priority_tuple, task))
                
            logger.info(f"从文件加载了 {len(self.tasks)} 个任务")
        except Exception as e:
            logger.error(f"加载任务队列失败: {str(e)}")
            
    def _save_queue(self) -> None:
        """将任务队列保存到文件"""
        try:
            # 将任务转换为字典
            tasks_data = [task.to_dict() for task in self.tasks.values()]
            
            # 确保目录存在
            os.makedirs(os.path.dirname(self.queue_file), exist_ok=True)
            
            # 写入文件
            with open(self.queue_file, 'w', encoding='utf-8') as f:
                json.dump(tasks_data, f, ensure_ascii=False, indent=2)
                
            logger.info(f"保存了 {len(tasks_data)} 个任务到文件")
        except Exception as e:
            logger.error(f"保存任务队列失败: {str(e)}")
            
    def _signal_handler(self, sig, frame) -> None:
        """信号处理程序"""
        logger.info(f"接收到信号: {sig}，准备关闭")
        self.stop()


# 单例模式
_scheduler_instance = None


def get_scheduler(config: Dict[str, Any] = None) -> Scheduler:
    """
    获取调度器实例
    
    Args:
        config: 调度器配置
        
    Returns:
        调度器实例
    """
    global _scheduler_instance
    # 使用线程锁确保单例安全性
    with threading.Lock():
        if _scheduler_instance is None:
            _scheduler_instance = Scheduler(config)
    return _scheduler_instance 