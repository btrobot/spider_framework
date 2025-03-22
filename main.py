#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
爬虫框架入口
负责初始化和启动框架
"""

import os
import sys
import time
import argparse
import json
import signal
import threading
from typing import Dict, Any, List, Optional

from spider_framework.utils.logger import get_logger, init_logging
from spider_framework.config.config_manager import get_config
from spider_framework.scheduler.scheduler import get_scheduler
from spider_framework.scheduler.task import Task, TaskPriority


logger = get_logger("Main")

# 信号接收计数器
signal_count = 0
MAX_SIGNALS_BEFORE_FORCE_EXIT = 2


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="爬虫框架")
    
    parser.add_argument(
        "-c", "--config",
        help="配置文件路径",
        type=str,
        default=None
    )
    
    parser.add_argument(
        "-u", "--url",
        help="要爬取的URL",
        type=str,
        default=None
    )
    
    parser.add_argument(
        "-t", "--tasks",
        help="任务文件路径（JSON格式）",
        type=str,
        default=None
    )
    
    parser.add_argument(
        "-w", "--workers",
        help="工作进程数量",
        type=int,
        default=None
    )
    
    parser.add_argument(
        "--headless",
        help="是否使用无头模式",
        action="store_true",
        default=None
    )
    
    parser.add_argument(
        "--exit",
        help="关闭所有正在运行的爬虫进程",
        action="store_true"
    )
    
    return parser.parse_args()


def load_tasks(task_file: str) -> List[Dict[str, Any]]:
    """
    从文件加载任务
    
    Args:
        task_file: 任务文件路径
        
    Returns:
        任务列表
    """
    try:
        with open(task_file, 'r', encoding='utf-8') as f:
            tasks_data = json.load(f)
            
        if isinstance(tasks_data, list):
            return tasks_data
        elif isinstance(tasks_data, dict):
            return [tasks_data]
        else:
            logger.error(f"无效的任务文件格式: {task_file}")
            return []
    except Exception as e:
        logger.error(f"加载任务文件失败: {str(e)}")
        return []


def create_tasks(tasks_data: List[Dict[str, Any]]) -> List[Task]:
    """
    创建任务对象
    
    Args:
        tasks_data: 任务数据列表
        
    Returns:
        任务对象列表
    """
    tasks = []
    
    for task_data in tasks_data:
        try:
            # 确保URL存在
            if "url" not in task_data:
                logger.error(f"任务缺少URL字段: {task_data}")
                continue
                
            # 处理优先级
            if "priority" in task_data and isinstance(task_data["priority"], str):
                try:
                    task_data["priority"] = TaskPriority[task_data["priority"]]
                except KeyError:
                    logger.warning(f"无效的优先级值: {task_data['priority']}，使用默认值NORMAL")
                    task_data["priority"] = TaskPriority.NORMAL
                    
            # 创建任务
            task = Task(**task_data)
            tasks.append(task)
            
        except Exception as e:
            logger.error(f"创建任务失败: {str(e)}")
            
    return tasks


def signal_handler(sig, frame):
    """信号处理函数"""
    global signal_count
    signal_count += 1
    
    logger.info(f"接收到信号: {sig}，准备关闭 (第 {signal_count} 次)")
    
    # 如果多次接收到信号，直接强制退出
    if signal_count > MAX_SIGNALS_BEFORE_FORCE_EXIT:
        logger.warning(f"多次接收到信号，强制退出")
        os._exit(1)
    
    try:
        # 获取调度器并停止
        scheduler = get_scheduler()
        scheduler.stop()
    except Exception as e:
        logger.error(f"关闭调度器时出错: {str(e)}")
    
    # 强制退出程序，确保不会卡住
    os._exit(0)


def emergency_exit(sig, frame):
    """紧急退出处理函数，用于处理Windows下的Ctrl+Break信号"""
    logger.warning(f"接收到紧急退出信号: {sig}，立即退出")
    os._exit(1)


def main():
    """主函数"""
    # 解析命令行参数
    args = parse_args()
    
    # 如果指定了--exit，尝试关闭所有爬虫进程
    if args.exit:
        try:
            import psutil
            killed = 0
            # 查找所有Python进程
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    # 检查命令行是否包含spider_framework
                    cmdline = proc.info['cmdline']
                    if cmdline and 'python' in cmdline[0].lower() and any('spider_framework' in arg for arg in cmdline if arg):
                        print(f"正在结束爬虫进程: {proc.info['pid']}")
                        proc.terminate()
                        killed += 1
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
            print(f"已结束 {killed} 个爬虫进程")
        except Exception as e:
            print(f"结束进程时出错: {str(e)}")
        return

    # 加载配置
    config = get_config(args.config)
    
    # 如果命令行指定了工作进程数，覆盖配置
    if args.workers is not None:
        config.set("scheduler.max_workers", args.workers)
        
    # 如果命令行指定了无头模式，覆盖配置
    if args.headless is not None:
        config.set("playwright.headless", args.headless)
        
    # 初始化日志
    init_logging(config.get("logging"))
    
    # 设置信号处理，确保只在主线程设置
    if threading.current_thread() is threading.main_thread():
        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)
        # 在Windows上添加对Ctrl+Break的处理
        if hasattr(signal, 'SIGBREAK'):
            signal.signal(signal.SIGBREAK, emergency_exit)
            
    # 输出配置信息
    logger.info(f"最大工作进程数: {config.get('scheduler.max_workers')}")
    logger.info(f"无头模式: {config.get('playwright.headless')}")
    
    # 获取调度器
    scheduler = get_scheduler(config.get("scheduler"))
    
    # 启动调度器
    scheduler.start()
    
    # 从命令行添加单个URL任务
    if args.url:
        task = Task(url=args.url)
        scheduler.add_task(task)
        logger.info(f"添加URL任务: {args.url}")
        
    # 从文件添加任务
    if args.tasks:
        tasks_data = load_tasks(args.tasks)
        tasks = create_tasks(tasks_data)
        
        if tasks:
            task_ids = scheduler.add_tasks(tasks)
            logger.info(f"从文件 {args.tasks} 添加了 {len(task_ids)} 个任务")
        else:
            logger.warning(f"从文件 {args.tasks} 加载任务失败或没有任务")
            
    # 如果没有添加任何任务，提示用户
    if not args.url and not args.tasks:
        logger.info("未指定任务，调度器将等待手动添加任务")
        
    try:
        # 主循环
        while True:
            # 获取调度器统计信息
            stats = scheduler.get_stats()
            
            # 输出状态
            logger.info(
                f"状态: 总任务={stats['total_tasks']}, "
                f"队列中={stats['queue_size']}, "
                f"运行中={stats['running_tasks']}, "
                f"工作进程={stats['workers']}"
            )
            
            # 检查是否所有任务都已完成
            if (stats['total_tasks'] > 0 and 
                stats['queue_size'] == 0 and 
                stats['running_tasks'] == 0):
                logger.info("所有任务已完成")
                break
                
            # 休眠
            time.sleep(10)
            
    except KeyboardInterrupt:
        logger.info("接收到用户中断，正在关闭...")
        
    finally:
        # 停止调度器
        scheduler.stop()
        
    logger.info("爬虫框架已关闭")


if __name__ == "__main__":
    main() 