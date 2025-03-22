#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
工作进程模块
负责执行爬虫任务
"""

import os
import time
import uuid
import json
import asyncio
import threading
import multiprocessing as mp
from typing import Dict, List, Any, Optional, Union, Tuple
from dataclasses import asdict
import traceback

from spider_framework.utils.logger import get_logger, init_logging
from spider_framework.utils.exceptions import WorkerError, TaskError, BrowserError, PageError
from spider_framework.utils.browser import get_browser_manager
from spider_framework.config.config_manager import get_config
from spider_framework.scheduler.task import Task, TaskStatus, TaskResult, TaskPriority


logger = get_logger("Worker")


class Worker:
    """工作进程类，负责执行爬虫任务"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化工作进程
        
        Args:
            config: 工作进程配置
        """
        self.config = config or {}
        self.worker_id = str(uuid.uuid4())
        self.current_task: Optional[Task] = None
        self.browser_manager = None
        self.stop_event = threading.Event()
        self.heartbeat_thread = None
        self.start_time = time.time()
        
        # 读取配置
        self.timeout = self.config.get("timeout", 300)
        self.heartbeat_interval = self.config.get("heartbeat_interval", 10)
        self.shutdown_timeout = self.config.get("shutdown_timeout", 30)
        
        logger.info(f"工作进程初始化完成，ID: {self.worker_id}")
        
    async def initialize(self) -> None:
        """初始化工作进程环境"""
        try:
            # 获取浏览器管理器
            browser_config = self.config.get("playwright", {})
            self.browser_manager = await get_browser_manager(browser_config)
            
            # 启动心跳线程
            self.heartbeat_thread = threading.Thread(target=self._send_heartbeat, daemon=True)
            self.heartbeat_thread.start()
            
            logger.info("工作进程环境初始化完成")
        except Exception as e:
            logger.error(f"工作进程环境初始化失败: {str(e)}")
            raise WorkerError(f"初始化失败: {str(e)}", worker_id=self.worker_id)
            
    async def execute_task(self, task: Task) -> TaskResult:
        """
        执行爬虫任务
        
        Args:
            task: 爬虫任务
            
        Returns:
            任务执行结果
        """
        self.current_task = task
        
        # 记录开始时间
        start_time = time.time()
        
        # 标记任务开始
        task.start(self.worker_id)
        
        # 如果浏览器管理器未初始化，先初始化
        if not self.browser_manager:
            await self.initialize()
            
        logger.info(f"开始执行任务 {task.task_id}: {task.url}")
        
        # 初始化任务结果
        result = TaskResult(success=False)
        
        try:
            # 创建新页面
            page = await self.browser_manager.new_page()
            
            try:
                # 设置超时
                timeout = task.timeout or self.timeout
                
                # 添加Cookie（如果有）
                if task.cookies:
                    for cookie in task.cookies:
                        await page.context.add_cookies([cookie])
                        
                # 设置请求头（如果有）
                if task.headers:
                    await page.set_extra_http_headers(task.headers)
                    
                # 导航到目标URL
                await self.browser_manager.navigate(page, task.url)
                
                # 执行页面操作规则
                if task.action_rules:
                    await self._execute_actions(page, task.action_rules)
                    
                # 提取数据
                if task.extraction_rules:
                    data = await self._extract_data(page, task.extraction_rules)
                    result.data = data
                    
                # 截图（如果需要）
                if task.metadata.get("screenshot", False):
                    screenshot_path = os.path.join(
                        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "data",
                        "screenshots",
                        f"task_{task.task_id}_{int(time.time())}.png"
                    )
                    await self.browser_manager.screenshot(page, screenshot_path)
                    result.screenshot_path = screenshot_path
                    
                # 标记为成功
                result.success = True
                logger.info(f"任务 {task.task_id} 执行成功")
                
            except Exception as e:
                logger.error(f"任务 {task.task_id} 执行失败: {str(e)}")
                logger.debug(traceback.format_exc())
                result.error = str(e)
                result.success = False
                
            finally:
                # 关闭页面
                await self.browser_manager.close_page(page)
                
        except Exception as e:
            logger.error(f"创建页面失败: {str(e)}")
            logger.debug(traceback.format_exc())
            result.error = f"创建页面失败: {str(e)}"
            result.success = False
            
        # 计算执行时间
        result.execution_time = time.time() - start_time
        
        # 清除当前任务
        self.current_task = None
        
        return result
        
    async def _execute_actions(self, page, action_rules: List[Dict[str, Any]]) -> None:
        """
        执行页面操作规则
        
        Args:
            page: Playwright页面对象
            action_rules: 操作规则列表
        """
        for rule in action_rules:
            action_type = rule.get("type")
            
            if action_type == "click":
                selector = rule.get("selector")
                await page.click(selector)
                logger.debug(f"点击元素: {selector}")
                
            elif action_type == "fill":
                selector = rule.get("selector")
                value = rule.get("value")
                await page.fill(selector, value)
                logger.debug(f"填充表单 {selector}: {value}")
                
            elif action_type == "select":
                selector = rule.get("selector")
                value = rule.get("value")
                await page.select_option(selector, value)
                logger.debug(f"选择选项 {selector}: {value}")
                
            elif action_type == "wait":
                time_ms = rule.get("time", 1000)
                await page.wait_for_timeout(time_ms)
                logger.debug(f"等待: {time_ms}ms")
                
            elif action_type == "wait_for_selector":
                selector = rule.get("selector")
                state = rule.get("state", "visible")
                timeout = rule.get("timeout", 30000)
                await self.browser_manager.wait_for_selector(page, selector, state, timeout)
                logger.debug(f"等待选择器 {selector} {state}")
                
            elif action_type == "wait_for_navigation":
                url = rule.get("url")
                wait_until = rule.get("wait_until", "load")
                timeout = rule.get("timeout", 30000)
                await page.wait_for_navigation(url=url, wait_until=wait_until, timeout=timeout)
                logger.debug("等待导航完成")
                
            elif action_type == "scroll":
                x = rule.get("x", 0)
                y = rule.get("y", 0)
                await page.evaluate(f"window.scrollTo({x}, {y})")
                logger.debug(f"滚动到 x:{x}, y:{y}")
                
            elif action_type == "screenshot":
                path = rule.get("path")
                full_page = rule.get("full_page", True)
                await page.screenshot(path=path, full_page=full_page)
                logger.debug(f"截图已保存至: {path}")
                
            elif action_type == "execute_js":
                script = rule.get("script")
                result = await page.evaluate(script)
                logger.debug(f"执行JavaScript: {script[:50]}... 结果: {result}")
                
            else:
                logger.warning(f"未知的操作类型: {action_type}")
                
            # 操作间隔
            delay = rule.get("delay", 500)
            if delay > 0:
                await page.wait_for_timeout(delay)
                
    async def _extract_data(self, page, extraction_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        从页面提取数据
        
        Args:
            page: Playwright页面对象
            extraction_rules: 提取规则列表
            
        Returns:
            提取的数据
        """
        result = {}
        
        for rule in extraction_rules:
            field = rule.get("field")
            selector = rule.get("selector")
            attr = rule.get("attr", "textContent")
            multiple = rule.get("multiple", False)
            
            try:
                if multiple:
                    # 提取多个元素
                    elements = await page.query_selector_all(selector)
                    values = []
                    
                    for element in elements:
                        if attr == "textContent":
                            value = await element.text_content()
                        elif attr == "innerText":
                            value = await element.inner_text()
                        elif attr == "innerHTML":
                            value = await element.inner_html()
                        else:
                            value = await element.get_attribute(attr)
                            
                        # 应用后处理
                        value = self._post_process_value(value, rule)
                        values.append(value)
                        
                    result[field] = values
                else:
                    # 提取单个元素
                    element = await page.query_selector(selector)
                    if element:
                        if attr == "textContent":
                            value = await element.text_content()
                        elif attr == "innerText":
                            value = await element.inner_text()
                        elif attr == "innerHTML":
                            value = await element.inner_html()
                        else:
                            value = await element.get_attribute(attr)
                            
                        # 应用后处理
                        value = self._post_process_value(value, rule)
                        result[field] = value
                    else:
                        result[field] = None
                        
            except Exception as e:
                logger.error(f"提取字段 {field} 失败: {str(e)}")
                result[field] = None
                
        return result
        
    def _post_process_value(self, value: str, rule: Dict[str, Any]) -> Any:
        """
        对提取的值进行后处理
        
        Args:
            value: 提取的值
            rule: 提取规则
            
        Returns:
            处理后的值
        """
        if value is None:
            return None
            
        # 去除空白
        strip = rule.get("strip", True)
        if strip and isinstance(value, str):
            value = value.strip()
            
        # 正则表达式
        regex = rule.get("regex")
        if regex and isinstance(value, str):
            import re
            pattern = re.compile(regex)
            match = pattern.search(value)
            if match:
                value = match.group(rule.get("regex_group", 0))
                
        # 类型转换
        value_type = rule.get("type")
        if value_type:
            try:
                if value_type == "int":
                    value = int(value)
                elif value_type == "float":
                    value = float(value)
                elif value_type == "bool":
                    value = value.lower() in ("true", "yes", "1", "t", "y")
                elif value_type == "json":
                    value = json.loads(value)
            except Exception:
                pass
                
        return value
        
    def _send_heartbeat(self) -> None:
        """发送心跳到调度器"""
        from spider_framework.scheduler.scheduler import get_scheduler
        
        scheduler = get_scheduler()
        
        # 添加心跳计数器和最大尝试次数
        failures = 0
        max_failures = 5
        
        while not self.stop_event.is_set():
            try:
                # 获取内存使用情况
                memory_usage = self.get_memory_usage()
                
                # 构造状态信息
                status = {
                    "memory": memory_usage,
                    "current_task": self.current_task.task_id if self.current_task else None,
                    "timestamp": time.time()
                }
                
                # 更新心跳
                scheduler.update_worker_heartbeat(self.worker_id, status)
                logger.debug(f"发送心跳，当前任务: {self.current_task.task_id if self.current_task else '无'}")
                
                # 成功后重置失败计数
                failures = 0
                
            except Exception as e:
                logger.error(f"发送心跳失败: {str(e)}")
                failures += 1
                
                # 如果连续失败多次，停止工作进程
                if failures >= max_failures:
                    logger.error(f"心跳发送连续失败 {max_failures} 次，停止工作进程")
                    self.stop_event.set()
                    break
                
            # 休眠
            time.sleep(self.heartbeat_interval)
            
    def get_memory_usage(self) -> Dict[str, float]:
        """
        获取内存使用情况
        
        Returns:
            内存使用情况
        """
        import psutil
        
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        
        return {
            "rss_mb": memory_info.rss / (1024 * 1024),  # MB
            "vms_mb": memory_info.vms / (1024 * 1024),  # MB
            "percent": process.memory_percent()
        }
        
    def get_status(self) -> Dict[str, Any]:
        """
        获取工作进程状态
        
        Returns:
            工作进程状态
        """
        return {
            "worker_id": self.worker_id,
            "current_task": self.current_task.to_dict() if self.current_task else None,
            "memory": self.get_memory_usage(),
            "uptime": time.time() - self.start_time
        }
        
    async def shutdown(self) -> None:
        """关闭工作进程"""
        logger.info("关闭工作进程")
        
        # 设置停止标志
        self.stop_event.set()
        
        # 等待当前任务完成
        shutdown_start = time.time()
        while self.current_task and time.time() - shutdown_start < self.shutdown_timeout:
            logger.info(f"等待任务 {self.current_task.task_id} 完成...")
            await asyncio.sleep(1)
            
        # 如果仍有任务在执行，强制关闭
        if self.current_task:
            logger.warning(f"强制关闭工作进程，任务 {self.current_task.task_id} 未完成")
            
        # 关闭浏览器管理器
        if self.browser_manager:
            await self.browser_manager.close()
            
        # 等待心跳线程结束
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            self.heartbeat_thread.join(timeout=5)
            
        logger.info("工作进程已关闭")


async def worker_process_task_loop(config: Dict[str, Any]) -> None:
    """
    工作进程任务循环
    
    Args:
        config: 配置
    """
    # 初始化工作进程
    worker = Worker(config.get("worker", {}))
    await worker.initialize()
    
    # 获取调度器
    from spider_framework.scheduler.scheduler import get_scheduler
    scheduler = get_scheduler()
    
    # 注册工作进程
    scheduler.register_worker(worker.worker_id, worker.get_status())
    
    try:
        while not worker.stop_event.is_set():
            # 获取下一个任务
            task = scheduler.get_next_task()
            
            if task:
                # 更新运行中任务列表
                scheduler.running_tasks[task.task_id] = task
                
                # 执行任务
                try:
                    result = await worker.execute_task(task)
                    
                    # 更新任务状态
                    if result.success:
                        scheduler.update_task_status(task.task_id, TaskStatus.SUCCESS, result, worker.worker_id)
                    else:
                        if task.retry_count < task.max_retries:
                            scheduler.update_task_status(task.task_id, TaskStatus.RETRY, result, worker.worker_id)
                        else:
                            scheduler.update_task_status(task.task_id, TaskStatus.FAILED, result, worker.worker_id)
                            
                except Exception as e:
                    logger.error(f"执行任务 {task.task_id} 发生异常: {str(e)}")
                    
                    # 创建错误结果
                    result = TaskResult(
                        success=False,
                        error=str(e),
                        execution_time=time.time() - (task.started_at or task.created_at)
                    )
                    
                    # 更新任务状态
                    if task.retry_count < task.max_retries:
                        scheduler.update_task_status(task.task_id, TaskStatus.RETRY, result, worker.worker_id)
                    else:
                        scheduler.update_task_status(task.task_id, TaskStatus.FAILED, result, worker.worker_id)
            else:
                # 没有任务，等待一会
                await asyncio.sleep(1)
                
    except Exception as e:
        logger.error(f"工作进程任务循环异常: {str(e)}")
        logger.debug(traceback.format_exc())
        
    finally:
        # 关闭工作进程
        await worker.shutdown()


def worker_process_main(config: Dict[str, Any]) -> None:
    """
    工作进程主函数
    
    Args:
        config: 配置
    """
    try:
        # 设置进程名称
        mp.current_process().name = f"SpiderWorker-{os.getpid()}"
        
        # 初始化日志
        init_logging(config.get("logging", {}))
        
        # 设置超时处理，防止进程卡住
        async def run_with_timeout():
            # 创建任务
            task_loop = asyncio.create_task(worker_process_task_loop(config))
            
            try:
                # 设置超时
                await asyncio.wait_for(task_loop, timeout=86400)  # 24小时超时
            except asyncio.TimeoutError:
                logger.error("工作进程执行超时")
            except asyncio.CancelledError:
                logger.info("工作进程任务被取消")
            except Exception as e:
                logger.error(f"工作进程异常: {str(e)}")
                logger.debug(traceback.format_exc())
        
        # 运行异步任务循环
        asyncio.run(run_with_timeout())
        
    except Exception as e:
        logger.error(f"工作进程异常: {str(e)}")
        logger.debug(traceback.format_exc())
    finally:
        # 确保进程能够退出
        logger.info("工作进程退出")
        # 强制退出进程，以防有阻塞调用
        os._exit(0) 