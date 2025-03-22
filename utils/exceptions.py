#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
异常处理模块
定义爬虫框架的自定义异常类
"""


class SpiderError(Exception):
    """爬虫框架基础异常类"""
    
    def __init__(self, message: str = None, code: int = None):
        """
        初始化异常
        
        Args:
            message: 异常消息
            code: 异常代码
        """
        self.message = message or "爬虫框架错误"
        self.code = code or 1000
        super().__init__(self.message)


class ConfigError(SpiderError):
    """配置错误"""
    
    def __init__(self, message: str = None, code: int = None):
        message = message or "配置错误"
        code = code or 1100
        super().__init__(message, code)


class NetworkError(SpiderError):
    """网络错误"""
    
    def __init__(self, message: str = None, code: int = None, url: str = None):
        """
        初始化网络异常
        
        Args:
            message: 异常消息
            code: 异常代码
            url: 请求的URL
        """
        message = message or "网络错误"
        code = code or 1200
        self.url = url
        super().__init__(f"{message} - URL: {url}" if url else message, code)


class RequestError(NetworkError):
    """请求错误"""
    
    def __init__(self, message: str = None, code: int = None, url: str = None, 
                 status_code: int = None, response: str = None):
        """
        初始化请求异常
        
        Args:
            message: 异常消息
            code: 异常代码
            url: 请求的URL
            status_code: HTTP状态码
            response: 响应内容
        """
        message = message or "请求错误"
        code = code or 1210
        self.status_code = status_code
        self.response = response
        
        if status_code:
            message = f"{message} (状态码: {status_code})"
            
        super().__init__(message, code, url)


class TimeoutError(NetworkError):
    """超时错误"""
    
    def __init__(self, message: str = None, code: int = None, url: str = None, timeout: float = None):
        """
        初始化超时异常
        
        Args:
            message: 异常消息
            code: 异常代码
            url: 请求的URL
            timeout: 超时时间（秒）
        """
        message = message or "请求超时"
        code = code or 1220
        self.timeout = timeout
        
        if timeout:
            message = f"{message} (超时: {timeout}秒)"
            
        super().__init__(message, code, url)


class ProxyError(NetworkError):
    """代理错误"""
    
    def __init__(self, message: str = None, code: int = None, url: str = None, proxy: str = None):
        """
        初始化代理异常
        
        Args:
            message: 异常消息
            code: 异常代码
            url: 请求的URL
            proxy: 代理地址
        """
        message = message or "代理错误"
        code = code or 1230
        self.proxy = proxy
        
        if proxy:
            message = f"{message} (代理: {proxy})"
            
        super().__init__(message, code, url)


class BrowserError(SpiderError):
    """浏览器错误"""
    
    def __init__(self, message: str = None, code: int = None, browser: str = None):
        """
        初始化浏览器异常
        
        Args:
            message: 异常消息
            code: 异常代码
            browser: 浏览器类型
        """
        message = message or "浏览器错误"
        code = code or 1300
        self.browser = browser
        
        if browser:
            message = f"{message} (浏览器: {browser})"
            
        super().__init__(message, code)


class PageError(BrowserError):
    """页面错误"""
    
    def __init__(self, message: str = None, code: int = None, browser: str = None, url: str = None):
        """
        初始化页面异常
        
        Args:
            message: 异常消息
            code: 异常代码
            browser: 浏览器类型
            url: 页面URL
        """
        message = message or "页面错误"
        code = code or 1310
        self.url = url
        
        if url:
            message = f"{message} - URL: {url}"
            
        super().__init__(message, code, browser)


class ElementError(PageError):
    """元素错误"""
    
    def __init__(self, message: str = None, code: int = None, browser: str = None, 
                 url: str = None, selector: str = None):
        """
        初始化元素异常
        
        Args:
            message: 异常消息
            code: 异常代码
            browser: 浏览器类型
            url: 页面URL
            selector: 元素选择器
        """
        message = message or "元素错误"
        code = code or 1320
        self.selector = selector
        
        if selector:
            message = f"{message} (选择器: {selector})"
            
        super().__init__(message, code, browser, url)


class ParseError(SpiderError):
    """解析错误"""
    
    def __init__(self, message: str = None, code: int = None, data: str = None):
        """
        初始化解析异常
        
        Args:
            message: 异常消息
            code: 异常代码
            data: 待解析数据（前几个字符）
        """
        message = message or "解析错误"
        code = code or 1400
        self.data = data
        
        if data and isinstance(data, str):
            preview = data[:100] + '...' if len(data) > 100 else data
            message = f"{message} (数据预览: {preview})"
            
        super().__init__(message, code)


class StorageError(SpiderError):
    """存储错误"""
    
    def __init__(self, message: str = None, code: int = None, storage_type: str = None, path: str = None):
        """
        初始化存储异常
        
        Args:
            message: 异常消息
            code: 异常代码
            storage_type: 存储类型
            path: 存储路径
        """
        message = message or "存储错误"
        code = code or 1500
        self.storage_type = storage_type
        self.path = path
        
        if storage_type:
            message = f"{message} (存储类型: {storage_type})"
        if path:
            message = f"{message} - 路径: {path}"
            
        super().__init__(message, code)


class DatabaseError(StorageError):
    """数据库错误"""
    
    def __init__(self, message: str = None, code: int = None, storage_type: str = None, 
                 path: str = None, query: str = None):
        """
        初始化数据库异常
        
        Args:
            message: 异常消息
            code: 异常代码
            storage_type: 存储类型
            path: 存储路径
            query: 执行的查询
        """
        message = message or "数据库错误"
        code = code or 1510
        self.query = query
        
        if query:
            preview = query[:100] + '...' if len(query) > 100 else query
            message = f"{message} (查询: {preview})"
            
        super().__init__(message, code, storage_type, path)


class SchedulerError(SpiderError):
    """调度器错误"""
    
    def __init__(self, message: str = None, code: int = None):
        message = message or "调度器错误"
        code = code or 1600
        super().__init__(message, code)


class QueueError(SchedulerError):
    """队列错误"""
    
    def __init__(self, message: str = None, code: int = None, queue_name: str = None):
        """
        初始化队列异常
        
        Args:
            message: 异常消息
            code: 异常代码
            queue_name: 队列名称
        """
        message = message or "队列错误"
        code = code or 1610
        self.queue_name = queue_name
        
        if queue_name:
            message = f"{message} (队列: {queue_name})"
            
        super().__init__(message, code)


class WorkerError(SpiderError):
    """工作进程错误"""
    
    def __init__(self, message: str = None, code: int = None, worker_id: str = None):
        """
        初始化工作进程异常
        
        Args:
            message: 异常消息
            code: 异常代码
            worker_id: 工作进程ID
        """
        message = message or "工作进程错误"
        code = code or 1700
        self.worker_id = worker_id
        
        if worker_id:
            message = f"{message} (工作进程ID: {worker_id})"
            
        super().__init__(message, code)


class TaskError(SpiderError):
    """任务错误"""
    
    def __init__(self, message: str = None, code: int = None, task_id: str = None):
        """
        初始化任务异常
        
        Args:
            message: 异常消息
            code: 异常代码
            task_id: 任务ID
        """
        message = message or "任务错误"
        code = code or 1800
        self.task_id = task_id
        
        if task_id:
            message = f"{message} (任务ID: {task_id})"
            
        super().__init__(message, code)


class ShutdownError(SpiderError):
    """关闭错误"""
    
    def __init__(self, message: str = None, code: int = None, component: str = None):
        """
        初始化关闭异常
        
        Args:
            message: 异常消息
            code: 异常代码
            component: 组件名称
        """
        message = message or "关闭错误"
        code = code or 1900
        self.component = component
        
        if component:
            message = f"{message} (组件: {component})"
            
        super().__init__(message, code) 