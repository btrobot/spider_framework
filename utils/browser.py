#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
浏览器管理器模块
负责创建和管理Playwright浏览器实例
"""

import os
import time
import asyncio
import atexit
from typing import Dict, Any, Optional, List, Tuple, Union

import psutil
from playwright.async_api import async_playwright, Browser, Page, Playwright
from playwright.async_api import Error as PlaywrightError

from .logger import get_logger
from .exceptions import BrowserError, PageError, ElementError


logger = get_logger("BrowserManager")


class BrowserManager:
    """浏览器管理器，负责创建和管理Playwright浏览器实例"""
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        初始化浏览器管理器
        
        Args:
            config: 浏览器配置
        """
        self.config = config or {}
        self._playwright: Optional[Playwright] = None
        self._browser: Optional[Browser] = None
        self._pages: List[Page] = []
        self._is_initialized = False
        
        # 注册退出处理程序
        atexit.register(self._cleanup)
        
    async def initialize(self) -> None:
        """初始化浏览器管理器"""
        if self._is_initialized:
            return
            
        try:
            # 启动Playwright
            self._playwright = await async_playwright().start()
            
            # 获取浏览器类型
            browser_type = self.config.get("browser", "chromium")
            
            # 选择浏览器
            if browser_type == "chromium":
                browser_instance = self._playwright.chromium
            elif browser_type == "firefox":
                browser_instance = self._playwright.firefox
            elif browser_type == "webkit":
                browser_instance = self._playwright.webkit
            else:
                raise BrowserError(f"不支持的浏览器类型: {browser_type}")
                
            # 准备启动参数
            launch_options = {
                "headless": self.config.get("headless", True),
                "slow_mo": self.config.get("slow_mo", 50),
            }
            
            # 添加代理配置
            proxy = self.config.get("proxy", "")
            if proxy:
                launch_options["proxy"] = {"server": proxy}
                
            # 启动浏览器
            self._browser = await browser_instance.launch(**launch_options)
            
            # 设置初始化标志
            self._is_initialized = True
            
            logger.info(f"成功初始化浏览器: {browser_type}")
        except Exception as e:
            logger.error(f"初始化浏览器失败: {str(e)}")
            await self._cleanup()
            raise BrowserError(f"初始化浏览器失败: {str(e)}", browser=browser_type)
            
    async def new_page(self) -> Page:
        """
        创建新页面
        
        Returns:
            Page: 页面实例
        """
        if not self._is_initialized:
            await self.initialize()
            
        try:
            # 创建新上下文
            context = await self._browser.new_context(
                viewport={
                    "width": self.config.get("viewport", {}).get("width", 1280),
                    "height": self.config.get("viewport", {}).get("height", 800),
                },
                ignore_https_errors=self.config.get("ignore_https_errors", True),
                user_agent=self.config.get("user_agent", ""),
            )
            
            # 创建新页面
            page = await context.new_page()
            
            # 设置默认超时
            page.set_default_timeout(self.config.get("timeout", 30000))
            
            # 存储页面引用
            self._pages.append(page)
            
            logger.debug(f"创建新页面，当前页面数: {len(self._pages)}")
            return page
        except Exception as e:
            logger.error(f"创建页面失败: {str(e)}")
            raise PageError(f"创建页面失败: {str(e)}")
            
    async def close_page(self, page: Page) -> None:
        """
        关闭页面
        
        Args:
            page: 要关闭的页面
        """
        if page in self._pages:
            try:
                context = page.context
                await page.close()
                await context.close()
                self._pages.remove(page)
                logger.debug(f"关闭页面，当前页面数: {len(self._pages)}")
            except Exception as e:
                logger.error(f"关闭页面失败: {str(e)}")
                
    async def navigate(self, page: Page, url: str, wait_until: str = "load") -> None:
        """
        导航到指定URL
        
        Args:
            page: 页面实例
            url: 要导航的URL
            wait_until: 等待条件（load, domcontentloaded, networkidle, commit）
        """
        try:
            response = await page.goto(url, wait_until=wait_until, timeout=self.config.get("timeout", 30000))
            
            if response is None:
                logger.warning(f"导航到 {url} 没有收到响应")
                return
                
            status = response.status
            if status >= 400:
                logger.warning(f"导航到 {url} 失败，状态码: {status}")
                
            logger.debug(f"成功导航到 {url}，状态码: {status}")
        except PlaywrightError as e:
            logger.error(f"导航到 {url} 失败: {str(e)}")
            raise PageError(f"导航失败: {str(e)}", url=url)
            
    async def wait_for_selector(self, page: Page, selector: str, 
                               state: str = "visible", timeout: int = None) -> Any:
        """
        等待选择器元素
        
        Args:
            page: 页面实例
            selector: CSS选择器
            state: 等待状态（attached, detached, visible, hidden）
            timeout: 超时时间（毫秒）
            
        Returns:
            元素句柄
        """
        timeout = timeout or self.config.get("timeout", 30000)
        try:
            return await page.wait_for_selector(selector, state=state, timeout=timeout)
        except PlaywrightError as e:
            logger.error(f"等待选择器 {selector} 失败: {str(e)}")
            raise ElementError(f"等待选择器失败: {str(e)}", selector=selector, url=page.url)
            
    async def screenshot(self, page: Page, path: str) -> None:
        """
        截取页面截图
        
        Args:
            page: 页面实例
            path: 保存路径
        """
        try:
            # 确保目录存在
            os.makedirs(os.path.dirname(path), exist_ok=True)
            
            # 截图
            await page.screenshot(path=path, full_page=True)
            logger.debug(f"页面截图已保存至 {path}")
        except Exception as e:
            logger.error(f"截图失败: {str(e)}")
            
    async def get_memory_usage(self) -> Dict[str, Any]:
        """
        获取内存使用情况
        
        Returns:
            内存使用情况
        """
        process = psutil.Process(os.getpid())
        memory_info = process.memory_info()
        return {
            "rss": memory_info.rss / (1024 * 1024),  # MB
            "vms": memory_info.vms / (1024 * 1024),  # MB
            "percent": process.memory_percent(),
        }
        
    async def close(self) -> None:
        """关闭浏览器管理器"""
        await self._cleanup()
        
    async def _cleanup(self) -> None:
        """清理资源"""
        if not self._is_initialized:
            return
            
        # 关闭所有页面
        for page in self._pages[:]:
            try:
                await self.close_page(page)
            except:
                pass
        self._pages.clear()
        
        # 关闭浏览器
        if self._browser:
            try:
                await self._browser.close()
                self._browser = None
            except:
                pass
                
        # 关闭Playwright
        if self._playwright:
            try:
                await self._playwright.stop()
                self._playwright = None
            except:
                pass
                
        self._is_initialized = False
        logger.info("浏览器管理器已关闭")


# 单例模式
_browser_manager_instance = None


async def get_browser_manager(config: Dict[str, Any] = None) -> BrowserManager:
    """
    获取浏览器管理器实例
    
    Args:
        config: 浏览器配置
        
    Returns:
        浏览器管理器实例
    """
    global _browser_manager_instance
    if _browser_manager_instance is None:
        _browser_manager_instance = BrowserManager(config)
        await _browser_manager_instance.initialize()
    return _browser_manager_instance 