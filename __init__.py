#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Spider Framework
===============

一个基于多进程和Playwright的爬虫框架。
"""

__version__ = "0.1.0"
__author__ = "Spider Framework Team"

from .config.config_manager import get_config
from .scheduler.task import Task, TaskStatus, TaskPriority, TaskResult
from .scheduler.scheduler import get_scheduler
from .utils.logger import get_logger, init_logging
from .worker.worker_process import Worker 