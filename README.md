# 多进程爬虫框架

一个基于Python的多进程爬虫框架，使用Playwright作为浏览器自动化引擎。

## 特点

- **多进程架构**：采用调度进程 + 工作进程的模式，提高并发性能
- **Playwright支持**：基于强大的Playwright浏览器自动化工具，支持多种浏览器
- **完备的日志系统**：使用loguru提供详细的日志记录，支持日志轮转和压缩
- **异常处理机制**：完善的异常捕获和处理流程，确保系统稳定性
- **灵活的配置系统**：支持YAML、JSON和环境变量配置
- **优先级调度**：基于优先级的任务调度
- **任务持久化**：支持任务队列的持久化和恢复
- **心跳监控**：工作进程心跳机制，自动检测和重启失效的工作进程

## 安装

### 要求

- Python 3.7+
- 依赖库（见requirements.txt）

### 安装步骤

1. 克隆仓库

```bash
git clone https://github.com/yourusername/spider-framework.git
cd spider-framework
```

2. 安装依赖

```bash
pip install -r requirements.txt
```

3. 安装Playwright浏览器

```bash
playwright install
```

## 使用方法

### 基本用法

通过命令行运行爬虫框架：

```bash
python -m spider_framework.main -u https://example.com
```

### 使用任务文件

准备一个JSON格式的任务文件，然后运行：

```bash
python -m spider_framework.main -t path/to/tasks.json
```

### 命令行参数

- `-c, --config`: 配置文件路径
- `-u, --url`: 要爬取的URL（简单模式）
- `-t, --tasks`: 任务文件路径（JSON格式）
- `-w, --workers`: 工作进程数量
- `--headless`: 是否使用无头模式

### 配置文件

框架使用YAML格式的配置文件，默认配置文件为`config/default_config.yaml`。您可以创建自定义配置文件并通过`-c`参数指定。

## 项目结构

```
spider_framework/
├── config/                 # 配置模块
│   ├── config_manager.py   # 配置管理器
│   └── default_config.yaml # 默认配置
├── data/                   # 数据存储目录
│   ├── screenshots/        # 截图目录
│   └── task_queue.json     # 任务队列持久化
├── examples/               # 示例
│   └── tasks.json          # 示例任务文件
├── logs/                   # 日志目录
├── scheduler/              # 调度器模块
│   ├── scheduler.py        # 调度器实现
│   └── task.py             # 任务数据模型
├── utils/                  # 工具模块
│   ├── browser.py          # 浏览器管理器
│   ├── exceptions.py       # 异常定义
│   └── logger.py           # 日志管理器
├── worker/                 # 工作进程模块
│   └── worker_process.py   # 工作进程实现
├── main.py                 # 程序入口
└── requirements.txt        # 依赖列表
```

## 任务定义

任务以JSON格式定义，包含以下主要字段：

- `url`: 待爬取的URL（必需）
- `name`: 任务名称
- `priority`: 优先级（LOW/NORMAL/HIGH/CRITICAL）
- `timeout`: 超时时间（秒）
- `max_retries`: 最大重试次数
- `headers`: 请求头
- `cookies`: Cookie设置
- `action_rules`: 页面操作规则
- `extraction_rules`: 数据提取规则
- `metadata`: 元数据

示例任务：

```json
{
  "url": "https://example.com",
  "name": "示例任务",
  "priority": "NORMAL",
  "timeout": 60,
  "action_rules": [
    {
      "type": "wait",
      "time": 2000
    }
  ],
  "extraction_rules": [
    {
      "field": "title",
      "selector": "h1",
      "attr": "textContent"
    }
  ]
}
```

## 页面操作规则

支持以下操作类型：

- `click`: 点击元素
- `fill`: 填充表单
- `select`: 选择选项
- `wait`: 等待时间
- `wait_for_selector`: 等待选择器出现
- `wait_for_navigation`: 等待导航完成
- `scroll`: 滚动页面
- `screenshot`: 截图
- `execute_js`: 执行JavaScript

## 数据提取规则

每个提取规则包含以下字段：

- `field`: 字段名称
- `selector`: CSS选择器
- `attr`: 要提取的属性（默认为"textContent"）
- `multiple`: 是否提取多个元素（默认为false）
- `strip`: 是否去除空白（默认为true）
- `regex`: 正则表达式过滤
- `type`: 类型转换（int/float/bool/json）

## 错误处理

框架提供了全面的错误处理机制，包括：

- 任务重试
- 异常日志记录
- 工作进程监控和自动重启
- 超时控制

## 贡献

欢迎提交问题和贡献代码。要贡献代码，请：

1. Fork 仓库
2. 创建功能分支 (`git checkout -b feature/your-feature`)
3. 提交更改 (`git commit -am 'Add some feature'`)
4. 推送分支 (`git push origin feature/your-feature`)
5. 创建Pull Request

## 许可证

[MIT](LICENSE) 