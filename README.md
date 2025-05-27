# 瓶片期货加工费分析与交易系统

## 项目概述
本系统实现瓶片期货（PR/TA/EG）三腿加工费的实时分析、交易策略执行及交易日志管理，主要包含以下功能：
1. 基于秒级K线的每日最大/最小值计算与波动率分析
2. 多月份合约的独立交易策略执行
3. 完整的交易日志记录与利润核算
4. 持仓与盘面数据的可视化分析

## 目录结构
```
prfee/
├── pr_calculate.ipynb # 核心计算模块（最大值/最小值/波动率）
├── pr_fee.csv # 计算结果存储
├── base_strategy.py # 策略基类
├── PrTaEgStrategy/ # 具体月份合约策略
│ ├── pr2506strategy.py # 示例合约策略
│ └── ... # 其他月份合约策略
├── logs/
│ ├── pf/ # 示例合约日志目录
│ │ ├── 250516_trade.log # 日交易记录
│ │ ├── 250519_trade.log # 日交易记录
│ │ └── ...
│ │ ├── merged_date/ # 合并后的交易记录
│ │ ├── position.csv # 当前持仓信息
│ │ ├── profit.csv # 累计利润记录
│ ├── showLog.py # 可视化分析模块
│ └── profit.py # 利润计算模块
└── RiceQuantDB.py # RiceQuant数据库操作模块
```


## 依赖安装
```bash
pip install -r requirements.txt
```

## 注意事项
1. 确保每日收盘后执行profit.py生成利润报告
2. 各月份合约策略文件需独立维护
3. 交易日志格式：时间戳|合约|操作类型|数量|价格
4. 使用前配置好交易账户信息

## 许可协议
[MIT License](LICENSE) 
