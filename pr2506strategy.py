# 2506合约pr加工费
import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from base_strategy import BaseGridStrategy
from tqsds import TqAuth, TqAccount, TqKq
from datetime import datetime
import asyncio

class pr2506Strategy(BaseGridStrategy):
    def _get_symbols(self) -> dict:
        return {
            "pr": "CZCE.PR506",
            "ta": "CZCE.TA506", 
            "eg": "DCE.eg2506"
        }

    def _get_grid_settings(self) -> list:
        return [
            {'layer': -1, 'min': 0, 'max': 360, 'up': 4, 'down': 4},
            {'layer': 0, 'min': 360, 'max': 370, 'up': 4, 'down': 3},
            {'layer': 1, 'min': 370, 'max': 380, 'up': 3, 'down': 2},
            {'layer': 2, 'min': 380, 'max': 390, 'up': 2, 'down': 1},
            {'layer': 3, 'min': 390, 'max': 400, 'up': 1, 'down': 0},
            {'layer': 4, 'min': 400, 'max': 410, 'up': 0, 'down': -1},
            {'layer': 5, 'min': 410, 'max': 420, 'up': -1, 'down': -2},
            {'layer': 6, 'min': 420, 'max': 430, 'up': -2, 'down': -3},
            {'layer': 7, 'min': 430, 'max': 440, 'up': -3, 'down': -4},
            {'layer': 8, 'min': 440, 'max': 450, 'up': -4, 'down': -5},
            {'layer': 9, 'min': 450, 'max': 460, 'up': -5, 'down': -6},
            {'layer': 10, 'min': 460, 'max': 470, 'up': -6, 'down': -7},
            {'layer': 11, 'min': 470, 'max': 480, 'up': -7, 'down': -9},
            {'layer': 12, 'min': 480, 'max': float('inf'), 'up': -9, 'down': -9},
        ]

    def _get_min_unit(self) -> dict:
        return {'pr': 2, 'ta': -5, 'eg': -1}


async def main():
    # 配置账户信息
    auth_user = input("请输入天勤账号: ")
    auth_password = input("请输入天勤密码: ")
    auth = TqAuth(auth_user, auth_password)
    
    # 选择账户类型
    use_real_account = input("是否选择实盘账户登录? (Y/N): ").upper()
    
    if use_real_account == 'Y':
        # 实盘账户信息
        broker_id = input("请输入期货公司代码: ")
        account_id = input("请输入账号: ")
        account_password = input("请输入密码: ")
        
        # 创建实盘账户对象
        account = TqAccount(broker_id, account_id, account_password)
    else:
        # 使用快期模拟账户
        account = TqKq()
    
    # 初始化策略
    strategy = pr2506Strategy(auth, account)
    
    try:
        await strategy.run()
    finally:
        await strategy.stop()

if __name__ == "__main__":
    asyncio.run(main())