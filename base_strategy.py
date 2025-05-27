# base_strategy.py
from abc import ABC, abstractmethod
import uuid
from tqsdk import TqApi, TqAuth, TqAccount
from datetime import datetime
import pandas as pd
import os

class BaseGridStrategy(ABC):
    def __init__(self, auth: TqAuth, account=None):
        """基础策略类"""
        # 必需由子类定义的属性
        self.symbols = self._get_symbols()
        self.grid_settings = self._get_grid_settings()
        self.min_unit = self._get_min_unit()
        self.layer = 0
        
        # 初始化核心组件
        self.api = TqApi(account, auth)
        self._init_paths()
        self._init_files()
        self.position = self._load_position()
        self.running = True
        self.quotePrice = {
            "pr": {"bid":0,"ask":0},
            "ta": {"bid":0,"ask":0},
            "eg": {"bid":0,"ask":0}
        }

        # 订阅合约
        self.quotes = {
            sym: self.api.get_quote(contract)
            for sym, contract in self.symbols.items()
        }
        self.account = self.api.get_account()

    @abstractmethod
    def _get_symbols(self) -> dict:
        """子类必须实现的合约配置"""
        pass

    @abstractmethod
    def _get_grid_settings(self) -> list:
        """子类必须实现的网格参数"""
        pass

    @abstractmethod
    def _get_min_unit(self) -> dict:
        """子类必须实现的最小交易单位"""
        pass

    def _init_paths(self):
        """可被子类覆盖的路径生成逻辑"""
        date_str = datetime.now().strftime("%y%m%d")
        self.log_path = os.path.join(
            os.path.dirname(__file__), 
            "logs",  # 默认日志目录
            self.__class__.__name__  # 自动添加策略类名子目录
        )
        os.makedirs(self.log_path, exist_ok=True)
        self.position_file = os.path.join(self.log_path, "position.csv")
        self.trade_file = os.path.join(self.log_path, f"{date_str}_trade.csv")

    # 通用方法
    def _init_files(self):
        """初始化数据文件"""
        if not os.path.exists(self.position_file):
            cols = ['timestamp', 'pr_long', 'pr_short', 'ta_long', 'ta_short', 'eg_long', 'eg_short', 'layer']
            pd.DataFrame(columns=cols).to_csv(self.position_file, index=False)
            
        if not os.path.exists(self.trade_file):
            cols = ['trade_id', 'timestamp', 'contract', 'action', 'price', 'volume', 'offset', 'commission', 'fee', 'quote',
                    'pr_long', 'pr_short', 'ta_long', 'ta_short', 'eg_long', 'eg_short', 'flag']
            pd.DataFrame(columns=cols).to_csv(self.trade_file, index=False)

    def _load_position(self):
        """ 从文件加载最新持仓"""
        if os.path.exists(self.position_file):
            df = pd.read_csv(self.position_file)
            if not df.empty:
                last = df.iloc[-1]
                self.layer = last['layer']
                return {
                    'pr': {'long':last['pr_long'], 'short':last['pr_short']},
                    'ta': {'long':last['ta_long'], 'short':last['ta_short']},
                    'eg': {'long':last['eg_long'], 'short':last['eg_short']}
                }
        return {
            'pr': {'long': 0, 'short': 0},
            'ta': {'long': 0, 'short': 0},
            'eg': {'long': 0, 'short': 0}
        }

    def _get_current_grid(self, fee):
        """获取当前网格区间"""
        for grid in self.grid_settings:
            if grid['min'] <= fee < grid['max']:
                return grid
        return None
    async def _save_position(self):
        """保存持仓到文件（异步）"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        new_row = {
            'timestamp': timestamp,
            'pr_long': self.position['pr']['long'],
            'pr_short': self.position['pr']['short'],
            'ta_long': self.position['ta']['long'],
            'ta_short': self.position['ta']['short'],
            'eg_long': self.position['eg']['long'],
            'eg_short': self.position['eg']['short'],
            'layer': self.layer
        }
        
        with open(self.position_file, 'a') as f:
            f.write(f"{new_row['timestamp']},"
                   f"{new_row['pr_long']},{new_row['pr_short']},"
                   f"{new_row['ta_long']},{new_row['ta_short']},"
                   f"{new_row['eg_long']},{new_row['eg_short']},"
                   f"{new_row['layer']}\n")

    async def _save_trade(self, trade_records, commission, fee, symbol, id):
        """保存交易记录（异步）"""
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        pos = self.position
        trade = trade_records.get(next(iter(trade_records)))
        total_price = sum([trade["price"] * trade["volume"] for trade in trade_records.values()])
        total_volume = sum([trade['volume'] for trade in trade_records.values()])
        quote_price = (self.quotePrice[symbol]['ask'] if trade['direction'] == 'BUY' 
                  else self.quotePrice[symbol]['bid'])
        record = {
            'trade_id': id,
            'timestamp': timestamp,
            'contract': trade['instrument_id'],
            'action': trade['direction'],
            'price': total_price / total_volume,
            'volume': total_volume,
            'offset': trade['offset'],
            'commission': commission,
            'fee': fee,
            'quote': quote_price,
            'pr_long': pos['pr']['long'],
            'pr_short':  pos['pr']['short'],
            'ta_long':  pos['ta']['long'],
            'ta_short': pos['ta']['short'],
            'eg_long': pos['eg']['long'],
            'eg_short': pos['eg']['short'],
            'flag': 1
        }
        
        with open(self.trade_file, 'a') as f:
            f.write(f"{record['trade_id']},"
                f"{record['timestamp']},"
                f"{record['contract']},"
                f"{record['action']},"
                f"{record['price']},"
                f"{record['volume']},"
                f"{record['offset']},"
                f"{record['commission']},"
                f"{record['fee']},"
                f"{record['quote']},"
                f"{record['pr_long']},{record['pr_short']},"
                f"{record['ta_long']},{record['ta_short']},"
                f"{record['eg_long']},{record['eg_short']},"
                f"{record['flag']}\n")

    async def place_orders(self, symbol, volume, direction, fee, id):
        """下单函数"""
        if volume == 0:
            return

        contract = self.symbols[symbol]
        temp = self.account.commission
        if direction == 'BUY':
            if self.position[symbol]['short'] > 0:
                #先平空头
                close_vol = min(volume, self.position[symbol]['short'])
                order = self.api.insert_order(
                    contract,
                    direction=direction,
                    offset='CLOSE',
                    volume=close_vol
                )
                self.position[symbol]['short'] -= close_vol
                volume -= close_vol
                #剩余开多头
                if volume > 0:
                    while order.status != 'FINISHED':
                        self.api.wait_update()
                    await self._save_trade(order.trade_records, self.account.commission - temp, fee, symbol, id)
                    temp = self.account.commission

                    order = self.api.insert_order(
                        contract,
                        direction=direction,
                        offset='OPEN',
                        volume=volume
                    )
                    self.position[symbol]['long'] += volume

            else:
                order = self.api.insert_order(
                    contract,
                    direction=direction,
                    offset='OPEN',
                    volume=volume
                )
                self.position[symbol]['long'] += volume
        else:
            if self.position[symbol]['long'] > 0:
                #先平多头
                close_vol = min(volume, self.position[symbol]['long'])
                order = self.api.insert_order(
                    contract,
                    direction=direction,
                    offset='CLOSE',
                    volume=close_vol
                )
                self.position[symbol]['long'] -= close_vol
                volume -= close_vol

                #剩余开空头
                if volume > 0:
                    while order.status != 'FINISHED':
                        self.api.wait_update()
                    await self._save_trade(order.trade_records, self.account.commission - temp, fee, symbol, id)
                    temp = self.account.commission

                    order = self.api.insert_order(
                        contract,
                        direction=direction,
                        offset='OPEN',
                        volume=volume
                    )
                    self.position[symbol]['short'] += volume
            else:
                order = self.api.insert_order(
                    contract,
                    direction=direction,
                    offset='OPEN',
                    volume=volume
                )
                self.position[symbol]['short'] += volume
        
        # 等待订单成交
        while order.status != 'FINISHED':
            self.api.wait_update()
        
        # 记录交易
        #await self._update_position(symbol, volume, direction)
        await self._save_trade(order.trade_records, self.account.commission - temp, fee, symbol, id)
        await self._save_position()

    async def strategy_loop(self):
        """策略主循环"""
        while self.running:
            try:
                # 等待行情更新
                self.api.wait_update()
                
                time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
                print(f"Strategy:{self.__class__.__name__} Time: { time }")
                # 计算加工费
                fee_buy = self._calculate_fee(direction='BUY')
                # 获取当前网格
                grid_buy = self._get_current_grid(fee_buy)
                if not grid_buy:
                    print(f"当前 BUY 方向加工费：{ fee_buy } ,未触发网格")
                else:
                    new_layer = grid_buy['layer']
                    print(f"当前 BUY 方向加工费 { fee_buy } 位于第 { new_layer } 层 ")
                    if(new_layer < self.layer):   
                        target = {k: v * grid_buy['down'] for k, v in self.min_unit.items()}
                        flag = True

                        orders = []
                        for sym in ['pr', 'ta', 'eg']:
                            current = self.position[sym]
                            target_net = target[sym]
                            current_net = current['long'] - current['short']
                            delta = target_net - current_net
                            if( delta > 0 ):
                                orders.append((sym, delta, 'BUY'))
                            elif( delta == 0):
                                flag = False
                            else:
                                orders.append((sym, -delta, 'SELL'))
                        if( flag ):
                            print(f"原加工费位于第 { self.layer } 层,高于现在,下单方向:BUY")
                            self.layer = new_layer
                            trade_id = str(uuid.uuid4())
                            for sym, vol, direction in orders:
                                await self.place_orders(sym, vol, direction, fee_buy, trade_id)
                        else:
                            print(f"原加工费位于第 { self.layer } 层,高于现在,但无需调整持仓")
                    else:
                        print(f"原加工费位于第 { self.layer } 层,无需调整持仓")

                
                fee_sell = self._calculate_fee(direction='SELL')
                grid_sell = self._get_current_grid(fee_sell)
                if not grid_sell:
                    print(f"当前 SELL 方向加工费：{ fee_sell } ,未触发网格")
                else:
                    new_layer = grid_sell['layer']
                    print(f"当前 SELL 方向加工费 { fee_sell } 位于第{ new_layer } 层 ")
                    if(new_layer > self.layer):
                        target = {k: v * grid_sell['up'] for k, v in self.min_unit.items()}
                        flag = True

                        orders = []
                        for sym in ['pr', 'ta', 'eg']:
                            current = self.position[sym]
                            target_net = target[sym]
                            current_net = current['long'] - current['short']
                            delta = current_net - target_net
                            if( delta > 0 ):
                                orders.append((sym, delta, 'SELL'))
                            elif( delta == 0):
                                flag = False
                            else:
                                orders.append((sym, -delta, 'BUY'))
                        if( flag ):
                            print(f"原加工费位于第 { self.layer } 层,低于现在,下单方向:SELL")
                            self.layer = new_layer
                            trade_id = str(uuid.uuid4())
                            for sym, vol, direction in orders:
                                await self.place_orders(sym, vol, direction, fee_sell, trade_id)
                        else:
                            print(f"原加工费位于第 { self.layer } 层,低于现在,但无需调整持仓")
                    else:
                        print(f"原加工费位于 { self.layer } 层,无需调整持仓")
                                                
            except Exception as e:
                print(f"策略异常类型: {type(e)}，信息：{str(e)}")
                await self.stop()
    def _calculate_fee(self, direction: str) -> float:
        """具体加工费计算"""
        self.quotePrice['pr']['bid'] = self.quotes['pr'].bid_price1
        self.quotePrice['eg']['bid'] = self.quotes['eg'].ask_price1
        self.quotePrice['ta']['bid'] = self.quotes['ta'].ask_price1
        self.quotePrice['pr']['ask'] = self.quotes['pr'].ask_price1
        self.quotePrice['eg']['ask'] = self.quotes['eg'].bid_price1
        self.quotePrice['ta']['ask'] = self.quotes['ta'].bid_price1
        if direction == 'SELL':
            return self.quotes['pr'].bid_price1 - 0.857 * self.quotes['ta'].ask_price1 - 0.335 * self.quotes['eg'].ask_price1
        else:
            return self.quotes['pr'].ask_price1 - 0.857 * self.quotes['ta'].bid_price1 - 0.335 * self.quotes['eg'].bid_price1


    # 通用生命周期管理
    async def stop(self):
        """停止策略（通用）"""
        self.running = False
        self.api.close()

    async def run(self):
        """启动策略（通用）"""
        await self.strategy_loop()