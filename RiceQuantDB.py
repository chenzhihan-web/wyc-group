from rqdatac import *
import pandas as pd
import numpy as np
import datetime as dt
from collections import defaultdict

# 主力连续合约
class DominantContractAnalyzer:
    def __init__(self, future_symbol, rule=0, lookback_days=3, threshold=1.1,field='settlement'):
        """
        主力合约分析器初始化
        参数:
        - future_symbol: 期货品种代码(如'EG'表示乙二醇)
        - rule: 主力月切换规则 ，= 0时为持续lookback_days天持仓大于原主力月  = 1时为首次持仓大于原主力月持仓的threshold倍
        - lookback_days: 持续天数
        - threshold: 倍数阈值
        """
        self.future_symbol = future_symbol
        self._initialize_rqdata()
        self.rule = rule
        self.lookback_days = lookback_days
        self.threshold = threshold
        self.field=field
        self.dominant_contract = self._analyze_dominant_contracts()
        self.dominant_contract_close = self._fetch_data()

    def _initialize_rqdata(self):
        """初始化RiceQuant数据连接"""
        try:
            init()
        except Exception as e:
            raise ConnectionError(f"无法连接RQData: {str(e)}")

    def _generate_contract_codes(self):
        """生成要查询的合约代码列表"""
        months = [f"{i:02d}" for i in range(1, 13)]
        current_year_short = int(dt.datetime.today().strftime('%y'))
        years = [f"{i:02d}" for i in range(current_year_short + 1, 10, -1)]
        return [self.future_symbol + y + m for y in years for m in months]

    def _fetch_open_interest_data(self):
        """获取所有合约的持仓量数据"""
        contract_codes = self._generate_contract_codes()
        open_interest_data = pd.DataFrame()
        for contract in contract_codes:
            try:
                data = get_price(contract,start_date='2000-01-01',end_date=dt.datetime.today(),fields=['open_interest']).droplevel(level='order_book_id').iloc[::-1]
                data.columns = [contract[-4:]]
                open_interest_data = pd.concat([open_interest_data, data],axis=1)
            except Exception as e:
                pass

        return open_interest_data.sort_index(ascending=True)

    def _fetch_data(self):
        """获取所有合约的收盘价数据"""
        contract_codes = self._generate_contract_codes()
        close_data = pd.DataFrame()
        for contract in contract_codes:
            try:
                data = get_price(contract,start_date='2000-01-01',end_date=dt.datetime.today(),fields=[self.field]).droplevel(level='order_book_id').iloc[::-1]
                data.columns = [contract[-4:]]
                close_data = pd.concat([close_data, data],axis=1)
            except Exception as e:
                pass

        return close_data.sort_index(ascending=True)

    def _find_next_main_contracts(self, start_index, candidates, reference_contract):
        """
        寻找次主力和次次主力合约
        参数:
        - start_index: 数组循环开始下标
        - candidates: 按持仓量排序的合约列表
        - reference_contract: 参考合约(通常是当前主力合约)

        返回:
        - (secondary_main, thirdly_main) 元组
        """
        for i in range(start_index, len(candidates)):
            if int(candidates[i]) > int(reference_contract):
                secondary = candidates[i]
                for j in range(i+1, len(candidates)):
                    if int(candidates[j]) > int(candidates[i]):
                        return secondary, candidates[j]
                return secondary, None
        return None, None

    def _analyze_dominant_contracts(self):
        """
        分析主力合约变化
        返回:
        - 包含主力、次主力和次次主力合约信息的DataFrame
        """
        open_interest = self._fetch_open_interest_data()
        results = pd.DataFrame(
            index=open_interest.index,
            columns=[
                'main', 'secondary_main', 'thirdly_main',
                'main_oi', 'secondary_main_oi', 'thirdly_main_oi'
            ]
        )

        current_main = None
        secondary_main = None
        thirdly_main = None
        count = defaultdict(int)
        for date, row in open_interest.iterrows():
            sorted_contract = row.dropna().sort_values(ascending=False)
            candidates = sorted_contract.index.tolist()

            if not candidates:
                continue

            # 主力合约初始化或切换逻辑
            if self.rule == 0:
                # 初始化逻辑
                if current_main is None:
                    # 主力合约即为持仓量最大的合约
                    current_main = candidates[0]
                    # 次主力和次次主力为持仓量第二和第三的合约
                    secondary_main, thirdly_main = self._find_next_main_contracts(1,candidates, current_main)
                # 切换逻辑
                elif int(candidates[0]) > int(current_main) and sorted_contract.loc[candidates[0]] > sorted_contract.loc[current_main]:
                    # 当有其他月份的持仓超过主力合约持仓时，计数+1
                    count[candidates[0]] = count[candidates[0]] + 1
                    # 当有某合约计数超过设定天数时，切换主力合约为该合约
                    if count[candidates[0]] >= self.lookback_days:
                        current_main = candidates[0]
                    secondary_main, thirdly_main = self._find_next_main_contracts(0,candidates, current_main)
                else:
                    secondary_main, thirdly_main = self._find_next_main_contracts(0,candidates, current_main)

            elif self.rule == 1:
                # 初始化逻辑
                if (current_main is None):
                    current_main = candidates[0]
                    secondary_main, thirdly_main = self._find_next_main_contracts(1,candidates, current_main)
                # 切换逻辑
                # 出现月份持仓量超过主力月持仓量且比值大于1.1倍时,进行主力月切换
                elif int(candidates[0]) > int(current_main) and sorted_contract.loc[candidates[0]] > self.threshold*sorted_contract.loc[current_main]:
                    current_main = candidates[0]
                    secondary_main, thirdly_main = self._find_next_main_contracts(0,candidates, current_main)
                # 出现月份持仓量超过主力月持仓量但比值小于1.1倍时，不进行主力月切换
                else:
                    secondary_main, thirdly_main = self._find_next_main_contracts(0,candidates, current_main)

            # 存储结果
            results.loc[date] = [
                current_main,
                secondary_main,
                thirdly_main,
                sorted_contract.get(current_main, None),
                sorted_contract.get(secondary_main, None),
                sorted_contract.get(thirdly_main, None)
            ]

        return results

    def get_dominant_contract(self,rank=0,start_date='2015-01-01',end_date=dt.datetime.today()):
        """
        分析主力合约列表
        参数:
        - rank=0: rank=0表示获取主力合约，=1表示次主力合约 =2 表示次次主力合约
        - start_date: 开始时间
        - end_date: 结束时间
        返回:
        - 包含主力合约列表
        """
        columns = self.dominant_contract.columns.tolist()
        r = {0:'main_contract',
             1:'secondary_main_contract',}
        if start_date is None:
            result = self.dominant_contract.loc[:end_date,columns[rank]].sort_index(ascending=False)
            return result.to_frame()
        else:
            result = self.dominant_contract.loc[start_date:end_date,columns[rank]].sort_index(ascending=False)
            return result.to_frame()

    def get_dominant_contract_price(self,rank=0,start_date='2015-01-01',end_date=dt.datetime.today()):
        """
        分析主力合约列表
        参数:
        - rank: rank=0表示获取主力合约，=1表示次主力合约 =2 表示次次主力合约
        - start_date: 开始时间
        - end_date: 结束时间
        返回:
        - 主力合约价格
        """
        # 获取主力连续合约列表
        dominant_contract = self.get_dominant_contract(rank,start_date,end_date)
        column = dominant_contract.columns
        dominant_contract_price = pd.DataFrame(index=dominant_contract.index,columns=[column[0]])
        for date, row in dominant_contract.iterrows():
            dominant_contract_price.loc[date,column[0]] = self.dominant_contract_close.loc[date,row[column[0]]]
        return dominant_contract_price

    def get_low_high_price(self,rank=0):
        """
        获取主力合约的最高最低价
        参数:
        - rank: rank=0表示获取主力合约，=1表示次主力合约 =2 表示次次主力合约
        - start_date: 开始时间
        - end_date: 结束时间
        返回:
        - 主力合约的最高最低价
        """
        # 获取主力连续合约列表
        dominant_contract = self.get_dominant_contract(rank)
        dominant_contract['main'] = self.future_symbol + dominant_contract['main']
        price_data = pd.DataFrame()
        # 获取主力连续合约价格数据
        for contract in sorted(set(dominant_contract['main'])):
            min_date = min(dominant_contract[dominant_contract['main'] == contract].index.to_list())
            max_date = max(dominant_contract[dominant_contract['main'] == contract].index.to_list())
            price_data = pd.concat(
                [price_data, get_price(contract, min_date, max_date, fields=['low', 'high', 'open', 'close'])], axis=0)
        price_data = price_data.reset_index().set_index(['date'])
        return price_data
