import pandas as pd
import os
from pathlib import Path
from datetime import datetime

def merge_trade(tradePath) -> int:
    """
    处理单个交易文件，返回符合要求的交易组
    参数说明：
    - file_name: 相对logs目录的文件路径（如 "pr/250507_trade.csv"）
    返回值示例：
    - {trade_id: pd.DataFrame, ...}
    """
    
    # 返回值容器
    result = {}
    merged_rows = []
    try:
        df = pd.read_csv(tradePath)
        valid_df = df[df['flag'] == 1].copy()
        groups = valid_df.groupby('trade_id')
        result = {}
        for trade_id, group in groups:
            try:
                if(len(group) == 3):
                    df.loc[df['trade_id'] == trade_id, 'flag'] = 0
                    result[trade_id] = group
                    continue
                if(len(group) == 6):
                    close_group = group.loc[(group['offset'] == 'CLOSE')]
                    open_group = group.loc[(group['offset'] == 'OPEN')]
                    if len(close_group) == 3 and len(open_group) == 3:
                            df.loc[df['trade_id'] == trade_id, 'flag'] = 0
                            result[f"{trade_id}_close"] = close_group
                            result[f"{trade_id}_open"] = open_group
                            continue
                raise ValueError(f"无效交易组: trade_id={trade_id}, 记录数={len(group)}")

            except Exception as e:
                print(f"处理交易组失败 {trade_id}: {str(e)}")
                continue
        df.to_csv(tradePath, index=False)
    except Exception as e:
        print(f"处理文件失败 {tradePath}: {str(e)}")

    for trade_id, group in result.items():
        pr_row = group[group['action'].isin(['BUY', 'SELL']) & 
                        (group['contract'].str.contains('pr', case=False))].iloc[0]
        ta_row = group[group['action'].isin(['BUY', 'SELL']) & 
                    (group['contract'].str.contains('ta', case=False))].iloc[0]
        eg_row = group[group['action'].isin(['BUY', 'SELL']) & 
                    (group['contract'].str.contains('eg', case=False))].iloc[0]
        
        # 提取共同信息
        timestamp = group['timestamp'].iloc[0]
        direction = group['action'].iloc[0]
        offset = group['offset'].iloc[0]
        fee_target = float(f"{(pr_row['quote'] - ta_row['quote'] * 0.857 -eg_row['quote'] * 0.335):.2f}")
        fee_actually = float(f"{(pr_row['price'] - ta_row['price'] * 0.857 -eg_row['price'] * 0.335):.2f}")
        commission = float(f"{(pr_row['commission'] + ta_row['commission'] + eg_row['commission']):.2f}")
        if direction == "SELL":
            slippage = fee_target - fee_actually
        else:
            slippage = fee_actually - fee_target
        slippage = float(f"{slippage:.2f}")
            

        # 创建合并后的行
        merged_row = {
            'trade_id': trade_id,
            'timestamp': timestamp,
            'direction': direction,
            'offset': offset,
            'fee_target': fee_target,
            'fee_actually': fee_actually,
            'slippage': slippage,
            'commission': commission,
            # PR信息
            'pr_quote': pr_row['quote'],
            'pr_price': pr_row['price'],
            'pr_volume': pr_row['volume'],
            #'pr_commission': float(f"{(pr_row['commission']):.2f}"),
            
            # TA信息
            'ta_quote': ta_row['quote'],
            'ta_price': ta_row['price'],
            'ta_volume': ta_row['volume'],
            #'ta_commission': float(f"{(ta_row['commission']):.2f}"),
            
            # EG信息
            'eg_quote': eg_row['quote'],
            'eg_price': eg_row['price'],
            'eg_volume': eg_row['volume'],
            #'eg_commission': float(f"{(eg_row['commission']):.2f}"),

            # 订单对应平仓信息
            'flag': 0,
            'pr_left': pr_row['volume'],
            'ta_left': ta_row['volume'],
            'eg_left': eg_row['volume'],
            'profit': 0,
            'marked_trade_id': None,
        }
        merged_rows.append(merged_row)

    merged_df = pd.DataFrame(merged_rows)
    output_file = os.path.join(os.path.dirname(tradePath), "merged_data.csv")
    file_exists = os.path.exists(output_file) and os.path.getsize(output_file) > 0

    merged_df.to_csv(
        output_file,
        mode='a' if file_exists else 'w',  # 追加/写入模式自动切换
        header=not file_exists,            # 文件存在时不写表头
        index=False,                       # 不保存索引
        float_format='%.2f'                # 统一保留两位小数
    )

    print(f"处理完成，保存{len(merged_rows)}条数据到 merged_trades.csv")   

    return merged_df

def process_trades(mergedPath, profitPath) -> int :
    """处理交易记录"""
    profit_rows = []
    df = pd.read_csv(mergedPath)
    process_df = df[df['flag'] == 0].copy().sort_values(by='timestamp')
    profitDict = {
        'today_close_profit':0, 
        'history_close_profit': 0,
        'total_close_profit':0,
        'pr': {'long':0, 'short':0, 'avg_price':0},
        'ta': {'long':0, 'short':0, 'avg_price':0},
        'eg': {'long':0, 'short':0, 'avg_price':0}
    }
    if os.path.exists(profitPath):
        profitDf = pd.read_csv(profitPath)
        if not profitDf.empty:
            last = profitDf.iloc[-1]
            profitDict.update({
                'today_close_profit': last['today_close_profit'], 
                'history_close_profit': last['history_close_profit'],
                'total_close_profit': last['total_close_profit'],
                'pr': {'long':last['pr_long'], 'short':last['pr_short'], 'avg_price':last['pr_avg_price']},
                'ta': {'long':last['ta_long'], 'short':last['ta_short'], 'avg_price':last['ta_avg_price']},
                'eg': {'long':last['eg_long'], 'short':last['eg_short'], 'avg_price':last['eg_avg_price']}
            })
    
    for idx, row in process_df.iterrows():
        offset = row['offset']
        direction = row['direction']
        tradeDict = {
            'pr': {'volume':row['pr_volume'], 'price':row['pr_price'], 'remaining':row['pr_left']},
            'ta': {'volume':row['ta_volume'], 'price':row['ta_price'], 'remaining':row['ta_left']},
            'eg': {'volume':row['eg_volume'], 'price':row['eg_price'], 'remaining':row['eg_left']}
        }
        totalProfit = 0
        
        if offset == 'OPEN':
            for sym in ['pr', 'ta', 'eg']:
                if (direction == 'BUY' and sym  == 'pr') or (direction == 'SELL' and (sym  == 'ta' or sym  == 'eg')):
                    dire = 'long'
                else:
                    dire = 'short'
                
                total_value = profitDict[sym]['avg_price'] * profitDict[sym][dire] + tradeDict[sym]['volume'] * tradeDict[sym]['price']
                volume = profitDict[sym][dire] + tradeDict[sym]['volume']
                
                avg_price = total_value / volume
                profitDict[sym].update(
                        {dire:volume, 'avg_price':avg_price}
                    )

        if offset == 'CLOSE':
            totalProfitDict = {
                'pr': 0,
                'ta': 0,
                'eg': 0
            }
            
            if direction == 'BUY':
                open_rocords = df[
                    (df['timestamp'] < row['timestamp']) &
                    (df['direction'] == 'SELL') &
                    (df['offset'] == 'OPEN') &
                    (df['flag'] == 0)
                ].sort_values('timestamp', ascending=False)
                tag =  -1

            else:
                open_rocords = df[
                    (df['timestamp'] < row['timestamp']) &
                    (df['direction'] == 'BUY') &
                    (df['offset'] == 'OPEN') &
                    (df['flag'] == 0)
                ].sort_values('timestamp', ascending=False)
                tag =  1

            for index, open_record in open_rocords.iterrows():
                if tradeDict['pr']['remaining'] > 0:
                    if tradeDict['pr']['remaining'] >= open_record['pr_left']:
                        for sym in ['pr', 'ta', 'eg']:
                            tradeDict[sym]['remaining'] -= open_record[f'{sym}_left']
                            df.loc[df['trade_id'] == open_record['trade_id'], f'{sym}_left'] = 0
                            totalProfitDict[sym] += open_record[f'{sym}_price'] * open_record[f'{sym}_left']
                        profit = ((row['pr_price'] - open_record['pr_price']) * open_record['pr_volume'] + 
                                    (- row['ta_price'] + open_record['ta_price']) * open_record['ta_volume'] + 
                                    (- row['eg_price'] + open_record['eg_price']) * open_record['eg_volume']) * tag
                        df.loc[df['trade_id'] == open_record['trade_id'], 'flag'] = 1            

                    else:
                        for sym in ['pr', 'ta', 'eg']:
                            df.loc[df['trade_id'] == open_record['trade_id'], f'{sym}_left'] -= tradeDict[sym]['remaining']
                            totalProfitDict[sym] += open_record[f'{sym}_price'] * tradeDict[sym]['remaining']
                        profit = ((row['pr_price'] - open_record['pr_price']) * open_record['pr_volume'] + 
                                    (- row['ta_price'] + open_record['ta_price']) * open_record['ta_volume'] + 
                                    (- row['eg_price'] + open_record['eg_price']) * open_record['eg_volume']) * tag
                    for sym in ['pr', 'ta', 'eg']:
                        df.loc[df['trade_id'] == row['trade_id'], f'{sym}_left'] = 0
                    df.loc[df['trade_id'] == row['trade_id'], 'flag'] = 1
                    df.loc[df['trade_id'] == open_record['trade_id'], 'profit'] += profit
                    df.loc[df['trade_id'] == row['trade_id'], 'profit'] += profit
                    totalProfit += profit
                    #df.loc[df['trade_id'] == open_record['trade_id'], 'marked_trade_id'] += row['trade_id']
                    #df.loc[df['trade_id'] == row['trade_id'], 'marked_trade_id'] += open
            for sym in ['pr', 'ta', 'eg']:
                if (direction == 'BUY' and sym  == 'pr') or (direction == 'SELL' and (sym  == 'ta' or sym  == 'eg')):
                    dire = 'short'
                else:
                    dire = 'long'
                
                total_value = profitDict[sym]['avg_price'] * profitDict[sym][dire] - totalProfitDict[sym]
                volume = profitDict[sym][dire] - tradeDict[sym]['volume']
                if  volume > 0:
                    avg_price = total_value / volume
                    profitDict[sym].update(
                            {dire:volume, 'avg_price':avg_price}
                        )
                else:
                    profitDict[sym].update(
                        {dire:0, 'avg_price':0}
                    )
        profitDict['today_close_profit'] += totalProfit
        profitDict['total_close_profit'] += totalProfit
        profit_row = {
            'timestamp': row['timestamp'],
            'total_profit':None, # 策略的累计盈亏：策略的浮动盈亏 + 策略的累计平仓盈亏
            'float_profit':None, # 策略的浮动盈亏：（现价 - 开仓均价）* 持仓量
            'today_close_profit':profitDict['today_close_profit'], # 策略的今日平仓盈亏： sum（今日（平仓价 - 对应订单开仓价）* 平仓量）
            'history_close_profit':profitDict['history_close_profit'], # 策略的历史平仓盈亏： 策略昨日的累计平仓盈亏
            'total_close_profit':profitDict['total_close_profit'], # 策略的累计平仓盈亏：策略的今日平仓盈亏 + 策略的历史平仓盈亏
            'pr_long': profitDict['pr']['long'],
            'pr_short': profitDict['pr']['short'],
            'pr_avg_price': profitDict['pr']['avg_price'],
            'ta_long': profitDict['ta']['long'],
            'ta_short': profitDict['ta']['short'],
            'ta_avg_price': profitDict['ta']['avg_price'],
            'eg_long': profitDict['eg']['long'],
            'eg_short': profitDict['eg']['short'],
            'eg_avg_price': profitDict['eg']['avg_price']
        }
        profit_rows.append(profit_row)
    df.to_csv(mergedPath,index=False)
    file_exists = os.path.exists(profitPath) and os.path.getsize(profitPath) > 0
    profit_df = pd.DataFrame(profit_rows)
    profit_df.to_csv(
        profitPath,
        mode='a' if file_exists else 'w',  # 追加/写入模式自动切换
        header=not file_exists,            # 文件存在时不写表头
        index=False,                       # 不保存索引
        float_format='%.2f'                # 统一保留两位小数
    )

    print(f"处理完成，保存{len(profit_rows)}条数据到 merged_trades.csv")   
    return len(profit_rows)

if __name__ == "__main__":
    log_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent / "logs"
    trade_files = log_dir.glob("*.csv")
    total_rows = 0
    for trade_file in trade_files:
        total_rows += merge_trade(trade_file)
    print(f"处理完成，总共处理了{total_rows}条数据")