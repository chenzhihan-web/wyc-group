from pathlib import Path
import os
import matplotlib
import pandas as pd
from tqsdk import TqApi, TqAuth
import tkinter as tk
from tkinter import ttk
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from datetime import datetime, timedelta, time
plt.rcParams["font.sans-serif"] = ["SimHei"]
plt.rcParams["axes.unicode_minus"] = False
matplotlib.use('Agg')
class StrategyMonitorGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("期货策略监控系统")
        
        # 初始化天勤API
        self.api = TqApi(auth=TqAuth("lingzzz", "a37429855"))
        
        # 配置界面布局
        self.current_dir = Path(__file__).parent
        self.strategy_frames = {}
        self.current_year = datetime.now().year
        self.current_month = datetime.now().month
        
        # 创建主容器
        self.main_frame = ttk.Frame(root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 创建Notebook（标签页容器）
        self.notebook = ttk.Notebook(self.main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        
        # 创建图表框架
        self.chart_frame = ttk.Frame(root)
        self.chart_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # 初始化图表
        self.fig, self.ax = plt.subplots(figsize=(8, 4))
        self.canvas = FigureCanvasTkAgg(self.fig, master=self.chart_frame)
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        
        # 生成策略列表
        self.strategy_dirs = self.generate_strategy_dirs()
        self.create_strategy_tabs()
        
        # 启动数据更新循环
        self.update_data()
    
    def generate_strategy_dirs(self):
        months = []
        year = self.current_year
        month = self.current_month + 1
        for _ in range(12):
            if month > 12:
                year += 1
                month = 1
            months.append(f"{year%100:02d}{month:02d}")
            month += 1
        
        valid_dirs = []
        for m in months:
            dir_name = f"pr{m}strategy"
            profit_path = self.current_dir / dir_name / "profit.csv"
            position_path = self.current_dir / dir_name / "position.csv"
            if profit_path.exists() and position_path.exists():
                valid_dirs.append(dir_name)
        return valid_dirs
    
    def create_strategy_tabs(self):
        for strategy_dir in self.strategy_dirs:
            tab_frame = ttk.Frame(self.notebook)
            self.notebook.add(tab_frame, text=strategy_dir)
            
            # 创建滚动容器
            container = ttk.Frame(tab_frame)
            container.pack(fill=tk.BOTH, expand=True)
            
            # 创建带滚动条的Canvas
            canvas = tk.Canvas(container)
            scrollbar = ttk.Scrollbar(container, orient="vertical", command=canvas.yview)
            scrollable_frame = ttk.Frame(canvas)
            
            scrollable_frame.bind(
                "<Configure>",
                lambda e: canvas.configure(
                    scrollregion=canvas.bbox("all")
                )
            )
            
            # 绑定鼠标滚轮事件
            def _on_mousewheel(event):
                canvas.yview_scroll(int(-1*(event.delta/120)), "units")
            
            canvas.bind_all("<MouseWheel>", _on_mousewheel)
            
            canvas.create_window((0, 0), window=scrollable_frame, anchor="nw")
            canvas.configure(yscrollcommand=scrollbar.set)
            
            canvas.pack(side="left", fill="both", expand=True)
            scrollbar.pack(side="right", fill="y")
            
            # 添加策略信息组件
            components = {'labels': []}
            
            # 时间显示
            time_label = ttk.Label(scrollable_frame, text="最后更新时间：")
            time_label.grid(row=0, column=0, columnspan=3, sticky=tk.W, pady=5)
            components['time'] = time_label
            
            # 三个品种并列显示
            products_frame = ttk.Frame(scrollable_frame)
            products_frame.grid(row=1, column=0, columnspan=3, sticky=tk.EW)
            
            for col, product in enumerate(['pr', 'ta', 'eg']):
                frame = ttk.LabelFrame(products_frame, text=product.upper())
                frame.grid(row=0, column=col, padx=5, pady=5, sticky=tk.NSEW)
                
                # 持仓信息
                ttk.Label(frame, text="多头持仓:").grid(row=0, column=0, sticky=tk.W)
                long_label = ttk.Label(frame, text="0")
                long_label.grid(row=0, column=1, sticky=tk.W)
                
                ttk.Label(frame, text="空头持仓:").grid(row=1, column=0, sticky=tk.W)
                short_label = ttk.Label(frame, text="0")
                short_label.grid(row=1, column=1, sticky=tk.W)
                
                ttk.Label(frame, text="当前价格:").grid(row=2, column=0, sticky=tk.W)
                price_label = ttk.Label(frame, text="0.00")
                price_label.grid(row=2, column=1, sticky=tk.W)
                
                ttk.Label(frame, text="平均价格:").grid(row=3, column=0, sticky=tk.W)
                avg_price_label = ttk.Label(frame, text="0.00")
                avg_price_label.grid(row=3, column=1, sticky=tk.W)
                
                components.update({
                    f"{product}_long": long_label,
                    f"{product}_short": short_label,
                    f"{product}_price": price_label,
                    f"{product}_avg_price": avg_price_label
                })
            
            # 盈亏信息
            profit_frame = ttk.LabelFrame(scrollable_frame, text="盈亏信息")
            profit_frame.grid(row=2, column=0, columnspan=3, sticky=tk.EW, padx=5, pady=5)
            
            labels = [
                ("总吨数", "total_weight"),
                ("总盈亏", "total_profit"),
                ("浮动盈亏", "float_profit"),
                ("平仓盈亏", "close_profit"),
                ("今日浮动盈亏", "today_float_profit"),
                ("今日平仓盈亏", "today_close_profit"),
                ("合约情况", "describe"),
            ]
            
            for i, (text, key) in enumerate(labels):
                if key == "describe": 
                    row = len(labels) // 2 
                    ttk.Label(profit_frame, text=text+":").grid(row=row, column=0, sticky=tk.W)
                    label = ttk.Label(profit_frame, text="0.00")
                    label.grid(row=row, column=1, columnspan=3, sticky=tk.W) 
                    components[key] = label
                else:
                    row = i // 2
                    col = (i % 2) * 2
                    ttk.Label(profit_frame, text=text+":").grid(row=row, column=col, sticky=tk.W)
                    label = ttk.Label(profit_frame, text="0.00")
                    label.grid(row=row, column=col+1, sticky=tk.W)
                    components[key] = label
            
            self.strategy_frames[strategy_dir] = components
    
    def update_data(self):
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        try:
            None
            #self.api.wait_update(deadline)
        except Exception as e:
            print("API更新异常:", e)

        for strategy_dir in self.strategy_dirs:
            month = strategy_dir[2:-8]
            product_codes = {
                'pr': f"CZCE.PR{month[-3:]}",
                'ta': f"CZCE.TA{month[-3:]}",
                'eg': f"DCE.eg{month}"
            }
            try:
                # 读取持仓和收益数据
                position_path = self.current_dir / strategy_dir / "position.csv"
                profit_path = self.current_dir / strategy_dir / "profit.csv"
                
                position = pd.read_csv(position_path).iloc[-1]
                profit = pd.read_csv(profit_path).iloc[-1]
                
                # 获取实时报价
                last_prices = {
                    product: self.api.get_quote(code).last_price
                    for product, code in product_codes.items()
                }
                
                opens = {
                    product: self.api.get_quote(code).open
                    for product, code in product_codes.items()
                }
                quotes = {
                    product: self.api.get_quote(code)
                    for product, code in product_codes.items()
                }
                # 计算浮动盈亏
                float_profit = 0
                today_float_profit = 0
                for product in ['pr', 'ta', 'eg']:
                    avg_price = profit[f"{product}_avg_price"]
                    long = position[f"{product}_long"]
                    short = position[f"{product}_short"]
                    price = last_prices[product]
                    open_price = opens[product]
                    today_float_profit += (price - open_price) * long + (open_price - price) * short
                    float_profit += (price - avg_price) * long + (avg_price - price) * short

                total_weight = (position["pr_long"] + position["pr_short"]) * 15
                total_profit = float_profit + profit["total_close_profit"]
                
                # 更新界面组件
                components = self.strategy_frames[strategy_dir]
                components['time'].config(text=f"最后更新时间：{current_time}")
                
                # 更新颜色函数
                def set_color(label, value):
                    if value > 0:
                        label.config(foreground="red")
                    elif value < 0:
                        label.config(foreground="green")
                    else:
                        label.config(foreground="black")
                
                # 更新持仓信息
                for product in ['pr', 'ta', 'eg']:
                    components[f"{product}_long"].config(text=position[f"{product}_long"])
                    components[f"{product}_short"].config(text=position[f"{product}_short"])
                    components[f"{product}_price"].config(text=f"{quotes[product].last_price:.2f}")
                    components[f"{product}_avg_price"].config(text=f"{profit[f'{product}_avg_price']:.2f}")

                # 更新合约情况
                describes = {'pr':'', 'ta':'', 'eg':'', 'tag':1}
                describe = ''
                quotes = {
                    product: self.api.get_quote(code)
                    for product, code in product_codes.items()
                }
                for sym in ['pr', 'ta', 'eg']:
                    if quotes[sym].expire_rest_days < 40:
                        describes[sym] += f'{sym}临近交割 '
                        describes['tag'] = 0
                    if quotes[sym]['open_interest'] < min(10000 * quotes['pr'].volume_multiple / quotes[sym].volume_multiple, 10000):
                        describes[sym] += f'{sym}持仓量{quotes[sym]['open_interest']}过低 '
                        describes['tag'] = 0
                    if (quotes[sym]['last_price'] - quotes[sym]['lower_limit']) / quotes[sym]['lower_limit'] < 0.01 :
                        describes[sym] += f'{sym}接近跌停 '
                        describes['tag'] = 0
                    if (quotes[sym]['upper_limit'] - quotes[sym]['last_price']) / quotes[sym]['upper_limit'] < 0.01 :
                        describes[sym] += f'{sym}接近涨停 '
                        describes['tag'] = 0
                    describe += describes[sym]
                if describes['tag'] == 1:
                    describe += '合约正常'
                
                # 更新盈亏信息并设置颜色
                set_color(components['total_profit'], total_profit)
                components['total_weight'].config(text=f"{total_weight:.2f}")
                components['total_profit'].config(text=f"{total_profit:.2f}")
                
                set_color(components['float_profit'], float_profit)
                components['float_profit'].config(text=f"{float_profit:.2f}")
                
                set_color(components['close_profit'], profit['total_close_profit'])
                components['close_profit'].config(text=f"{profit['total_close_profit']:.2f}")
                
                set_color(components['today_float_profit'], today_float_profit)
                components['today_float_profit'].config(text=f"{today_float_profit:.2f}")

                set_color(components['today_close_profit'], profit['today_close_profit'])
                components['today_close_profit'].config(text=f"{profit['today_close_profit']:.2f}")

                components['describe'].config(text=describe)
                
                # 更新图表
                if self.notebook.select() == self.notebook.tabs()[self.strategy_dirs.index(strategy_dir)]:
                    self.update_chart(strategy_dir)
                    
            except Exception as e:
                print(f"更新策略 {strategy_dir} 时出错:", e)

        self.root.after(1000, self.update_data)
    
    def update_chart(self, strategy_dir):
        try:
            profit_path = self.current_dir / strategy_dir / "profit.csv"
            df = pd.read_csv(profit_path)
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            # 创建辅助列
            df['date'] = df['timestamp'].dt.date  # 提取日期用于分组
            df['target'] = df['date'].astype(str) + ' 17:00:00'  # 构造目标时间字符串
            df['target'] = pd.to_datetime(df['target'])  # 转换为datetime

            # 计算时间差绝对值,按日期分组并找到最接近的索引
            df['time_diff'] = (df['timestamp'] - df['target']).abs()
            idx = df.groupby('date')['time_diff'].idxmin()
            df = df.loc[idx].drop(columns=['target', 'time_diff'])
            date = []
            total_profit = []
            for i in range(len(df)):
                dt = df.iloc[i]['timestamp']
                if dt.time() >= time(16, 0):
                        if dt.weekday() == 4:  
                            dt = dt.date() + timedelta(days=3)
                        else:  
                            dt = dt.date() + timedelta(days=1)
                date.append(dt.strftime('%m-%d'))
                total_profit.append(float(df.iloc[i]['total_profit']))

            self.ax.clear()
            self.ax.plot(date, total_profit, label='总收益')
            self.ax.set_title(f"{strategy_dir} 收益曲线")
            self.ax.set_xlabel("时间")
            self.ax.set_ylabel("收益")
            self.ax.grid(True)
            self.ax.legend()
            self.fig.tight_layout()
            self.canvas.draw()
        except Exception as e:
            print("更新图表时出错:", e)

if __name__ == "__main__":
    root = tk.Tk()
    app = StrategyMonitorGUI(root)
    root.geometry("800x600")
    root.mainloop()
    
    