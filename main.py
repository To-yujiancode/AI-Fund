import yfinance as yf
import os
import matplotlib.pyplot as plt

proxy = 'http://127.0.0.1:7897' # 注意这里的端口要和科学上网的端口一致
os.environ['HTTP_PROXY'] = proxy
os.environ['HTTPS_PROXY'] = proxy

print("start to get fund date, need modify port ...")
# 美股的 SPY（标普 500 ETF）、QQQ（纳斯达克 ETF）、VOO（先锋标普 500 ETF），A 股相关的纳指 ETF（如 513100）、恒生科技 ETF（如 513180）等。
# 获取基金数据
#fund_name = "ASHR"  # 德银沪深300ETF
fund_name = "SPY" #标普500
#fund_name = "VOO" #先锋标普 500 ETF
fund_name = "513100.ss" #上交所加.ss/深交所加.sz

# 1. 获取德银沪深300ETF (ASHR) 的1年历史数据
# ticker = "ASHR"  # 基金代码
ticker = fund_name

data = yf.Ticker(ticker).history(period="20y")  # 获取1年数据

# 2. 检查数据（可选）
print(data.head())  # 查看前5行数据
print(data.tail())  # 查看后5行数据

# 3. 绘制收盘价走势图
plt.figure(figsize=(12, 6))  # 设置图表大小
plt.plot(data.index, data["Close"], label=f"{ticker} Close Price", color="blue")

# 4. 添加图表标题和标签
plt.title(f"{ticker} (Deutsche X-trackers Harvest CSI 300 ETF) - 1 Year Price", fontsize=14)
plt.xlabel("Date", fontsize=12)
plt.ylabel("Price (USD)", fontsize=12)
plt.legend()  # 显示图例
plt.grid(True)  # 显示网格

# 5. 自动调整日期显示格式
plt.gcf().autofmt_xdate()  # 避免日期重叠

# 6. 显示图表
plt.show()



# 使用指南,需要激活环境.可以获取国外基金的数据。
# conda env list
# conda activate AI-Fund
# 需要科学上网的端口proxy。