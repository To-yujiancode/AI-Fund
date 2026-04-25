import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
import time
from typing import Optional, List

# 设置全局字体（解决中文显示问题）
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']  # 微软雅黑
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题

# 设置显示选项（对齐关键参数）
pd.set_option('display.max_columns', None)  # 显示所有列
pd.set_option('display.max_colwidth', 20)  # 单列最大宽度（避免过长文本截断）
pd.set_option('display.width', 1000)       # 总输出宽度（根据终端调整）
pd.set_option('display.unicode.ambiguous_as_wide', True)  # 中文字符对齐
pd.set_option('display.unicode.east_asian_width', True)   # 处理中文宽度

print("the version of AKShare :", ak.__version__)  # 查看版本
print("all function :", dir(ak))  # 查看所有可用的函数和属性

def get_all_fund_list(
        fund_type: Optional[str] = None,
        save_to_excel: bool = False,
        save_path: str = "fund_list.xlsx"
) -> pd.DataFrame:
    """
    适配AKShare 1.18.19版本：获取全量基金列表（含名称、代码、类型、基金公司等）

    参数说明：
    --------
    fund_type : str, 可选
        筛选的基金类型（模糊匹配），如"ETF"、"货币"、"股票型"，None返回全量
    save_to_excel : bool, 可选
        是否保存到Excel，默认False
    save_path : str, 可选
        保存路径，默认"fund_list.xlsx"

    返回值：
    -------
    pd.DataFrame
        包含基金代码、基金名称、基金类型、基金公司等字段的DataFrame
    """

    try:
        print(f"AKShare版本：{ak.__version__}，正在获取全量基金列表...")

        # 1. 1.18.19版本核心接口：获取全量公募基金基本信息
        # 接口返回字段：基金代码、基金名称、基金简称、基金类型、基金公司、基金经理等
        fund_df = ak.fund_open_fund_info_em()

        # 2. 数据清洗：确保核心字段存在
        core_columns = ["基金代码", "基金名称", "基金类型", "基金公司"]
        # 补充缺失字段（防止接口返回字段名变动）
        for col in core_columns:
            if col not in fund_df.columns:
                fund_df[col] = "未知"

        # 3. 按类型筛选基金
        if fund_type is not None:
            fund_df = fund_df[
                fund_df["基金类型"].str.contains(fund_type, na=False, case=False)
            ]
            print(f"✅ 筛选出[{fund_type}]类型基金共 {len(fund_df)} 只")
        else:
            print(f"✅ 获取到全量基金共 {len(fund_df)} 只")

        # 4. 保存到Excel（可选）
        if save_to_excel:
            # 只保存核心字段（避免冗余）
            save_df = fund_df[core_columns].drop_duplicates(subset=["基金代码"])
            save_df.to_excel(save_path, index=False)
            print(f"✅ 基金列表已保存到：{save_path}")

        # 返回核心字段的DataFrame（去重）
        return fund_df[core_columns].drop_duplicates(subset=["基金代码"])

    except Exception as e:
        print(f"❌ 获取基金列表失败：{str(e)}")
        # 备用方案：使用开放式基金列表接口兜底
        try:
            print("🔄 尝试备用接口获取...")
            fund_df = ak.fund_em_open_fund_info_df()
            # 字段统一
            fund_df.rename(
                columns={"代码": "基金代码", "名称": "基金名称", "类型": "基金类型"},
                inplace=True
            )
            return fund_df[["基金代码", "基金名称", "基金类型"]].drop_duplicates()
        except:
            print("❌ 备用接口也失败，请检查网络或升级AKShare")
            return pd.DataFrame()


get_all_fund_list()

#基金代码
fund_code = "015916"
#fund_code = "022365" #永赢科技智选混合
#fund_code = "SZ300346" #南大光电
#fund_name_em_df = ak.fund_name_em() #所有基金基本信息数据
#print(fund_name_em_df)


#stock_zh_kcb_daily_df = ak.stock_zh_kcb_daily(symbol=fund_code, adjust="hfq")
#print(stock_zh_kcb_daily_df)

fund_individual_basic_info_xq_df = ak.fund_individual_basic_info_xq(symbol=fund_code) #查询单只基金970214
print(fund_individual_basic_info_xq_df)
print("*********************基金基础信息********************")

#fund_open_fund_rank_em_df = ak.fund_open_fund_rank_em(symbol="全部") #开发基金排行榜
#print(fund_open_fund_rank_em_df)

#基金数据分析
#fund_individual_analysis_xq_df = ak.fund_individual_analysis_xq(symbol=fund_code)
#print(fund_individual_analysis_xq_df)
print("*********************基金数据分析********************")
#基金业绩-雪球
fund_individual_achievement_xq_df = ak.fund_individual_achievement_xq(symbol=fund_code)
print(fund_individual_achievement_xq_df)
print("*********************基金业绩********************")
#基金盈利概率
#fund_individual_profit_probability_xq_df = ak.fund_individual_profit_probability_xq(symbol=fund_code)
#print(fund_individual_profit_probability_xq_df)
print("*********************基金基赢利概率********************")
#基金经理
#fund_manager_em_df = ak.fund_manager_em()
#print(fund_manager_em_df)

#新发基金
#fund_new_found_em_df = ak.fund_new_found_em()
#print(fund_new_found_em_df)

#基金评级汇总
#fund_rating_all_df = ak.fund_rating_all()
#print(fund_rating_all_df)

#单位净值走势
fund_open_fund_info_em_df = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")
print(fund_open_fund_info_em_df)
print("*********************基金净值********************")
# 2. 数据预处理
# 转换日期格式并排序
fund_open_fund_info_em_df['净值日期'] = pd.to_datetime(fund_open_fund_info_em_df['净值日期'])
fund_open_fund_info_em_df = fund_open_fund_info_em_df.sort_values('净值日期')

# 3. 绘制净值走势图
plt.figure(figsize=(12, 6))
plt.plot(fund_open_fund_info_em_df['净值日期'],
         fund_open_fund_info_em_df['单位净值'],
         label=f'{fund_code} 单位净值',
         color='blue',
         linewidth=2)

# 4. 添加图表元素
plt.title(f'基金{fund_code}单位净值走势', fontsize=15, pad=20)
plt.xlabel('日期', fontsize=12)
plt.ylabel('单位净值', fontsize=12)
plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=12)

# 5. 自动调整日期显示
plt.gcf().autofmt_xdate()

# 6. 显示图表
plt.tight_layout()
plt.show()