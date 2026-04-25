"""
基金数据获取模块
使用 akshare 从东方财富获取基金基本信息、历史净值和 K线(OHLC)数据，存入 SQLite。
支持增量下载、断点续传、进度显示、并发下载。
"""

import time
import sys
import signal
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import akshare as ak
import pandas as pd

from database import (
    init_db, batch_upsert_fund_basic, upsert_fund_basic,
    save_fund_nav, save_fund_ohlc, update_fund_category,
    get_nav_count, get_nav_date_range, get_latest_nav_date,
    get_ohlc_count, get_ohlc_date_range, get_latest_ohlc_date,
    get_fund_count_by_type, get_funds_with_nav_count,
    get_etf_lof_codes, get_fund_detail, get_connection,
    close_connection
)

# 退出标志
_exit_flag = False


def _handle_exit(signum, frame):
    global _exit_flag
    print("\n[数据获取] 收到退出信号，正在安全终止...")
    _exit_flag = True


def setup_exit_handler():
    """设置优雅退出处理。"""
    signal.signal(signal.SIGINT, _handle_exit)
    signal.signal(signal.SIGTERM, _handle_exit)


# ==================== 基金基本信息获取 ====================

def fetch_and_save_all_fund_basic():
    """从东方财富获取所有基金基本信息并存入数据库。"""
    print("[数据获取] 正在获取基金列表，请稍候...")
    try:
        df = ak.fund_name_em()
    except Exception as e:
        print(f"[错误] 获取基金列表失败: {e}")
        return False

    if df is None or df.empty:
        print("[错误] 获取到的基金列表为空")
        return False

    print(f"[数据获取] 共获取到 {len(df)} 只基金")

    records = []
    for _, row in df.iterrows():
        records.append({
            'code': str(row['基金代码']).zfill(6),
            'name': str(row['基金简称']),
            'py_abbr': str(row.get('拼音缩写', '')),
            'fund_type': str(row.get('基金类型', '')),
            'py_full': str(row.get('拼音全称', '')),
        })

    batch_upsert_fund_basic(records)
    print(f"[数据获取] 基金基本信息已保存到数据库（共 {len(records)} 条）")

    # 打印类型统计
    types = get_fund_count_by_type()
    print(f"\n[数据获取] 基金类型分布（Top 10）：")
    for fund_type, count in types[:10]:
        print(f"  {fund_type}: {count}")
    if len(types) > 10:
        print(f"  ... 以及其他 {len(types) - 10} 种类型")

    return True


def fetch_and_save_fund_detail(code: str) -> bool:
    """获取单只基金的详细信息并存入数据库。"""
    try:
        df = ak.fund_individual_basic_info_xq(symbol=code)
    except Exception as e:
        print(f"[警告] 获取基金 {code} 详细信息失败: {e}")
        return False

    if df is None or df.empty:
        return False

    info = {}
    for _, row in df.iterrows():
        val = str(row['value'])
        if val in ('nan', '<NA>', 'None', ''):
            val = ''
        info[str(row['item'])] = val

    upsert_fund_basic(
        code=code,
        name=info.get('基金名称', ''),
        fund_type=info.get('基金类型', ''),
        full_name=info.get('基金全称', ''),
        found_date=info.get('成立时间', ''),
        latest_scale=info.get('最新规模', ''),
        company=info.get('基金公司', ''),
        manager=info.get('基金经理', ''),
        trustee=info.get('托管银行', ''),
        rating=info.get('基金评级', ''),
        strategy=info.get('投资策略', ''),
        objective=info.get('投资目标', ''),
        benchmark=info.get('业绩比较基准', ''),
        detail_fetched=1
    )
    return True


# ==================== 基金历史净值获取（增量） ====================

def _fetch_single_fund_nav_incremental(code: str) -> tuple[str, int, bool]:
    """
    增量获取单只基金的历史净值数据。
    逻辑：
    - 如果数据库中没有该基金数据 → 全量下载
    - 如果数据库中最新日期距今 <= 2 天 → 跳过（已是最新）
    - 否则 → 重新下载全部数据，用 INSERT OR IGNORE 去重
      （fund_open_fund_info_em 不支持日期范围参数，只能全量拉取后去重）

    返回: (code, new_records_count, success)
    """
    global _exit_flag
    if _exit_flag:
        return (code, 0, False)

    # 增量判断：检查是否已是最新
    latest_date = get_latest_nav_date(code)
    if latest_date:
        try:
            last_dt = datetime.strptime(latest_date, '%Y-%m-%d')
            now_dt = datetime.now()
            # 如果最新数据距今不超过 2 天，跳过
            if (now_dt - last_dt).days <= 2:
                return (code, 0, True)  # 已是最新，跳过
        except ValueError:
            pass

    # 下载全量数据
    try:
        df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
    except Exception as e:
        err_str = str(e)
        if '货币' in err_str or '不支持' in err_str or '没有' in err_str:
            return (code, 0, True)
        if 'Connection' in err_str or 'timeout' in err_str or 'aborted' in err_str:
            time.sleep(2)
            try:
                df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
            except Exception as e2:
                print(f"[重试失败] {code}: {e2}")
                return (code, 0, False)
        else:
            return (code, 0, True)

    if df is None or df.empty:
        return (code, 0, True)

    records = []
    for _, row in df.iterrows():
        nav_date = str(row['净值日期'])
        # 增量过滤：如果指定了起始日期，只保留更新的数据
        if latest_date and nav_date <= latest_date:
            continue
        unit_nav = float(row['单位净值']) if pd.notna(row['单位净值']) else None
        daily_chg = row.get('日增长率', None)
        if pd.notna(daily_chg):
            try:
                daily_chg = float(daily_chg)
            except (ValueError, TypeError):
                daily_chg = None
        records.append((code, nav_date, unit_nav, None, daily_chg))

    if records:
        save_fund_nav(records)

    return (code, len(records), True)


# ==================== ETF/LOF K线 (OHLC) 数据获取 ====================

def _fetch_fund_ohlc(code: str, category: str = 'etf') -> tuple[str, int, bool]:
    """
    增量获取 ETF/LOF 的 K线 (OHLC) 数据。
    支持日期范围参数，只下载新增数据。

    返回: (code, new_records_count, success)
    """
    global _exit_flag
    if _exit_flag:
        return (code, 0, False)

    # 增量判断
    latest_date = get_latest_ohlc_date(code)
    if latest_date:
        try:
            last_dt = datetime.strptime(latest_date, '%Y-%m-%d')
            now_dt = datetime.now()
            if (now_dt - last_dt).days <= 2:
                return (code, 0, True)  # 已是最新
            start_date = (last_dt + timedelta(days=1)).strftime('%Y%m%d')
        except ValueError:
            start_date = ''
    else:
        start_date = ''

    end_date = datetime.now().strftime('%Y%m%d')

    # 尝试下载 OHLC 数据
    df = None
    try:
        if category == 'lof':
            df = ak.fund_lof_hist_em(
                symbol=code, period='daily',
                start_date=start_date, end_date=end_date, adjust=''
            )
        else:
            df = ak.fund_etf_hist_em(
                symbol=code, period='daily',
                start_date=start_date, end_date=end_date, adjust=''
            )
    except Exception as e:
        err_str = str(e)
        # 如果 ETF 接口失败，尝试 LOF
        if category == 'etf' and start_date == '':
            try:
                df = ak.fund_lof_hist_em(
                    symbol=code, period='daily',
                    start_date=start_date, end_date=end_date, adjust=''
                )
                if df is not None and not df.empty:
                    category = 'lof'
            except Exception:
                pass
        if df is None or (hasattr(df, 'empty') and df.empty):
            return (code, 0, True)  # 不支持 OHLC 的基金，跳过
        if 'Connection' in err_str or 'timeout' in err_str:
            time.sleep(2)
            try:
                if category == 'lof':
                    df = ak.fund_lof_hist_em(symbol=code, period='daily',
                                             start_date=start_date, end_date=end_date, adjust='')
                else:
                    df = ak.fund_etf_hist_em(symbol=code, period='daily',
                                             start_date=start_date, end_date=end_date, adjust='')
            except Exception as e2:
                return (code, 0, False)

    if df is None or df.empty:
        return (code, 0, True)

    records = []
    for _, row in df.iterrows():
        date_str = str(row['日期'])
        try:
            # 统一日期格式为 YYYY-MM-DD
            if '-' in date_str:
                pass  # 已经是 YYYY-MM-DD
            else:
                date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
        except (ValueError, IndexError):
            continue

        records.append((
            code, date_str,
            float(row['开盘']) if pd.notna(row.get('开盘')) else None,
            float(row['收盘']) if pd.notna(row.get('收盘')) else None,
            float(row['最高']) if pd.notna(row.get('最高')) else None,
            float(row['最低']) if pd.notna(row.get('最低')) else None,
            int(row['成交量']) if pd.notna(row.get('成交量')) else None,
            float(row['成交额']) if pd.notna(row.get('成交额')) else None,
            float(row['振幅']) if pd.notna(row.get('振幅')) else None,
            float(row['涨跌幅']) if pd.notna(row.get('涨跌幅')) else None,
            float(row['涨跌额']) if pd.notna(row.get('涨跌额')) else None,
            float(row['换手率']) if pd.notna(row.get('换手率')) else None,
        ))

    if records:
        save_fund_ohlc(records)
        update_fund_category(code, category)

    return (code, len(records), True)


def fetch_all_fund_history(max_workers: int = 4, skip_existing: bool = True):
    """
    下载所有基金的历史净值数据（增量模式）。
    已有近期数据的基金会自动跳过，只下载新数据。
    """
    global _exit_flag

    conn = get_connection()
    cursor = conn.execute("SELECT code, name FROM fund_basic ORDER BY code")
    funds = cursor.fetchall()
    total = len(funds)

    if total == 0:
        print("[数据获取] 数据库中没有基金基本信息，请先执行：更新基金列表")
        return

    # 全量模式 vs 增量模式
    todo_list = []
    skipped = 0
    for code, name in funds:
        if skip_existing:
            # 增量模式：只下载需要更新的基金
            latest_date = get_latest_nav_date(code)
            if latest_date:
                try:
                    last_dt = datetime.strptime(latest_date, '%Y-%m-%d')
                    if (datetime.now() - last_dt).days <= 2:
                        skipped += 1
                        continue
                except ValueError:
                    pass
        todo_list.append((code, name))

    print(f"\n[数据获取] 基金历史净值增量下载")
    print(f"  总基金数: {total}")
    print(f"  已是最新: {skipped} (将跳过)")
    print(f"  待更新: {len(todo_list)}")
    print(f"  并发线程: {max_workers}")
    print(f"  按 Ctrl+C 可安全退出\n")

    if not todo_list:
        print("[数据获取] 所有基金数据已是最新，无需更新")
        return

    done_count = 0
    success_count = 0
    fail_count = 0
    total_new = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for code, name in todo_list:
            if _exit_flag:
                break
            future = executor.submit(_fetch_single_fund_nav_incremental, code)
            futures[future] = (code, name)

        for future in as_completed(futures):
            if _exit_flag:
                executor.shutdown(wait=False, cancel_futures=True)
                break

            code, name = futures[future]
            done_count += 1

            try:
                _, new_count, success = future.result()
            except Exception as e:
                print(f"[错误] {code} {name}: {e}")
                success = False
                new_count = 0

            if success:
                success_count += 1
                total_new += new_count
            else:
                fail_count += 1

            elapsed = time.time() - start_time
            speed = done_count / elapsed if elapsed > 0 else 0
            eta = (len(todo_list) - done_count) / speed if speed > 0 else 0

            sys.stdout.write(
                f"\r[净值] {done_count}/{len(todo_list)} | "
                f"成功: {success_count} | 失败: {fail_count} | "
                f"新增: {total_new:,} | "
                f"速度: {speed:.1f} 只/秒 | "
                f"剩余: {eta/60:.1f} 分钟"
            )
            sys.stdout.flush()

            if done_count % 100 == 0:
                print()

    elapsed = time.time() - start_time
    print(f"\n\n[数据获取] 净值增量下载完成！")
    print(f"  处理: {done_count} | 成功: {success_count} | 失败: {fail_count}")
    print(f"  新增记录: {total_new:,} | 耗时: {elapsed:.1f} 秒")


def fetch_all_etf_lof_ohlc(max_workers: int = 4):
    """
    下载所有 ETF/LOF 的 K线 (OHLC) 数据（增量模式）。
    ETF/LOF 的 K线数据支持日期范围参数，增量下载效率极高。
    """
    global _exit_flag

    etf_codes, lof_codes = get_etf_lof_codes()
    total = len(etf_codes) + len(lof_codes)

    if total == 0:
        print("[数据获取] 未找到 ETF/LOF 基金，请先更新基金列表")
        return

    # 构建下载任务
    tasks = []  # [(code, category), ...]
    for code in etf_codes:
        tasks.append((code, 'etf'))
    for code in lof_codes:
        if code not in etf_codes:  # 避免重复
            tasks.append((code, 'lof'))

    # 增量过滤
    todo_list = []
    skipped = 0
    for code, cat in tasks:
        latest_date = get_latest_ohlc_date(code)
        if latest_date:
            try:
                last_dt = datetime.strptime(latest_date, '%Y-%m-%d')
                if (datetime.now() - last_dt).days <= 2:
                    skipped += 1
                    continue
            except ValueError:
                pass
        todo_list.append((code, cat))

    print(f"\n[数据获取] ETF/LOF K线数据增量下载")
    print(f"  ETF 数量: {len(etf_codes)}")
    print(f"  LOF 数量: {len(lof_codes)}")
    print(f"  已是最新: {skipped} (将跳过)")
    print(f"  待下载: {len(todo_list)}")
    print(f"  并发线程: {max_workers}")
    print(f"  按 Ctrl+C 可安全退出\n")

    if not todo_list:
        print("[数据获取] 所有 ETF/LOF K线数据已是最新")
        return

    done_count = 0
    success_count = 0
    fail_count = 0
    total_new = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for code, cat in todo_list:
            if _exit_flag:
                break
            future = executor.submit(_fetch_fund_ohlc, code, cat)
            futures[future] = (code, cat)

        for future in as_completed(futures):
            if _exit_flag:
                executor.shutdown(wait=False, cancel_futures=True)
                break

            code, cat = futures[future]
            done_count += 1

            try:
                _, new_count, success = future.result()
            except Exception as e:
                print(f"[错误] {code} ({cat}): {e}")
                success = False
                new_count = 0

            if success:
                success_count += 1
                total_new += new_count
            else:
                fail_count += 1

            elapsed = time.time() - start_time
            speed = done_count / elapsed if elapsed > 0 else 0
            eta = (len(todo_list) - done_count) / speed if speed > 0 else 0

            sys.stdout.write(
                f"\r[K线] {done_count}/{len(todo_list)} | "
                f"成功: {success_count} | 失败: {fail_count} | "
                f"新增: {total_new:,} | "
                f"速度: {speed:.1f} 只/秒 | "
                f"剩余: {eta/60:.1f} 分钟"
            )
            sys.stdout.flush()

            if done_count % 100 == 0:
                print()

    elapsed = time.time() - start_time
    print(f"\n\n[数据获取] K线增量下载完成！")
    print(f"  处理: {done_count} | 成功: {success_count} | 失败: {fail_count}")
    print(f"  新增记录: {total_new:,} | 耗时: {elapsed:.1f} 秒")


# ==================== 单只基金下载 ====================

def fetch_single_fund_history(code: str) -> bool:
    """下载单只基金的历史净值（供查看器实时调用）。"""
    code = code.zfill(6)
    print(f"[数据获取] 正在获取 {code} 历史净值...")

    result = _fetch_single_fund_nav_incremental(code)
    count, success = result[1], result[2]

    if success and count > 0:
        print(f"[数据获取] {code} 历史净值已下载（新增 {count} 条记录）")
        return True
    elif success and count == 0:
        print(f"[数据获取] {code} 数据已是最新")
        return True
    else:
        print(f"[数据获取] {code} 下载失败")
        return False


def fetch_single_fund_ohlc(code: str) -> bool:
    """下载单只 ETF/LOF 的 K线数据（供查看器实时调用）。"""
    code = code.zfill(6)
    print(f"[数据获取] 正在获取 {code} K线数据...")

    # 判断类型
    ohlc_count = get_ohlc_count(code)
    # 先尝试 ETF
    result = _fetch_fund_ohlc(code, 'etf')
    if result[1] == 0:
        # ETF 没数据，尝试 LOF
        result = _fetch_fund_ohlc(code, 'lof')

    count, success = result[1], result[2]

    if success and count > 0:
        print(f"[数据获取] {code} K线数据已下载（新增 {count} 条记录）")
        return True
    elif success and count == 0:
        if ohlc_count > 0:
            print(f"[数据获取] {code} K线数据已是最新")
        else:
            print(f"[数据获取] {code} 无K线数据（非上市基金，只有净值数据）")
        return True
    else:
        print(f"[数据获取] {code} K线下载失败")
        return False


# ==================== 便捷函数 ====================

def update_all_basic():
    """更新全部基金基本信息。"""
    setup_exit_handler()
    init_db()
    fetch_and_save_all_fund_basic()
    close_connection()


def update_all_history(max_workers: int = 4):
    """增量更新全部基金历史净值。"""
    setup_exit_handler()
    init_db()
    fetch_all_fund_history(max_workers=max_workers)
    close_connection()


def update_all_ohlc(max_workers: int = 4):
    """增量更新全部 ETF/LOF K线数据。"""
    setup_exit_handler()
    init_db()
    fetch_all_etf_lof_ohlc(max_workers=max_workers)
    close_connection()


def update_all(max_workers: int = 4):
    """一键增量更新所有数据（净值 + K线）。"""
    setup_exit_handler()
    init_db()
    print("=" * 60)
    print("  一键增量更新所有数据")
    print("=" * 60)
    print()
    fetch_all_fund_history(max_workers=max_workers)
    print()
    fetch_all_etf_lof_ohlc(max_workers=max_workers)
    close_connection()


def update_fund_detail(code: str):
    """更新单只基金详细信息。"""
    init_db()
    fetch_and_save_fund_detail(code)
    close_connection()


def update_fund_nav(code: str):
    """更新单只基金历史净值。"""
    init_db()
    fetch_single_fund_history(code)
    close_connection()


def update_fund_ohlc(code: str):
    """更新单只 ETF/LOF K线数据。"""
    init_db()
    fetch_single_fund_ohlc(code)
    close_connection()


if __name__ == "__main__":
    # 命令行模式
    init_db()
    print("=" * 60)
    print("  基金数据下载工具")
    print("=" * 60)
    print("1. 下载所有基金基本信息（基金列表）")
    print("2. 增量更新所有基金历史净值")
    print("3. 增量更新所有 ETF/LOF K线数据")
    print("4. 一键增量更新（净值 + K线）")
    print("5. 下载单只基金历史净值")
    print("6. 下载单只基金 K线数据")
    print("7. 下载单只基金详细信息")
    print("0. 退出")
    print()

    while True:
        choice = input("请选择操作 (0-7): ").strip()
        if choice == "1":
            fetch_and_save_all_fund_basic()
        elif choice == "2":
            workers = input("并发线程数（默认4）: ").strip()
            max_workers = int(workers) if workers.isdigit() and int(workers) > 0 else 4
            fetch_all_fund_history(max_workers=max_workers)
        elif choice == "3":
            workers = input("并发线程数（默认4）: ").strip()
            max_workers = int(workers) if workers.isdigit() and int(workers) > 0 else 4
            fetch_all_etf_lof_ohlc(max_workers=max_workers)
        elif choice == "4":
            workers = input("并发线程数（默认4）: ").strip()
            max_workers = int(workers) if workers.isdigit() and int(workers) > 0 else 4
            update_all(max_workers=max_workers)
        elif choice == "5":
            code = input("输入基金代码: ").strip()
            if code:
                fetch_single_fund_history(code)
        elif choice == "6":
            code = input("输入基金代码: ").strip()
            if code:
                fetch_single_fund_ohlc(code)
        elif choice == "7":
            code = input("输入基金代码: ").strip()
            if code:
                fetch_and_save_fund_detail(code)
        elif choice == "0":
            break
        else:
            print("无效选择")

    close_connection()
