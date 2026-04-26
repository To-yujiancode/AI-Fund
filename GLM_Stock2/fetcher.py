"""
股票数据获取模块
使用 akshare 从东方财富获取股票基本信息、日线行情，存入 SQLite。
支持增量下载、断点续传、进度显示、并发下载。

API 说明：
- stock_info_a_code_name()：获取全部A股代码和名称（稳定接口）
- stock_individual_info_em(symbol)：获取单只股票详细信息（行业、市值等）
- stock_zh_a_hist(symbol, period, start_date, end_date, adjust)：日线OHLC数据（支持增量）
"""

import time
import sys
import os
import signal
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==================== 网络连接修复（必须在 import akshare 之前执行） ====================
# 东方财富 push2 / push2his 服务器对连接非常敏感，常见问题：
#   1. 连接池复用：requests 默认保持 Keep-Alive，服务器会主动关闭空闲连接，
#      再次复用时就会 RemoteDisconnected
#   2. 代理干扰：系统代理会导致连接异常
#   3. 缺少请求头：服务器可能拒绝没有 User-Agent / Referer 的请求
#
# 修复策略：
#   1. 立即清除所有代理环境变量（在所有 import 之前）
#   2. 猴子补丁 requests.Session：trust_env=False + Connection:close
#   3. 添加 User-Agent、Referer 等必要请求头
#   4. 连接池设为最小值，防止连接复用

# 第一步：立即清除代理环境变量（必须在 import requests / akshare 之前）
for _k in ['http_proxy', 'https_proxy', 'HTTP_PROXY', 'HTTPS_PROXY',
           'all_proxy', 'ALL_PROXY', 'no_proxy', 'NO_PROXY']:
    os.environ.pop(_k, None)
os.environ['NO_PROXY'] = '*'

# 第二步：猴子补丁 requests.Session
import requests as _requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry as _Retry

_original_session_init = None
_patched = False


def _patch_requests_session():
    """
    猴子补丁 requests.Session：禁用代理 + 修复连接问题。
    代理环境变量已在模块顶部清除，这里只做 Session 补丁。
    """
    global _original_session_init, _patched
    if _patched:
        return

    if _original_session_init is None:
        _original_session_init = _requests.Session.__init__

    _default_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'close',
        'Referer': 'https://quote.eastmoney.com/',
    }

    def _patched_init(self, *args, **kwargs):
        _original_session_init(self, *args, **kwargs)
        self.trust_env = False
        self.headers.update(_default_headers)
        adapter = HTTPAdapter(
            pool_connections=1,
            pool_maxsize=1,
            max_retries=_Retry(total=0, connect=0),
        )
        self.mount('http://', adapter)
        self.mount('https://', adapter)

    _requests.Session.__init__ = _patched_init
    _patched = True


# 应用补丁
_patch_requests_session()

# 第三步：在代理清除和 Session 补丁之后才导入 akshare
import pandas as pd
import akshare as ak

from database import (
    init_db, batch_upsert_stock_basic, upsert_stock_detail,
    save_stock_daily, get_stock_daily_count, get_stock_date_range,
    get_latest_stock_date, get_stock_count, get_connection, close_connection
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


def _call_with_retry(func, *args, max_retries: int = 5, quiet: bool = False, **kwargs):
    """
    带重试的函数调用。
    - 每次请求使用新连接（Connection: close）
    - 遇到网络错误自动重试，间隔递增（2s, 5s, 8s, 11s, 14s）
    - quiet=True 时仅输出最终结果，减少日志刷屏（适用于非关键数据如股票详情）
    """
    last_err = None
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            err_str = str(e)
            is_network_err = any(kw in err_str for kw in [
                'ProxyError', 'proxy', 'ConnectionError',
                'RemoteDisconnected', 'ConnectionResetError',
                'ConnectionAbortedError',
                'timeout', 'timed out', 'Connection aborted',
                'NewConnectionError', 'MaxRetryError',
            ])
            if is_network_err and attempt < max_retries - 1:
                wait = 2 + attempt * 3
                if not quiet:
                    print(f"\n  [重试 {attempt+1}/{max_retries}] 网络错误，{wait}秒后重试: {err_str[:100]}")
                time.sleep(wait)
                continue
            raise


# ==================== 股票基本信息获取 ====================

def _fetch_stock_list_from_exchanges():
    """
    直接从沪深北交易所获取A股列表，不依赖 akshare 的 stock_info_a_code_name。
    akshare 内部调用 pd.read_excel 时未指定 engine，在 pandas 2.2+ 会报错。
    此函数直接请求交易所 API 并正确指定 openpyxl 引擎。
    """
    import warnings
    from io import BytesIO
    all_records = []

    # --- 上交所（主板A股 + 科创板）---
    print("  [1/3] 获取上交所列表...")
    try:
        url_sh = "https://query.sse.com.cn/sseQuery/commonQuery.do"
        headers_sh = {
            "Host": "query.sse.com.cn",
            "Referer": "https://www.sse.com.cn/assortment/stock/list/share/",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        for label, stock_type in [("主板A股", "1"), ("科创板", "8")]:
            params_sh = {
                "STOCK_TYPE": stock_type, "REG_PROVINCE": "", "CSRC_CODE": "",
                "STOCK_CODE": "", "sqlId": "COMMON_SSE_CP_GPJCTPZ_GPLB_GP_L",
                "COMPANY_STATUS": "2,4,5,7,8", "type": "inParams",
                "isPagination": "true", "pageHelp.cacheSize": "1",
                "pageHelp.beginPage": "1", "pageHelp.pageSize": "10000",
                "pageHelp.pageNo": "1", "pageHelp.endPage": "1",
            }
            r = _requests.get(url_sh, params=params_sh, headers=headers_sh, timeout=15)
            data = r.json()
            if data.get("result"):
                col_code = "A_STOCK_CODE" if stock_type == "1" else "A_STOCK_CODE"
                for item in data["result"]:
                    code = str(item.get(col_code, "")).strip()
                    name = str(item.get("SEC_NAME_CN", "")).strip()
                    if code and name:
                        all_records.append({"code": code, "name": name})
        print(f"  [1/3] 上交所: {len(all_records)} 只")
    except Exception as e:
        print(f"  [1/3] 上交所获取失败: {e}")

    # --- 深交所（A股）---
    print("  [2/3] 获取深交所列表...")
    try:
        url_sz = "https://www.szse.cn/api/report/ShowReport"
        params_sz = {
            "SHOWTYPE": "xlsx", "CATALOGID": "1110",
            "TABKEY": "tab1", "random": "0.6935816432433362",
        }
        headers_sz = {
            "Referer": "https://www.szse.cn/market/product/stock/list/index.html",
        }
        r = _requests.get(url_sz, params=params_sz, headers=headers_sz, timeout=15)
        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            df_sz = pd.read_excel(BytesIO(r.content), engine="openpyxl")
        if df_sz is not None and not df_sz.empty:
            sz_count = 0
            for _, row in df_sz.iterrows():
                code_raw = str(row.get("A股代码", ""))
                code = code_raw.split(".")[0].strip().zfill(6).replace("000nan", "")
                name = str(row.get("A股简称", "")).strip()
                if code and name and len(code) == 6:
                    all_records.append({"code": code, "name": name})
                    sz_count += 1
            print(f"  [2/3] 深交所: {sz_count} 只")
    except Exception as e:
        print(f"  [2/3] 深交所获取失败: {e}")

    # --- 北交所 ---
    print("  [3/3] 获取北交所列表...")
    try:
        import json as _json
        # 北交所 API 需要大量重定向（16次+），补丁后的 session（pool_maxsize=1）
        # 无法正常处理重定向链。使用原始 Session.__init__ 创建干净 session。
        _bse_session = _requests.Session.__new__(_requests.Session)
        _original_session_init(_bse_session)
        _bse_session.trust_env = False  # 仍然禁用代理

        url_bse = "https://www.bse.cn/nqxxController/nqxxCnzq.do"
        payload_bse = {
            "page": "0", "typejb": "T", "xxfcbj[]": "2",
            "xxzqdm": "", "sortfield": "xxzqdm", "sorttype": "asc",
        }
        headers_bse = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                          "(KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            "Referer": "https://www.bse.cn/nq/listedcompany.html",
        }
        r = _bse_session.post(url_bse, data=payload_bse, headers=headers_bse, timeout=15)
        text = r.text
        data_json = _json.loads(text[text.find("["):-1])
        total_pages = data_json[0].get("totalPages", 1)
        bse_count = 0
        for page_idx in range(total_pages):
            payload_bse["page"] = str(page_idx)
            r = _bse_session.post(url_bse, data=payload_bse, headers=headers_bse, timeout=15)
            text = r.text
            data_json = _json.loads(text[text.find("["):-1])
            for item in data_json[0].get("content", []):
                code = str(item.get("xxzqdm", "")).strip().zfill(6)
                name = str(item.get("xxzqjc", "")).strip()
                if not name:
                    name = str(item.get("xxywjc", "")).strip()
                if code and name:
                    all_records.append({"code": code, "name": name})
                    bse_count += 1
        print(f"  [3/3] 北交所: {bse_count} 只")
    except Exception as e:
        print(f"  [3/3] 北交所获取失败: {e}")

    return all_records


def fetch_and_save_stock_list():
    """
    获取全部A股股票列表（代码+名称）并存入数据库。
    直接从沪深北交易所 API 获取，不依赖 akshare（避免 pandas read_excel 引擎兼容问题）。
    """
    print("[股票] 正在获取A股列表...")
    try:
        records = _call_with_retry(_fetch_stock_list_from_exchanges)
    except Exception as e:
        print(f"[错误] 获取股票列表失败: {e}")
        return False

    if not records:
        print("[错误] 获取到的股票列表为空")
        return False

    # 去重（按 code）
    seen = set()
    unique_records = []
    for r in records:
        if r["code"] not in seen:
            seen.add(r["code"])
            unique_records.append(r)

    batch_upsert_stock_basic(unique_records)
    print(f"[股票] 共获取到 {len(unique_records)} 只股票，已保存")
    return True


def fetch_stock_detail(code: str) -> bool:
    """获取单只股票的详细信息（行业、上市日期、市值等）。"""
    try:
        # 股票详情为非关键数据，quiet=True 减少日志刷屏，2次重试即可
        df = _call_with_retry(ak.stock_individual_info_em, symbol=code,
                              max_retries=2, quiet=True)
    except Exception as e:
        # 仅在网络异常时输出一行简要警告，不再输出完整 URL
        err_brief = str(e).split('(')[0].strip() if '(' in str(e) else str(e)[:60]
        print(f"[警告] 获取 {code} 详情失败: {err_brief}")
        return False

    if df is None or df.empty:
        return False

    info = {}
    for _, row in df.iterrows():
        info[str(row['item'])] = str(row['value'])

    try:
        industry = info.get('行业', '')
        list_date = info.get('上市时间', '')
        if list_date and len(list_date) == 8:
            list_date = f"{list_date[:4]}-{list_date[4:6]}-{list_date[6:8]}"

        total_shares = float(info.get('总股本', 0) or 0)
        float_shares = float(info.get('流通股', 0) or 0)
        total_market_cap = float(info.get('总市值', 0) or 0)
        float_market_cap = float(info.get('流通市值', 0) or 0)
        latest_price = float(info.get('最新', 0) or 0)
        name = info.get('股票简称', '')

        upsert_stock_detail(
            code=code, name=name, industry=industry,
            list_date=list_date, total_shares=total_shares,
            float_shares=float_shares, total_market_cap=total_market_cap,
            float_market_cap=float_market_cap, latest_price=latest_price,
        )
    except (ValueError, TypeError) as e:
        print(f"[警告] 解析 {code} 详细信息时出错: {e}")
        return False

    return True


# ==================== 股票日线行情获取（增量） ====================

def _fetch_single_stock_daily(code: str) -> tuple:
    """
    增量获取单只股票的日线行情数据。
    stock_zh_a_hist 支持 start_date 参数，可以精确增量下载。

    返回: (code, new_records_count, success)
    """
    global _exit_flag
    if _exit_flag:
        return (code, 0, False)

    # 增量判断
    latest_date = get_latest_stock_date(code)
    if latest_date:
        try:
            last_dt = datetime.strptime(latest_date, '%Y-%m-%d')
            if (datetime.now() - last_dt).days <= 2:
                return (code, 0, True)  # 已是最新
            start_date = (last_dt + timedelta(days=1)).strftime('%Y%m%d')
        except ValueError:
            start_date = ''
    else:
        start_date = ''

    end_date = datetime.now().strftime('%Y%m%d')

    try:
        time.sleep(0.5)  # 避免触发东方财富限频
        df = _call_with_retry(ak.stock_zh_a_hist,
                              symbol=code, period='daily',
                              start_date=start_date, end_date=end_date, adjust='qfq')
    except Exception as e:
        err_str = str(e)
        # 停牌、退市等无法获取数据的股票，静默跳过
        is_network_err = any(kw in err_str for kw in [
            'Connection', 'timeout', 'aborted', 'RemoteDisconnected',
            'ProxyError', 'MaxRetryError', 'NewConnectionError'
        ])
        if not is_network_err:
            return (code, 0, True)
        # 网络错误
        print(f"\n[失败] {code}: {err_str[:120]}")
        return (code, 0, False)

    if df is None or df.empty:
        return (code, 0, True)

    records = []
    for _, row in df.iterrows():
        date_str = str(row['日期'])
        if '-' not in date_str:
            date_str = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"

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
        save_stock_daily(records)

    return (code, len(records), True)


def fetch_all_stock_daily(max_workers: int = 4):
    """增量下载所有股票日线行情。"""
    global _exit_flag

    conn = get_connection()
    cursor = conn.execute("SELECT code, name FROM stock_basic ORDER BY code")
    stocks = cursor.fetchall()
    total = len(stocks)

    if total == 0:
        print("[股票] 数据库中没有股票列表，请先获取股票列表")
        return

    # 增量过滤
    todo_list = []
    skipped = 0
    for code, name in stocks:
        latest_date = get_latest_stock_date(code)
        if latest_date:
            try:
                last_dt = datetime.strptime(latest_date, '%Y-%m-%d')
                if (datetime.now() - last_dt).days <= 2:
                    skipped += 1
                    continue
            except ValueError:
                pass
        todo_list.append((code, name))

    print(f"\n[股票] 日线行情增量下载")
    print(f"  总股票数: {total}")
    print(f"  已是最新: {skipped}")
    print(f"  待下载: {len(todo_list)}")
    print(f"  并发: {max_workers}")
    print(f"  Ctrl+C 安全退出\n")

    if not todo_list:
        print("[股票] 所有股票行情已是最新")
        return

    done_count = success_count = fail_count = total_new = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for code, name in todo_list:
            if _exit_flag:
                break
            future = executor.submit(_fetch_single_stock_daily, code)
            futures[future] = (code, name)

        for future in as_completed(futures):
            if _exit_flag:
                executor.shutdown(wait=False, cancel_futures=True)
                break
            code, name = futures[future]
            done_count += 1
            try:
                _, new_count, success = future.result()
            except Exception:
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
                f"\r[股票] {done_count}/{len(todo_list)} | "
                f"成功: {success_count} | 失败: {fail_count} | "
                f"新增: {total_new:,} | "
                f"速度: {speed:.1f}/秒 | 剩余: {eta/60:.1f}分钟"
            )
            sys.stdout.flush()
            if done_count % 100 == 0:
                print()

    elapsed = time.time() - start_time
    print(f"\n\n[股票] 日线下载完成！")
    print(f"  处理: {done_count} | 成功: {success_count} | 失败: {fail_count}")
    print(f"  新增: {total_new:,} | 耗时: {elapsed:.1f}秒")


def fetch_single_stock_daily(code: str) -> bool:
    """下载单只股票日线行情。"""
    code = code.strip().zfill(6)
    print(f"[股票] 正在获取 {code} 日线行情...")
    result = _fetch_single_stock_daily(code)
    count, success = result[1], result[2]
    if success and count > 0:
        print(f"[股票] {code} 日线已下载（新增 {count} 条）")
        return True
    elif success and count == 0:
        dc = get_stock_daily_count(code)
        if dc > 0:
            print(f"[股票] {code} 数据已是最新（共 {dc:,} 条）")
        else:
            print(f"[股票] {code} 无数据（可能已退市或停牌）")
        return True
    else:
        print(f"[股票] {code} 下载失败")
        return False


def fetch_single_stock_all(code: str) -> bool:
    """下载单只股票全部数据（详情 + 日线）。"""
    code = code.strip().zfill(6)
    fetch_stock_detail(code)
    return fetch_single_stock_daily(code)


# ==================== 批量获取股票详情 ====================

def _fetch_single_stock_detail(code: str) -> tuple:
    """获取单只股票详情（供批量调用）。"""
    global _exit_flag
    if _exit_flag:
        return (code, False)
    try:
        time.sleep(0.3)
        ok = fetch_stock_detail(code)
        return (code, ok)
    except Exception:
        return (code, False)


def fetch_all_stock_details(max_workers: int = 4):
    """批量获取所有股票的详细信息（行业、市值等）。"""
    global _exit_flag

    conn = get_connection()
    cursor = conn.execute(
        "SELECT code FROM stock_basic WHERE industry = '' OR industry IS NULL ORDER BY code"
    )
    stocks = cursor.fetchall()
    total = len(stocks)

    if total == 0:
        print("[股票] 所有股票详情已获取")
        return

    print(f"\n[股票] 批量获取股票详情")
    print(f"  待获取: {total}")
    print(f"  并发: {max_workers}\n")

    done_count = success_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for (code,) in stocks:
            if _exit_flag:
                break
            future = executor.submit(_fetch_single_stock_detail, code)
            futures[future] = code

        for future in as_completed(futures):
            if _exit_flag:
                executor.shutdown(wait=False, cancel_futures=True)
                break
            code = futures[future]
            done_count += 1
            try:
                _, ok = future.result()
            except Exception:
                ok = False
            if ok:
                success_count += 1

            elapsed = time.time() - start_time
            speed = done_count / elapsed if elapsed > 0 else 0
            eta = (total - done_count) / speed if speed > 0 else 0

            sys.stdout.write(
                f"\r[详情] {done_count}/{total} | "
                f"成功: {success_count} | "
                f"速度: {speed:.1f}/秒 | 剩余: {eta/60:.1f}分钟"
            )
            sys.stdout.flush()
            if done_count % 100 == 0:
                print()

    elapsed = time.time() - start_time
    print(f"\n\n[股票] 详情获取完成！")
    print(f"  处理: {done_count} | 成功: {success_count} | 耗时: {elapsed:.1f}秒")


# ==================== 便捷函数 ====================

def update_stock_list():
    """更新股票列表。"""
    init_db()
    fetch_and_save_stock_list()
    close_connection()


def update_stock_history(max_workers: int = 4):
    """增量更新所有股票日线行情。"""
    setup_exit_handler()
    init_db()
    fetch_all_stock_daily(max_workers=max_workers)
    close_connection()


def update_stock_details(max_workers: int = 4):
    """批量获取股票详情。"""
    setup_exit_handler()
    init_db()
    fetch_all_stock_details(max_workers=max_workers)
    close_connection()


def update_all(max_workers: int = 4):
    """一键增量更新所有数据（详情 + 日线）。"""
    setup_exit_handler()
    init_db()
    print("=" * 60)
    print("  一键增量更新股票数据")
    print("=" * 60)
    print()
    fetch_all_stock_details(max_workers=max_workers)
    print()
    fetch_all_stock_daily(max_workers=max_workers)
    close_connection()


if __name__ == "__main__":
    init_db()
    print("=" * 60)
    print("  股票数据下载工具")
    print("=" * 60)
    print("1. 下载股票列表（代码+名称）")
    print("2. 增量下载所有股票日线行情")
    print("3. 批量获取股票详细信息（行业、市值等）")
    print("4. 一键更新（详情 + 日线）")
    print("5. 下载单只股票数据")
    print("0. 退出")
    print()

    while True:
        choice = input("请选择操作 (0-5): ").strip()
        if choice == "1":
            fetch_and_save_stock_list()
        elif choice == "2":
            workers = input("并发线程数（默认4）: ").strip()
            max_workers = int(workers) if workers.isdigit() and int(workers) > 0 else 4
            setup_exit_handler()
            fetch_all_stock_daily(max_workers=max_workers)
        elif choice == "3":
            workers = input("并发线程数（默认4）: ").strip()
            max_workers = int(workers) if workers.isdigit() and int(workers) > 0 else 4
            setup_exit_handler()
            fetch_all_stock_details(max_workers=max_workers)
        elif choice == "4":
            workers = input("并发线程数（默认4）: ").strip()
            max_workers = int(workers) if workers.isdigit() and int(workers) > 0 else 4
            setup_exit_handler()
            fetch_all_stock_details(max_workers=max_workers)
            fetch_all_stock_daily(max_workers=max_workers)
        elif choice == "5":
            code = input("输入股票代码: ").strip()
            if code:
                fetch_single_stock_all(code)
        elif choice == "0":
            break
        else:
            print("无效选择")

    close_connection()
