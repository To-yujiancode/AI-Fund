#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基金/股票数据管理与分析系统
功能：
1. 通过akshare获取基金/股票基本信息，存储到SQLite
2. 下载历史数据，支持增量更新
3. 查询和可视化历史数据，支持鼠标框选缩放
4. 多种技术指标计算与绘图
5. 缺失数据用彩色方块标记
"""

import sys
import os
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
import warnings

warnings.filterwarnings('ignore')

import pandas as pd
import numpy as np
import akshare as ak

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QPushButton, QLineEdit, QLabel, QComboBox, QListWidget,
    QListWidgetItem, QProgressBar, QTextEdit, QCheckBox,
    QSplitter, QGroupBox, QGridLayout, QMessageBox, QTabWidget,
    QScrollArea, QFrame, QSizePolicy, QDialog, QFormLayout,
    QDialogButtonBox, QDateEdit, QSpinBox, QDoubleSpinBox
)
from PyQt5.QtCore import (
    Qt, QThread, pyqtSignal, QTimer, QDate, QSize
)
from PyQt5.QtGui import QFont, QColor, QPalette, QIcon

import matplotlib

matplotlib.use('Qt5Agg')
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.dates import DateFormatter, AutoDateLocator
import matplotlib.ticker as mticker

# ==================== 配置 ====================
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fund_data.db')
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


# ==================== 数据库管理 ====================
class DatabaseManager:
    """SQLite数据库管理器"""

    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self.conn = None
        self._init_db()

    def _get_connection(self):
        """获取数据库连接"""
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self.conn.execute("PRAGMA journal_mode=WAL")
            self.conn.execute("PRAGMA synchronous=NORMAL")
            self.conn.execute("PRAGMA cache_size=-64000")
        return self.conn

    def _init_db(self):
        """初始化数据库表结构"""
        conn = self._get_connection()
        cursor = conn.cursor()

        # 基金基本信息表
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS fund_basic_info
                       (
                           fund_code
                           TEXT
                           PRIMARY
                           KEY,
                           fund_name
                           TEXT,
                           fund_type
                           TEXT,
                           fund_category
                           TEXT,
                           establish_date
                           TEXT,
                           fund_manager
                           TEXT,
                           fund_company
                           TEXT,
                           net_asset_value
                           REAL,
                           accumulated_net_value
                           REAL,
                           management_fee
                           REAL,
                           custody_fee
                           REAL,
                           purchase_status
                           TEXT,
                           redemption_status
                           TEXT,
                           is_private
                           INTEGER
                           DEFAULT
                           0,
                           update_time
                           TEXT
                           DEFAULT (
                           datetime
                       (
                           'now',
                           'localtime'
                       )),
                           notes TEXT
                           )
                       ''')

        # 基金历史净值表
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS fund_nav_history
                       (
                           fund_code
                           TEXT
                           NOT
                           NULL,
                           nav_date
                           TEXT
                           NOT
                           NULL,
                           unit_nav
                           REAL,
                           accumulated_nav
                           REAL,
                           daily_return
                           REAL,
                           subscription_status
                           TEXT,
                           redemption_status
                           TEXT,
                           PRIMARY
                           KEY
                       (
                           fund_code,
                           nav_date
                       )
                           )
                       ''')

        # 股票基本信息表
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS stock_basic_info
                       (
                           stock_code
                           TEXT
                           PRIMARY
                           KEY,
                           stock_name
                           TEXT,
                           industry
                           TEXT,
                           market
                           TEXT,
                           list_date
                           TEXT,
                           total_shares
                           REAL,
                           circulating_shares
                           REAL,
                           update_time
                           TEXT
                           DEFAULT (
                           datetime
                       (
                           'now',
                           'localtime'
                       ))
                           )
                       ''')

        # 股票历史日线表
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS stock_daily_history
                       (
                           stock_code
                           TEXT
                           NOT
                           NULL,
                           trade_date
                           TEXT
                           NOT
                           NULL,
                           open
                           REAL,
                           high
                           REAL,
                           low
                           REAL,
                           close
                           REAL,
                           volume
                           REAL,
                           amount
                           REAL,
                           amplitude
                           REAL,
                           pct_change
                           REAL,
                           turnover_rate
                           REAL,
                           PRIMARY
                           KEY
                       (
                           stock_code,
                           trade_date
                       )
                           )
                       ''')

        # 下载日志表
        cursor.execute('''
                       CREATE TABLE IF NOT EXISTS download_log
                       (
                           id
                           INTEGER
                           PRIMARY
                           KEY
                           AUTOINCREMENT,
                           data_type
                           TEXT,
                           code
                           TEXT,
                           download_time
                           TEXT
                           DEFAULT (
                           datetime
                       (
                           'now',
                           'localtime'
                       )),
                           records_count INTEGER,
                           status TEXT,
                           error_msg TEXT
                           )
                       ''')

        # 创建索引
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fund_nav_date ON fund_nav_history(fund_code, nav_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_stock_daily_date ON stock_daily_history(stock_code, trade_date)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_fund_type ON fund_basic_info(fund_type)')

        conn.commit()

    def get_fund_list(self, fund_type: str = None, search_text: str = None) -> List[Dict]:
        """获取基金列表"""
        conn = self._get_connection()
        query = "SELECT * FROM fund_basic_info WHERE 1=1"
        params = []
        if fund_type and fund_type != '全部':
            query += " AND fund_type = ?"
            params.append(fund_type)
        if search_text:
            query += " AND (fund_code LIKE ? OR fund_name LIKE ?)"
            params.extend([f'%{search_text}%', f'%{search_text}%'])
        query += " ORDER BY fund_code LIMIT 5000"
        cursor = conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_stock_list(self, search_text: str = None) -> List[Dict]:
        """获取股票列表"""
        conn = self._get_connection()
        query = "SELECT * FROM stock_basic_info WHERE 1=1"
        params = []
        if search_text:
            query += " AND (stock_code LIKE ? OR stock_name LIKE ?)"
            params.extend([f'%{search_text}%', f'%{search_text}%'])
        query += " ORDER BY stock_code LIMIT 5000"
        cursor = conn.execute(query, params)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_fund_nav_history(self, fund_code: str) -> pd.DataFrame:
        """获取基金历史净值数据"""
        conn = self._get_connection()
        query = "SELECT * FROM fund_nav_history WHERE fund_code = ? ORDER BY nav_date"
        df = pd.read_sql_query(query, conn, params=(fund_code,))
        if not df.empty:
            df['nav_date'] = pd.to_datetime(df['nav_date'])
            df.set_index('nav_date', inplace=True)
        return df

    def get_stock_daily_history(self, stock_code: str) -> pd.DataFrame:
        """获取股票历史日线数据"""
        conn = self._get_connection()
        query = "SELECT * FROM stock_daily_history WHERE stock_code = ? ORDER BY trade_date"
        df = pd.read_sql_query(query, conn, params=(stock_code,))
        if not df.empty:
            df['trade_date'] = pd.to_datetime(df['trade_date'])
            df.set_index('trade_date', inplace=True)
        return df

    def get_fund_types(self) -> List[str]:
        """获取所有基金类型"""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT DISTINCT fund_type FROM fund_basic_info WHERE fund_type IS NOT NULL ORDER BY fund_type")
        return ['全部'] + [row[0] for row in cursor.fetchall()]

    def get_max_nav_date(self, fund_code: str) -> Optional[str]:
        """获取某基金在数据库中的最大净值日期"""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT MAX(nav_date) FROM fund_nav_history WHERE fund_code = ?", (fund_code,)
        )
        result = cursor.fetchone()
        return result[0] if result[0] else None

    def get_max_trade_date(self, stock_code: str) -> Optional[str]:
        """获取某股票在数据库中的最大交易日期"""
        conn = self._get_connection()
        cursor = conn.execute(
            "SELECT MAX(trade_date) FROM stock_daily_history WHERE stock_code = ?", (stock_code,)
        )
        result = cursor.fetchone()
        return result[0] if result[0] else None

    def insert_fund_basic_info(self, fund_info: Dict):
        """插入或更新基金基本信息"""
        conn = self._get_connection()
        conn.execute('''
            INSERT OR REPLACE INTO fund_basic_info 
            (fund_code, fund_name, fund_type, fund_category, establish_date,
             fund_manager, fund_company, net_asset_value, accumulated_net_value,
             management_fee, custody_fee, purchase_status, redemption_status,
             is_private, update_time, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'), ?)
        ''', (
            fund_info.get('fund_code'),
            fund_info.get('fund_name'),
            fund_info.get('fund_type'),
            fund_info.get('fund_category'),
            fund_info.get('establish_date'),
            fund_info.get('fund_manager'),
            fund_info.get('fund_company'),
            fund_info.get('net_asset_value'),
            fund_info.get('accumulated_net_value'),
            fund_info.get('management_fee'),
            fund_info.get('custody_fee'),
            fund_info.get('purchase_status'),
            fund_info.get('redemption_status'),
            fund_info.get('is_private', 0),
            fund_info.get('notes', '')
        ))
        conn.commit()

    def insert_fund_nav_batch(self, records: List[Dict]):
        """批量插入基金净值数据"""
        if not records:
            return
        conn = self._get_connection()
        conn.executemany('''
                         INSERT
                         OR IGNORE INTO fund_nav_history 
            (fund_code, nav_date, unit_nav, accumulated_nav, daily_return,
             subscription_status, redemption_status)
            VALUES (?, ?, ?, ?, ?, ?, ?)
                         ''', [
                             (
                                 r['fund_code'], r['nav_date'], r.get('unit_nav'),
                                 r.get('accumulated_nav'), r.get('daily_return'),
                                 r.get('subscription_status'), r.get('redemption_status')
                             )
                             for r in records
                         ])
        conn.commit()

    def insert_stock_basic_info(self, stock_info: Dict):
        """插入或更新股票基本信息"""
        conn = self._get_connection()
        conn.execute('''
            INSERT OR REPLACE INTO stock_basic_info
            (stock_code, stock_name, industry, market, list_date,
             total_shares, circulating_shares, update_time)
            VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
        ''', (
            stock_info.get('stock_code'),
            stock_info.get('stock_name'),
            stock_info.get('industry'),
            stock_info.get('market'),
            stock_info.get('list_date'),
            stock_info.get('total_shares'),
            stock_info.get('circulating_shares'),
        ))
        conn.commit()

    def insert_stock_daily_batch(self, records: List[Dict]):
        """批量插入股票日线数据"""
        if not records:
            return
        conn = self._get_connection()
        conn.executemany('''
                         INSERT
                         OR IGNORE INTO stock_daily_history
            (stock_code, trade_date, open, high, low, close, volume, amount,
             amplitude, pct_change, turnover_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                         ''', [
                             (
                                 r['stock_code'], r['trade_date'], r.get('open'),
                                 r.get('high'), r.get('low'), r.get('close'),
                                 r.get('volume'), r.get('amount'), r.get('amplitude'),
                                 r.get('pct_change'), r.get('turnover_rate')
                             )
                             for r in records
                         ])
        conn.commit()

    def get_database_stats(self) -> Dict:
        """获取数据库统计信息"""
        conn = self._get_connection()
        stats = {}
        cursor = conn.execute("SELECT COUNT(*) FROM fund_basic_info")
        stats['fund_count'] = cursor.fetchone()[0]
        cursor = conn.execute("SELECT COUNT(*) FROM stock_basic_info")
        stats['stock_count'] = cursor.fetchone()[0]
        cursor = conn.execute("SELECT COUNT(DISTINCT fund_code) FROM fund_nav_history")
        stats['fund_with_nav'] = cursor.fetchone()[0]
        cursor = conn.execute("SELECT COUNT(DISTINCT stock_code) FROM stock_daily_history")
        stats['stock_with_data'] = cursor.fetchone()[0]
        cursor = conn.execute("SELECT COUNT(*) FROM fund_nav_history")
        stats['total_nav_records'] = cursor.fetchone()[0]
        cursor = conn.execute("SELECT COUNT(*) FROM stock_daily_history")
        stats['total_stock_records'] = cursor.fetchone()[0]
        return stats

    def close(self):
        if self.conn:
            self.conn.close()
            self.conn = None


# ==================== 指标计算 ====================
class IndicatorCalculator:
    """技术指标计算器"""

    @staticmethod
    def calc_ma(data: pd.Series, periods: List[int] = [5, 10, 20, 60]) -> pd.DataFrame:
        """计算移动平均线"""
        result = pd.DataFrame(index=data.index)
        for p in periods:
            result[f'MA{p}'] = data.rolling(window=p).mean()
        return result

    @staticmethod
    def calc_macd(data: pd.Series, fast=12, slow=26, signal=9) -> pd.DataFrame:
        """计算MACD指标"""
        result = pd.DataFrame(index=data.index)
        ema_fast = data.ewm(span=fast, adjust=False).mean()
        ema_slow = data.ewm(span=slow, adjust=False).mean()
        result['DIF'] = ema_fast - ema_slow
        result['DEA'] = result['DIF'].ewm(span=signal, adjust=False).mean()
        result['MACD'] = 2 * (result['DIF'] - result['DEA'])
        return result

    @staticmethod
    def calc_rsi(data: pd.Series, period: int = 14) -> pd.Series:
        """计算RSI指标"""
        delta = data.diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        return rsi

    @staticmethod
    def calc_bollinger_bands(data: pd.Series, period: int = 20, std_dev: float = 2) -> pd.DataFrame:
        """计算布林带"""
        result = pd.DataFrame(index=data.index)
        result['BB_MIDDLE'] = data.rolling(window=period).mean()
        bb_std = data.rolling(window=period).std()
        result['BB_UPPER'] = result['BB_MIDDLE'] + std_dev * bb_std
        result['BB_LOWER'] = result['BB_MIDDLE'] - std_dev * bb_std
        result['BB_WIDTH'] = (result['BB_UPPER'] - result['BB_LOWER']) / result['BB_MIDDLE'] * 100
        return result

    @staticmethod
    def calc_kdj(data_high: pd.Series, data_low: pd.Series, data_close: pd.Series,
                 period: int = 9, k_period: int = 3, d_period: int = 3) -> pd.DataFrame:
        """计算KDJ指标"""
        result = pd.DataFrame(index=data_high.index)
        lowest_low = data_low.rolling(window=period).min()
        highest_high = data_high.rolling(window=period).max()
        rsv = ((data_close - lowest_low) / (highest_high - lowest_low)) * 100
        rsv = rsv.fillna(50)
        result['K'] = rsv.ewm(span=k_period, adjust=False).mean()
        result['D'] = result['K'].ewm(span=d_period, adjust=False).mean()
        result['J'] = 3 * result['K'] - 2 * result['D']
        return result

    @staticmethod
    def calc_max_drawdown(data: pd.Series) -> pd.Series:
        """计算滚动最大回撤"""
        cumulative_max = data.expanding().max()
        drawdown = (data - cumulative_max) / cumulative_max
        return drawdown

    @staticmethod
    def calc_cumulative_return(data: pd.Series) -> pd.Series:
        """计算累计收益率"""
        return data / data.iloc[0] - 1

    @staticmethod
    def calc_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.03) -> float:
        """计算年化夏普比率"""
        if len(returns) < 2:
            return 0
        excess_returns = returns - risk_free_rate / 252
        if excess_returns.std() == 0:
            return 0
        return np.sqrt(252) * excess_returns.mean() / excess_returns.std()

    @staticmethod
    def calc_annual_return(returns: pd.Series) -> float:
        """计算年化收益率"""
        if len(returns) < 2:
            return 0
        total_return = (1 + returns).prod() - 1
        years = len(returns) / 252
        if years == 0:
            return 0
        return (1 + total_return) ** (1 / years) - 1

    @staticmethod
    def calc_volatility(returns: pd.Series) -> float:
        """计算年化波动率"""
        if len(returns) < 2:
            return 0
        return returns.std() * np.sqrt(252)


# ==================== 数据获取线程 ====================
class DataFetchThread(QThread):
    """后台数据获取线程"""
    progress = pyqtSignal(int, int)  # 当前进度, 总数
    status = pyqtSignal(str)  # 状态信息
    finished = pyqtSignal(bool, str)  # 完成标志, 消息
    log = pyqtSignal(str)  # 日志信息

    def __init__(self, db_manager: DatabaseManager, task_type: str,
                 code_list: List[str] = None, data_type: str = 'fund'):
        super().__init__()
        self.db = db_manager
        self.task_type = task_type  # 'fetch_basic', 'fetch_history', 'fetch_stock_basic', 'fetch_stock_history'
        self.code_list = code_list or []
        self.data_type = data_type  # 'fund' or 'stock'
        self._is_running = True

    def stop(self):
        self._is_running = False

    def run(self):
        try:
            if self.task_type == 'fetch_fund_basic':
                self._fetch_fund_basic_info()
            elif self.task_type == 'fetch_fund_history':
                self._fetch_fund_history()
            elif self.task_type == 'fetch_stock_basic':
                self._fetch_stock_basic_info()
            elif self.task_type == 'fetch_stock_history':
                self._fetch_stock_history()
        except Exception as e:
            self.finished.emit(False, f"任务执行出错: {str(e)}")
            self.log.emit(f"❌ 错误: {str(e)}")

    def _fetch_fund_basic_info(self):
        """获取基金基本信息"""
        self.status.emit("正在从akshare获取基金列表...")
        self.log.emit("📡 开始获取基金基本信息...")
        try:
            # 使用akshare获取基金列表
            fund_df = ak.fund_name_em()
            total = len(fund_df)
            self.log.emit(f"📊 获取到 {total} 条基金记录")

            count = 0
            for idx, row in fund_df.iterrows():
                if not self._is_running:
                    break
                try:
                    fund_info = {
                        'fund_code': str(row.get('基金代码', '')),
                        'fund_name': str(row.get('基金简称', '')),
                        'fund_type': str(row.get('基金类型', '')),
                        'fund_category': '',
                        'establish_date': '',
                        'fund_manager': '',
                        'fund_company': '',
                        'net_asset_value': None,
                        'accumulated_net_value': None,
                        'management_fee': None,
                        'custody_fee': None,
                        'purchase_status': '',
                        'redemption_status': '',
                        'is_private': 0,
                        'notes': ''
                    }
                    self.db.insert_fund_basic_info(fund_info)
                    count += 1
                    if count % 500 == 0:
                        self.progress.emit(count, total)
                        self.status.emit(f"正在保存基金信息... {count}/{total}")
                except Exception:
                    continue

            self.progress.emit(total, total)
            self.log.emit(f"✅ 基金基本信息保存完成，共 {count} 条")
            self.finished.emit(True, f"基金基本信息更新完成！共 {count} 条记录")
        except Exception as e:
            self.log.emit(f"❌ 获取基金列表失败: {str(e)}")
            self.finished.emit(False, f"获取失败: {str(e)}")

    def _fetch_fund_history(self):
        """获取基金历史净值数据（增量更新）"""
        if not self.code_list:
            self.finished.emit(False, "没有选择基金")
            return

        total = len(self.code_list)
        success_count = 0
        fail_count = 0
        total_records = 0

        for i, code in enumerate(self.code_list):
            if not self._is_running:
                break

            self.progress.emit(i + 1, total)
            self.status.emit(f"正在下载基金净值: {code} ({i + 1}/{total})")

            try:
                # 检查数据库中已有数据的最新日期
                max_date = self.db.get_max_nav_date(code)
                start_date = None
                if max_date:
                    # 从最大日期的下一天开始
                    start_date_dt = datetime.strptime(max_date, '%Y-%m-%d') + timedelta(days=1)
                    start_date = start_date_dt.strftime('%Y%m%d')
                    self.log.emit(f"  📅 {code} 增量更新，从 {start_date} 开始")
                else:
                    self.log.emit(f"  🆕 {code} 首次下载，获取全部历史数据")

                # 获取基金净值数据
                try:
                    nav_df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
                except Exception:
                    # 尝试备用接口
                    try:
                        nav_df = ak.fund_nav_em(symbol=code)
                    except Exception:
                        self.log.emit(f"  ⚠️ {code} 无法获取净值数据（可能是私募基金或已清盘）")
                        fail_count += 1
                        continue

                if nav_df is None or nav_df.empty:
                    self.log.emit(f"  ⚠️ {code} 返回空数据")
                    fail_count += 1
                    continue

                # 处理列名（不同API返回的列名可能不同）
                records = []
                for _, row in nav_df.iterrows():
                    try:
                        # 尝试多种可能的列名
                        nav_date = None
                        unit_nav = None
                        accumulated_nav = None
                        daily_return = None

                        for date_col in ['净值日期', 'nav_date', '日期', 'date', '时间']:
                            if date_col in nav_df.columns:
                                val = row.get(date_col)
                                if pd.notna(val):
                                    try:
                                        nav_date = pd.to_datetime(val).strftime('%Y-%m-%d')
                                    except:
                                        nav_date = str(val)[:10]
                                    break

                        for nav_col in ['单位净值', 'unit_nav', '净值', 'nav', '单位净值(元)']:
                            if nav_col in nav_df.columns:
                                val = row.get(nav_col)
                                if pd.notna(val):
                                    unit_nav = float(val)
                                break

                        for acc_col in ['累计净值', 'accumulated_nav', '累计净值(元)']:
                            if acc_col in nav_df.columns:
                                val = row.get(acc_col)
                                if pd.notna(val):
                                    accumulated_nav = float(val)
                                break

                        for ret_col in ['日增长率', 'daily_return', '日涨幅', 'pct_change']:
                            if ret_col in nav_df.columns:
                                val = row.get(ret_col)
                                if pd.notna(val):
                                    try:
                                        daily_return = float(str(val).replace('%', ''))
                                    except:
                                        daily_return = float(val)
                                break

                        if nav_date and unit_nav is not None:
                            # 增量过滤
                            if start_date and nav_date < start_date.replace('', '-'):
                                continue
                            records.append({
                                'fund_code': code,
                                'nav_date': nav_date,
                                'unit_nav': unit_nav,
                                'accumulated_nav': accumulated_nav,
                                'daily_return': daily_return,
                                'subscription_status': '',
                                'redemption_status': ''
                            })
                    except Exception:
                        continue

                if records:
                    self.db.insert_fund_nav_batch(records)
                    total_records += len(records)
                    self.log.emit(f"  ✅ {code} 新增 {len(records)} 条净值记录")

                success_count += 1
                time.sleep(0.3)  # 避免请求过快

            except Exception as e:
                self.log.emit(f"  ❌ {code} 下载失败: {str(e)[:100]}")
                fail_count += 1

        msg = f"历史数据下载完成！成功: {success_count}, 失败: {fail_count}, 新增记录: {total_records}"
        self.log.emit(f"🎉 {msg}")
        self.finished.emit(True, msg)

    def _fetch_stock_basic_info(self):
        """获取股票基本信息"""
        self.status.emit("正在获取A股股票列表...")
        self.log.emit("📡 开始获取股票基本信息...")
        try:
            stock_df = ak.stock_zh_a_spot_em()
            total = len(stock_df)
            self.log.emit(f"📊 获取到 {total} 条股票记录")

            count = 0
            for idx, row in stock_df.iterrows():
                if not self._is_running:
                    break
                try:
                    stock_info = {
                        'stock_code': str(row.get('代码', '')),
                        'stock_name': str(row.get('名称', '')),
                        'industry': str(row.get('所属行业', '')),
                        'market': 'A股',
                        'list_date': '',
                        'total_shares': None,
                        'circulating_shares': None,
                    }
                    self.db.insert_stock_basic_info(stock_info)
                    count += 1
                    if count % 500 == 0:
                        self.progress.emit(count, total)
                        self.status.emit(f"正在保存股票信息... {count}/{total}")
                except Exception:
                    continue

            self.progress.emit(total, total)
            self.log.emit(f"✅ 股票基本信息保存完成，共 {count} 条")
            self.finished.emit(True, f"股票基本信息更新完成！共 {count} 条记录")
        except Exception as e:
            self.log.emit(f"❌ 获取股票列表失败: {str(e)}")
            self.finished.emit(False, f"获取失败: {str(e)}")

    def _fetch_stock_history(self):
        """获取股票历史日线数据"""
        if not self.code_list:
            self.finished.emit(False, "没有选择股票")
            return

        total = len(self.code_list)
        total_records = 0

        for i, code in enumerate(self.code_list):
            if not self._is_running:
                break

            self.progress.emit(i + 1, total)
            self.status.emit(f"正在下载股票数据: {code} ({i + 1}/{total})")

            try:
                max_date = self.db.get_max_trade_date(code)
                start_date = "19900101"
                if max_date:
                    start_dt = datetime.strptime(max_date, '%Y-%m-%d') + timedelta(days=1)
                    start_date = start_dt.strftime('%Y%m%d')
                    self.log.emit(f"  📅 {code} 增量更新，从 {start_date} 开始")

                end_date = datetime.now().strftime('%Y%m%d')
                if start_date >= end_date:
                    self.log.emit(f"  ✅ {code} 数据已是最新")
                    continue

                try:
                    hist_df = ak.stock_zh_a_hist(symbol=code, period='daily',
                                                 start_date=start_date, end_date=end_date,
                                                 adjust='qfq')
                except Exception:
                    self.log.emit(f"  ⚠️ {code} 无法获取历史数据")
                    continue

                if hist_df is None or hist_df.empty:
                    continue

                records = []
                for _, row in hist_df.iterrows():
                    try:
                        records.append({
                            'stock_code': code,
                            'trade_date': str(row.get('日期', ''))[:10],
                            'open': float(row.get('开盘', 0)),
                            'high': float(row.get('最高', 0)),
                            'low': float(row.get('最低', 0)),
                            'close': float(row.get('收盘', 0)),
                            'volume': float(row.get('成交量', 0)),
                            'amount': float(row.get('成交额', 0)),
                            'amplitude': float(row.get('振幅', 0)) if pd.notna(row.get('振幅', 0)) else None,
                            'pct_change': float(row.get('涨跌幅', 0)) if pd.notna(row.get('涨跌幅', 0)) else None,
                            'turnover_rate': float(row.get('换手率', 0)) if pd.notna(row.get('换手率', 0)) else None,
                        })
                    except Exception:
                        continue

                if records:
                    self.db.insert_stock_daily_batch(records)
                    total_records += len(records)
                    self.log.emit(f"  ✅ {code} 新增 {len(records)} 条日线记录")

                time.sleep(0.2)

            except Exception as e:
                self.log.emit(f"  ❌ {code} 下载失败: {str(e)[:100]}")

        msg = f"股票历史数据下载完成！新增记录: {total_records}"
        self.log.emit(f"🎉 {msg}")
        self.finished.emit(True, msg)


# ==================== Matplotlib 图表画布 ====================
class ChartCanvas(FigureCanvas):
    """可交互的图表画布"""

    def __init__(self, parent=None, width=12, height=8, dpi=100):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.fig.set_facecolor('#f8f9fa')
        super().__init__(self.fig)
        self.setParent(parent)
        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Expanding)
        self.axes = []  # 存储所有子图
        self._setup_figure()

    def _setup_figure(self):
        """初始化图表"""
        self.fig.clf()
        self.axes = []

    def clear_all(self):
        """清除所有子图"""
        self.fig.clf()
        self.axes = []
        self.draw()

    def plot_fund_data(self, nav_df: pd.DataFrame, fund_info: Dict = None,
                       indicators: List[str] = None, is_private: bool = False):
        """绘制基金净值数据和指标"""
        self._setup_figure()
        indicators = indicators or []

        if nav_df.empty or 'unit_nav' not in nav_df.columns:
            # 数据缺失 - 显示彩色方块
            self._show_missing_data_block('fund', fund_info)
            self.draw()
            return

        # 确定需要的子图数量
        subplot_count = 1  # 主图
        if 'MACD' in indicators:
            subplot_count += 1
        if 'RSI' in indicators:
            subplot_count += 1
        if 'KDJ' in indicators:
            subplot_count += 1

        # 创建子图
        gs_kw = {'height_ratios': [3] + [1] * (subplot_count - 1)}
        if subplot_count > 1:
            gs_kw['height_ratios'] = [3] + [1] * (subplot_count - 1)

        axes_list = []
        main_ax = self.fig.add_subplot(subplot_count, 1, 1)
        axes_list.append(main_ax)

        for i in range(1, subplot_count):
            ax = self.fig.add_subplot(subplot_count, 1, i + 1, sharex=main_ax)
            axes_list.append(ax)

        self.axes = axes_list

        # === 主图：净值走势 ===
        ax = axes_list[0]
        nav_series = nav_df['unit_nav'].dropna()
        dates = nav_series.index

        # 检查数据缺失区域
        full_date_range = pd.date_range(start=dates.min(), end=dates.max(), freq='B')
        missing_dates = full_date_range.difference(dates)
        if len(missing_dates) > 0:
            # 标记缺失数据区域
            self._mark_missing_periods(ax, missing_dates, nav_series.min(), nav_series.max())

        # 绘制净值曲线
        ax.plot(dates, nav_series.values, color='#2196F3', linewidth=1.5,
                label='单位净值', alpha=0.9)
        ax.fill_between(dates, nav_series.values, nav_series.min() * 0.95,
                        color='#2196F3', alpha=0.1)

        # 移动平均线
        if 'MA' in indicators:
            ma_df = IndicatorCalculator.calc_ma(nav_series, [5, 10, 20, 60])
            colors = ['#FF9800', '#4CAF50', '#9C27B0', '#F44336']
            for col, c in zip(ma_df.columns, colors):
                if col in ma_df.columns:
                    ax.plot(dates, ma_df[col].values, color=c, linewidth=0.8,
                            linestyle='--', label=col, alpha=0.7)

        # 布林带
        if 'BOLL' in indicators:
            bb_df = IndicatorCalculator.calc_bollinger_bands(nav_series)
            ax.plot(dates, bb_df['BB_UPPER'].values, color='#E91E63', linewidth=0.7,
                    linestyle=':', label='布林上轨', alpha=0.6)
            ax.plot(dates, bb_df['BB_MIDDLE'].values, color='#9E9E9E', linewidth=0.7,
                    linestyle=':', label='布林中轨', alpha=0.6)
            ax.plot(dates, bb_df['BB_LOWER'].values, color='#E91E63', linewidth=0.7,
                    linestyle=':', label='布林下轨', alpha=0.6)
            ax.fill_between(dates, bb_df['BB_UPPER'].values, bb_df['BB_LOWER'].values,
                            color='#E91E63', alpha=0.04)

        # 标题和标签
        title = fund_info.get('fund_name', '') if fund_info else ''
        code = fund_info.get('fund_code', '') if fund_info else ''
        ax.set_title(f'{title} ({code}) - 净值走势图', fontsize=13, fontweight='bold', pad=15)
        ax.set_ylabel('单位净值 (元)', fontsize=10)
        ax.legend(loc='upper left', fontsize=8, framealpha=0.8, ncol=2)
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.tick_params(axis='x', rotation=30, labelsize=8)

        # 格式化x轴日期
        ax.xaxis.set_major_formatter(DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(AutoDateLocator())

        # === 指标子图 ===
        subplot_idx = 1

        # MACD
        if 'MACD' in indicators and subplot_idx < len(axes_list):
            ax_macd = axes_list[subplot_idx]
            macd_df = IndicatorCalculator.calc_macd(nav_series)
            ax_macd.plot(dates, macd_df['DIF'].values, color='#2196F3', linewidth=0.8, label='DIF')
            ax_macd.plot(dates, macd_df['DEA'].values, color='#FF9800', linewidth=0.8, label='DEA')
            # MACD柱状图
            colors_bar = ['#F44336' if v >= 0 else '#4CAF50' for v in macd_df['MACD'].values]
            ax_macd.bar(dates, macd_df['MACD'].values, color=colors_bar, width=0.8, alpha=0.7)
            ax_macd.axhline(y=0, color='#9E9E9E', linewidth=0.5, linestyle='-')
            ax_macd.set_ylabel('MACD', fontsize=9)
            ax_macd.legend(loc='upper left', fontsize=7)
            ax_macd.grid(True, alpha=0.3, linestyle='--')
            ax_macd.tick_params(labelsize=7)
            subplot_idx += 1

        # RSI
        if 'RSI' in indicators and subplot_idx < len(axes_list):
            ax_rsi = axes_list[subplot_idx]
            rsi_series = IndicatorCalculator.calc_rsi(nav_series)
            ax_rsi.plot(dates, rsi_series.values, color='#9C27B0', linewidth=0.8, label='RSI(14)')
            ax_rsi.axhline(y=70, color='#F44336', linewidth=0.5, linestyle='--', alpha=0.5)
            ax_rsi.axhline(y=30, color='#4CAF50', linewidth=0.5, linestyle='--', alpha=0.5)
            ax_rsi.axhline(y=50, color='#9E9E9E', linewidth=0.3, linestyle='-', alpha=0.3)
            ax_rsi.fill_between(dates, 70, rsi_series.values,
                                where=(rsi_series.values >= 70), color='#F44336', alpha=0.15)
            ax_rsi.fill_between(dates, 30, rsi_series.values,
                                where=(rsi_series.values <= 30), color='#4CAF50', alpha=0.15)
            ax_rsi.set_ylabel('RSI', fontsize=9)
            ax_rsi.set_ylim(0, 100)
            ax_rsi.legend(loc='upper left', fontsize=7)
            ax_rsi.grid(True, alpha=0.3, linestyle='--')
            ax_rsi.tick_params(labelsize=7)
            subplot_idx += 1

        # KDJ
        if 'KDJ' in indicators and subplot_idx < len(axes_list):
            ax_kdj = axes_list[subplot_idx]
            kdj_df = IndicatorCalculator.calc_kdj(nav_series, nav_series, nav_series)
            ax_kdj.plot(dates, kdj_df['K'].values, color='#2196F3', linewidth=0.8, label='K')
            ax_kdj.plot(dates, kdj_df['D'].values, color='#FF9800', linewidth=0.8, label='D')
            ax_kdj.plot(dates, kdj_df['J'].values, color='#E91E63', linewidth=0.8, label='J')
            ax_kdj.axhline(y=80, color='#F44336', linewidth=0.5, linestyle='--', alpha=0.5)
            ax_kdj.axhline(y=20, color='#4CAF50', linewidth=0.5, linestyle='--', alpha=0.5)
            ax_kdj.set_ylabel('KDJ', fontsize=9)
            ax_kdj.set_ylim(0, 100)
            ax_kdj.legend(loc='upper left', fontsize=7)
            ax_kdj.grid(True, alpha=0.3, linestyle='--')
            ax_kdj.tick_params(labelsize=7)
            subplot_idx += 1

        # 调整布局
        self.fig.tight_layout(pad=2.0)
        self.draw()

    def plot_stock_data(self, stock_df: pd.DataFrame, stock_info: Dict = None,
                        indicators: List[str] = None):
        """绘制股票K线图和数据"""
        self._setup_figure()
        indicators = indicators or []

        if stock_df.empty or 'close' not in stock_df.columns:
            self._show_missing_data_block('stock', stock_info)
            self.draw()
            return

        # 确定子图数量
        subplot_count = 2  # K线主图 + 成交量
        if 'MACD' in indicators:
            subplot_count += 1
        if 'RSI' in indicators:
            subplot_count += 1
        if 'KDJ' in indicators:
            subplot_count += 1

        height_ratios = [3, 1] + [1] * (subplot_count - 2)

        axes_list = []
        for i in range(subplot_count):
            if i == 0:
                ax = self.fig.add_subplot(subplot_count, 1, 1)
            else:
                ax = self.fig.add_subplot(subplot_count, 1, i + 1, sharex=axes_list[0])
            axes_list.append(ax)

        self.axes = axes_list

        dates = stock_df.index
        close_series = stock_df['close']

        # === 主图：K线 ===
        ax = axes_list[0]

        # 简化K线绘制（使用收盘价折线 + 高低影线）
        ax.plot(dates, close_series.values, color='#2196F3', linewidth=1.2, label='收盘价')
        ax.fill_between(dates, stock_df['low'].values, stock_df['high'].values,
                        color='#2196F3', alpha=0.15)

        # 移动平均线
        if 'MA' in indicators:
            ma_df = IndicatorCalculator.calc_ma(close_series, [5, 10, 20, 60])
            colors = ['#FF9800', '#4CAF50', '#9C27B0', '#F44336']
            for col, c in zip(ma_df.columns, colors):
                ax.plot(dates, ma_df[col].values, color=c, linewidth=0.8, linestyle='--',
                        label=col, alpha=0.7)

        # 布林带
        if 'BOLL' in indicators:
            bb_df = IndicatorCalculator.calc_bollinger_bands(close_series)
            ax.plot(dates, bb_df['BB_UPPER'].values, color='#E91E63', linewidth=0.7,
                    linestyle=':', alpha=0.6)
            ax.plot(dates, bb_df['BB_MIDDLE'].values, color='#9E9E9E', linewidth=0.7,
                    linestyle=':', alpha=0.6)
            ax.plot(dates, bb_df['BB_LOWER'].values, color='#E91E63', linewidth=0.7,
                    linestyle=':', alpha=0.6)

        title = stock_info.get('stock_name', '') if stock_info else ''
        code = stock_info.get('stock_code', '') if stock_info else ''
        ax.set_title(f'{title} ({code}) - 日线图', fontsize=13, fontweight='bold', pad=15)
        ax.set_ylabel('价格 (元)', fontsize=10)
        ax.legend(loc='upper left', fontsize=8, framealpha=0.8)
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.tick_params(labelsize=8)

        # === 成交量 ===
        ax_vol = axes_list[1]
        colors_vol = ['#F44336' if close_series.iloc[i] >= close_series.iloc[i - 1]
                      else '#4CAF50' for i in range(1, len(close_series))]
        colors_vol.insert(0, '#9E9E9E')
        ax_vol.bar(dates, stock_df['volume'].values, color=colors_vol, width=0.8, alpha=0.7)
        ax_vol.set_ylabel('成交量', fontsize=9)
        ax_vol.grid(True, alpha=0.3, linestyle='--')
        ax_vol.tick_params(labelsize=7)

        subplot_idx = 2

        # MACD
        if 'MACD' in indicators and subplot_idx < len(axes_list):
            ax_macd = axes_list[subplot_idx]
            macd_df = IndicatorCalculator.calc_macd(close_series)
            ax_macd.plot(dates, macd_df['DIF'].values, color='#2196F3', linewidth=0.8, label='DIF')
            ax_macd.plot(dates, macd_df['DEA'].values, color='#FF9800', linewidth=0.8, label='DEA')
            colors_bar = ['#F44336' if v >= 0 else '#4CAF50' for v in macd_df['MACD'].values]
            ax_macd.bar(dates, macd_df['MACD'].values, color=colors_bar, width=0.8, alpha=0.7)
            ax_macd.axhline(y=0, color='#9E9E9E', linewidth=0.5)
            ax_macd.set_ylabel('MACD', fontsize=9)
            ax_macd.legend(loc='upper left', fontsize=7)
            ax_macd.grid(True, alpha=0.3, linestyle='--')
            ax_macd.tick_params(labelsize=7)
            subplot_idx += 1

        # RSI
        if 'RSI' in indicators and subplot_idx < len(axes_list):
            ax_rsi = axes_list[subplot_idx]
            rsi_series = IndicatorCalculator.calc_rsi(close_series)
            ax_rsi.plot(dates, rsi_series.values, color='#9C27B0', linewidth=0.8)
            ax_rsi.axhline(y=70, color='#F44336', linewidth=0.5, linestyle='--', alpha=0.5)
            ax_rsi.axhline(y=30, color='#4CAF50', linewidth=0.5, linestyle='--', alpha=0.5)
            ax_rsi.set_ylabel('RSI', fontsize=9)
            ax_rsi.set_ylim(0, 100)
            ax_rsi.grid(True, alpha=0.3, linestyle='--')
            ax_rsi.tick_params(labelsize=7)
            subplot_idx += 1

        # KDJ
        if 'KDJ' in indicators and subplot_idx < len(axes_list):
            ax_kdj = axes_list[subplot_idx]
            kdj_df = IndicatorCalculator.calc_kdj(stock_df['high'], stock_df['low'], stock_df['close'])
            ax_kdj.plot(dates, kdj_df['K'].values, color='#2196F3', linewidth=0.8, label='K')
            ax_kdj.plot(dates, kdj_df['D'].values, color='#FF9800', linewidth=0.8, label='D')
            ax_kdj.plot(dates, kdj_df['J'].values, color='#E91E63', linewidth=0.8, label='J')
            ax_kdj.axhline(y=80, color='#F44336', linewidth=0.5, linestyle='--', alpha=0.5)
            ax_kdj.axhline(y=20, color='#4CAF50', linewidth=0.5, linestyle='--', alpha=0.5)
            ax_kdj.set_ylabel('KDJ', fontsize=9)
            ax_kdj.set_ylim(0, 100)
            ax_kdj.legend(loc='upper left', fontsize=7)
            ax_kdj.grid(True, alpha=0.3, linestyle='--')
            ax_kdj.tick_params(labelsize=7)
            subplot_idx += 1

        self.fig.tight_layout(pad=2.0)
        self.draw()

    def _show_missing_data_block(self, data_type: str, info: Dict = None):
        """显示数据缺失的彩色方块"""
        ax = self.fig.add_subplot(111)
        self.axes = [ax]

        # 根据缺失类型使用不同颜色
        if data_type == 'fund':
            is_private = info.get('is_private', 0) if info else 0
            if is_private:
                block_color = '#FF9800'  # 橙色 - 私募基金
                msg = '私募基金 - 数据未公开'
            else:
                block_color = '#F44336'  # 红色 - 数据缺失
                msg = '数据缺失'
        else:
            block_color = '#9C27B0'  # 紫色 - 股票数据缺失
            msg = '股票数据缺失'

        # 绘制大的半透明方块
        rect = mpatches.FancyBboxPatch(
            (0.15, 0.2), 0.7, 0.6,
            boxstyle="round,pad=0.1",
            facecolor=block_color, alpha=0.3,
            edgecolor=block_color, linewidth=2
        )
        ax.add_patch(rect)

        # 添加文字
        code = info.get('fund_code', info.get('stock_code', '')) if info else ''
        name = info.get('fund_name', info.get('stock_name', '')) if info else ''
        ax.text(0.5, 0.55, f'{msg}', ha='center', va='center', fontsize=18,
                fontweight='bold', color=block_color, transform=ax.transAxes)
        ax.text(0.5, 0.42, f'{name} ({code})', ha='center', va='center', fontsize=12,
                color='#757575', transform=ax.transAxes)
        ax.text(0.5, 0.32, '该证券暂无可用历史数据', ha='center', va='center', fontsize=10,
                color='#9E9E9E', transform=ax.transAxes)

        # 添加图例方块
        legend_elements = [
            mpatches.Patch(facecolor='#F44336', alpha=0.3, label='完全缺失'),
            mpatches.Patch(facecolor='#FF9800', alpha=0.3, label='私募/非公开'),
            mpatches.Patch(facecolor='#FFEB3B', alpha=0.3, label='部分缺失'),
            mpatches.Patch(facecolor='#9E9E9E', alpha=0.3, label='数据过期'),
        ]
        ax.legend(handles=legend_elements, loc='lower center', fontsize=8, ncol=4,
                  framealpha=0.8)

        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_xticks([])
        ax.set_yticks([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_visible(False)
        ax.spines['left'].set_visible(False)

    def _mark_missing_periods(self, ax, missing_dates, y_min, y_max):
        """在图表上标记缺失数据的时间段"""
        if len(missing_dates) == 0:
            return

        # 将缺失日期分组为连续区间
        missing_sorted = sorted(missing_dates)
        groups = []
        current_group = [missing_sorted[0]]

        for i in range(1, len(missing_sorted)):
            if (missing_sorted[i] - missing_sorted[i - 1]).days <= 3:
                current_group.append(missing_sorted[i])
            else:
                groups.append(current_group)
