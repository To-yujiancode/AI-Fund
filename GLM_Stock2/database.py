"""
股票数据库管理模块
负责 SQLite 数据库的创建、连接、表结构管理及基本 CRUD 操作。
"""

import sqlite3
import os
import threading
from typing import Optional

# 数据库文件路径（与应用同目录）
DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DB_DIR, "stock_data.db")

# 线程本地存储，确保每个线程使用独立连接
_local = threading.local()


def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """获取当前线程的数据库连接（线程安全）。"""
    if not hasattr(_local, "connection"):
        _local.connection = sqlite3.connect(db_path, timeout=30)
        _local.connection.execute("PRAGMA journal_mode=WAL")
        _local.connection.execute("PRAGMA synchronous=NORMAL")
        _local.connection.execute("PRAGMA cache_size=-64000")  # 64MB 缓存
        _local.connection.row_factory = sqlite3.Row
    return _local.connection


def close_connection():
    """关闭当前线程的数据库连接。"""
    if hasattr(_local, "connection"):
        _local.connection.close()
        del _local.connection


DB_VERSION = 1


def init_db(db_path: str = DB_PATH):
    """初始化数据库，创建表和索引。"""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")

    conn.executescript("""
        -- 数据库版本号
        CREATE TABLE IF NOT EXISTS _meta (
            key   TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        -- 股票基本信息表
        CREATE TABLE IF NOT EXISTS stock_basic (
            code            TEXT PRIMARY KEY,
            name            TEXT NOT NULL,
            industry        TEXT DEFAULT '',
            list_date       TEXT DEFAULT '',
            total_shares    REAL DEFAULT 0,
            float_shares    REAL DEFAULT 0,
            total_market_cap REAL DEFAULT 0,
            float_market_cap REAL DEFAULT 0,
            latest_price    REAL,
            change_pct      REAL,
            pe_ttm          REAL,
            pb              REAL,
            update_time     TEXT DEFAULT (datetime('now', 'localtime'))
        );

        -- 股票日线行情表（OHLC）
        CREATE TABLE IF NOT EXISTS stock_daily (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            code            TEXT NOT NULL,
            date            TEXT NOT NULL,
            open_price      REAL,
            close_price     REAL,
            high_price      REAL,
            low_price       REAL,
            volume          INTEGER,
            turnover        REAL,
            amplitude       REAL,
            change_pct      REAL,
            change_amt      REAL,
            turnover_rate   REAL,
            UNIQUE(code, date)
        );

        -- 索引
        CREATE INDEX IF NOT EXISTS idx_stock_basic_name ON stock_basic(name);
        CREATE INDEX IF NOT EXISTS idx_stock_basic_industry ON stock_basic(industry);
        CREATE INDEX IF NOT EXISTS idx_stock_daily_code ON stock_daily(code);
        CREATE INDEX IF NOT EXISTS idx_stock_daily_code_date ON stock_daily(code, date);
        CREATE INDEX IF NOT EXISTS idx_stock_daily_date ON stock_daily(date);
    """)

    # 数据库迁移
    _migrate_db(conn)

    conn.commit()
    conn.close()
    print(f"[数据库] 初始化完成: {db_path}")


def _migrate_db(conn: sqlite3.Connection):
    """数据库迁移。"""
    row = conn.execute("SELECT value FROM _meta WHERE key = 'version'").fetchone()
    current = int(row[0]) if row else 0

    if current < DB_VERSION:
        # 未来在此添加迁移逻辑
        conn.execute("INSERT OR REPLACE INTO _meta (key, value) VALUES ('version', ?)",
                     (str(DB_VERSION),))
        conn.commit()


# ==================== 股票基本信息 CRUD ====================

def batch_upsert_stock_basic(records: list[dict]):
    """批量插入/更新股票基本信息。"""
    conn = get_connection()
    conn.executemany("""
        INSERT INTO stock_basic (code, name, update_time)
        VALUES (?, ?, datetime('now', 'localtime'))
        ON CONFLICT(code) DO UPDATE SET
            name        = excluded.name,
            update_time = datetime('now', 'localtime')
    """, [(r['code'], r['name']) for r in records])
    conn.commit()


def upsert_stock_detail(
    code: str, name: str = "", industry: str = "",
    list_date: str = "", total_shares: float = 0,
    float_shares: float = 0, total_market_cap: float = 0,
    float_market_cap: float = 0, latest_price: float = 0,
    change_pct: float = 0, pe_ttm: float = 0, pb: float = 0
):
    """插入或更新股票详细信息。"""
    conn = get_connection()
    conn.execute("""
        INSERT INTO stock_basic (code, name, industry, list_date, total_shares, float_shares,
                                 total_market_cap, float_market_cap, latest_price, change_pct,
                                 pe_ttm, pb, update_time)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'))
        ON CONFLICT(code) DO UPDATE SET
            name            = COALESCE(NULLIF(excluded.name, ''), stock_basic.name),
            industry        = COALESCE(NULLIF(excluded.industry, ''), stock_basic.industry),
            list_date       = COALESCE(NULLIF(excluded.list_date, ''), stock_basic.list_date),
            total_shares    = CASE WHEN excluded.total_shares > 0 THEN excluded.total_shares ELSE stock_basic.total_shares END,
            float_shares    = CASE WHEN excluded.float_shares > 0 THEN excluded.float_shares ELSE stock_basic.float_shares END,
            total_market_cap = CASE WHEN excluded.total_market_cap > 0 THEN excluded.total_market_cap ELSE stock_basic.total_market_cap END,
            float_market_cap = CASE WHEN excluded.float_market_cap > 0 THEN excluded.float_market_cap ELSE stock_basic.float_market_cap END,
            latest_price    = CASE WHEN excluded.latest_price > 0 THEN excluded.latest_price ELSE stock_basic.latest_price END,
            change_pct      = CASE WHEN excluded.change_pct != 0 THEN excluded.change_pct ELSE stock_basic.change_pct END,
            pe_ttm          = CASE WHEN excluded.pe_ttm > 0 THEN excluded.pe_ttm ELSE stock_basic.pe_ttm END,
            pb              = CASE WHEN excluded.pb > 0 THEN excluded.pb ELSE stock_basic.pb END,
            update_time     = datetime('now', 'localtime')
    """, (code, name, industry, list_date, total_shares, float_shares,
          total_market_cap, float_market_cap, latest_price, change_pct, pe_ttm, pb))
    conn.commit()


def search_stocks(keyword: str, limit: int = 200) -> list[dict]:
    """按股票代码或名称搜索股票。"""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT code, name, industry, list_date, latest_price, change_pct,
               total_market_cap, pe_ttm, pb
        FROM stock_basic
        WHERE code LIKE ? OR name LIKE ?
        ORDER BY CASE WHEN code = ? THEN 0 WHEN code LIKE ? THEN 1 ELSE 2 END,
                 LENGTH(code)
        LIMIT ?
    """, (f"%{keyword}%", f"%{keyword}%", keyword, f"{keyword}%", limit))
    return [dict(row) for row in cursor.fetchall()]


def get_stock_detail(code: str) -> Optional[dict]:
    """获取股票详细信息。"""
    conn = get_connection()
    cursor = conn.execute("SELECT * FROM stock_basic WHERE code = ?", (code,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_stock_industries() -> list[str]:
    """获取所有股票行业列表。"""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT DISTINCT industry FROM stock_basic WHERE industry != '' ORDER BY industry"
    )
    return [row[0] for row in cursor.fetchall()]


def get_stocks_by_industry(industry: str, limit: int = 500) -> list[dict]:
    """按行业获取股票列表。"""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT code, name, industry, list_date, latest_price, change_pct,
               total_market_cap, pe_ttm, pb
        FROM stock_basic WHERE industry = ? ORDER BY code LIMIT ?
    """, (industry, limit))
    return [dict(row) for row in cursor.fetchall()]


def get_stock_count() -> int:
    """获取股票总数。"""
    conn = get_connection()
    return conn.execute("SELECT COUNT(*) FROM stock_basic").fetchone()[0]


# ==================== 股票日线行情 CRUD ====================

def save_stock_daily(records: list[tuple]):
    """
    保存股票日线行情数据。
    records: [(code, date, open, close, high, low, volume, turnover,
               amplitude, change_pct, change_amt, turnover_rate), ...]
    """
    conn = get_connection()
    conn.executemany("""
        INSERT OR IGNORE INTO stock_daily
            (code, date, open_price, close_price, high_price, low_price,
             volume, turnover, amplitude, change_pct, change_amt, turnover_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()


def get_stock_daily(code: str, start_date: str = "", end_date: str = "") -> list[dict]:
    """获取股票日线行情数据。"""
    conn = get_connection()
    if start_date and end_date:
        cursor = conn.execute("""
            SELECT date, open_price, close_price, high_price, low_price,
                   volume, turnover, amplitude, change_pct, change_amt, turnover_rate
            FROM stock_daily
            WHERE code = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, (code, start_date, end_date))
    elif start_date:
        cursor = conn.execute("""
            SELECT date, open_price, close_price, high_price, low_price,
                   volume, turnover, amplitude, change_pct, change_amt, turnover_rate
            FROM stock_daily WHERE code = ? AND date >= ? ORDER BY date
        """, (code, start_date))
    else:
        cursor = conn.execute("""
            SELECT date, open_price, close_price, high_price, low_price,
                   volume, turnover, amplitude, change_pct, change_amt, turnover_rate
            FROM stock_daily WHERE code = ? ORDER BY date
        """, (code,))
    return [dict(row) for row in cursor.fetchall()]


def get_stock_date_range(code: str) -> tuple[str, str]:
    """获取股票行情日期范围。"""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT MIN(date) as min_date, MAX(date) as max_date FROM stock_daily WHERE code = ?",
        (code,)
    )
    row = cursor.fetchone()
    return (row['min_date'], row['max_date']) if row else ("", "")


def get_latest_stock_date(code: str) -> str:
    """获取股票最新行情日期（增量下载用）。"""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT MAX(date) as max_date FROM stock_daily WHERE code = ?", (code,)
    )
    row = cursor.fetchone()
    return row['max_date'] if row else ""


def get_stock_daily_count(code: str) -> int:
    """获取股票行情记录数。"""
    conn = get_connection()
    cursor = conn.execute("SELECT COUNT(*) as cnt FROM stock_daily WHERE code = ?", (code,))
    return cursor.fetchone()['cnt']


# ==================== 统计信息 ====================

def get_db_stats() -> dict:
    """获取数据库统计信息。"""
    conn = get_connection()
    stock_count = conn.execute("SELECT COUNT(*) as cnt FROM stock_basic").fetchone()['cnt']
    daily_count = conn.execute("SELECT COUNT(*) as cnt FROM stock_daily").fetchone()['cnt']
    stocks_with_daily = conn.execute("SELECT COUNT(DISTINCT code) as cnt FROM stock_daily").fetchone()['cnt']
    stocks_with_detail = conn.execute(
        "SELECT COUNT(*) FROM stock_basic WHERE industry != ''"
    ).fetchone()[0]
    db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    return {
        'stock_count': stock_count,
        'daily_records': daily_count,
        'stocks_with_daily': stocks_with_daily,
        'stocks_with_detail': stocks_with_detail,
        'db_size_mb': round(db_size / 1024 / 1024, 2),
    }
