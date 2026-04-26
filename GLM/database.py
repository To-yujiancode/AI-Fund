"""
基金数据库管理模块
负责 SQLite 数据库的创建、连接、表结构管理及基本 CRUD 操作。
"""

import sqlite3
import os
import threading
from typing import Optional

# 数据库文件路径（与应用同目录）
DB_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(DB_DIR, "fund_data.db")

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


def init_db(db_path: str = DB_PATH):
    """初始化数据库，创建表和索引。"""
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA cache_size=-64000")

    conn.executescript("""
        -- 基金基本信息表
        CREATE TABLE IF NOT EXISTS fund_basic (
            code        TEXT PRIMARY KEY,
            name        TEXT NOT NULL,
            py_abbr     TEXT,
            fund_type   TEXT,
            py_full     TEXT,
            full_name   TEXT,
            found_date  TEXT,
            latest_scale TEXT,
            company     TEXT,
            manager     TEXT,
            trustee     TEXT,
            rating      TEXT,
            strategy    TEXT,
            objective   TEXT,
            benchmark   TEXT,
            update_time TEXT DEFAULT (datetime('now', 'localtime')),
            detail_fetched INTEGER DEFAULT 0,
            fund_category TEXT DEFAULT ''
        );

        -- 基金历史净值表（核心表，数据量最大）
        CREATE TABLE IF NOT EXISTS fund_nav (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            code        TEXT NOT NULL,
            nav_date    TEXT NOT NULL,
            unit_nav    REAL,
            acc_nav     REAL,
            daily_chg   REAL,
            UNIQUE(code, nav_date)
        );

        -- ETF/LOF K线数据表（OHLC）
        CREATE TABLE IF NOT EXISTS fund_ohlc (
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

        -- 索引：按基金代码查询净值
        CREATE INDEX IF NOT EXISTS idx_nav_code ON fund_nav(code);
        -- 索引：按日期查询净值
        CREATE INDEX IF NOT EXISTS idx_nav_date ON fund_nav(nav_date);
        -- 复合索引：同时按代码和日期查询
        CREATE INDEX IF NOT EXISTS idx_nav_code_date ON fund_nav(code, nav_date);
        -- 基金类型索引
        CREATE INDEX IF NOT EXISTS idx_basic_type ON fund_basic(fund_type);
        -- 基金名称索引（模糊查询用）
        CREATE INDEX IF NOT EXISTS idx_basic_name ON fund_basic(name);
        -- OHLC 索引
        CREATE INDEX IF NOT EXISTS idx_ohlc_code ON fund_ohlc(code);
        CREATE INDEX IF NOT EXISTS idx_ohlc_code_date ON fund_ohlc(code, date);
        CREATE INDEX IF NOT EXISTS idx_ohlc_date ON fund_ohlc(date);
    """)

    # 数据库迁移：为旧表添加新字段
    _migrate_db(conn)

    conn.commit()
    conn.close()
    print(f"[数据库] 初始化完成: {db_path}")


def _migrate_db(conn: sqlite3.Connection):
    """数据库迁移：为旧表添加新增字段。"""
    # 获取已有列名
    basic_cols = {row[1] for row in conn.execute("PRAGMA table_info(fund_basic)").fetchall()}
    if 'fund_category' not in basic_cols:
        conn.execute("ALTER TABLE fund_basic ADD COLUMN fund_category TEXT DEFAULT ''")
        print("[迁移] fund_basic 表新增 fund_category 字段")
    conn.commit()


# ==================== 基金分类 (ETF/LOF) ====================

def get_etf_lof_codes() -> tuple[list[str], list[str]]:
    """
    从数据库中识别 ETF 和 LOF 基金代码。
    ETF 代码以 51xxxx 或 159xxx 开头，或 fund_category='etf'。
    LOF 名称含 '(LOF)' 或 fund_category='lof'。
    返回 (etf_codes, lof_codes)。
    """
    conn = get_connection()
    rows = conn.execute(
        "SELECT code, name, fund_category FROM fund_basic ORDER BY code"
    ).fetchall()

    etf_codes, lof_codes = [], []
    for row in rows:
        code = row['code']
        name = row['name']
        category = row['fund_category'] or ''

        if category == 'etf':
            etf_codes.append(code)
        elif category == 'lof':
            lof_codes.append(code)
        elif '(LOF)' in name or '(lof)' in name:
            lof_codes.append(code)
        elif code.startswith('51') or code.startswith('159'):
            etf_codes.append(code)

    return etf_codes, lof_codes


def update_fund_category(code: str, category: str):
    """更新基金分类标记。"""
    conn = get_connection()
    conn.execute(
        "UPDATE fund_basic SET fund_category = ? WHERE code = ?",
        (category, code)
    )
    conn.commit()


# ==================== 基金基本信息 CRUD ====================

def upsert_fund_basic(
    code: str, name: str, py_abbr: str = "", fund_type: str = "",
    py_full: str = "", full_name: str = "", found_date: str = "",
    latest_scale: str = "", company: str = "", manager: str = "",
    trustee: str = "", rating: str = "", strategy: str = "",
    objective: str = "", benchmark: str = "", detail_fetched: int = 0
):
    """插入或更新基金基本信息。"""
    conn = get_connection()
    conn.execute("""
        INSERT INTO fund_basic (code, name, py_abbr, fund_type, py_full, full_name,
                                found_date, latest_scale, company, manager, trustee,
                                rating, strategy, objective, benchmark, update_time,
                                detail_fetched)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now', 'localtime'), ?)
        ON CONFLICT(code) DO UPDATE SET
            name        = excluded.name,
            py_abbr     = excluded.py_abbr,
            fund_type   = excluded.fund_type,
            py_full     = excluded.py_full,
            full_name   = COALESCE(NULLIF(excluded.full_name, ''), fund_basic.full_name),
            found_date  = COALESCE(NULLIF(excluded.found_date, ''), fund_basic.found_date),
            latest_scale= COALESCE(NULLIF(excluded.latest_scale, ''), fund_basic.latest_scale),
            company     = COALESCE(NULLIF(excluded.company, ''), fund_basic.company),
            manager     = COALESCE(NULLIF(excluded.manager, ''), fund_basic.manager),
            trustee     = COALESCE(NULLIF(excluded.trustee, ''), fund_basic.trustee),
            rating      = COALESCE(NULLIF(excluded.rating, ''), fund_basic.rating),
            strategy    = COALESCE(NULLIF(excluded.strategy, ''), fund_basic.strategy),
            objective   = COALESCE(NULLIF(excluded.objective, ''), fund_basic.objective),
            benchmark   = COALESCE(NULLIF(excluded.benchmark, ''), fund_basic.benchmark),
            update_time = datetime('now', 'localtime'),
            detail_fetched = excluded.detail_fetched
    """, (code, name, py_abbr, fund_type, py_full, full_name,
          found_date, latest_scale, company, manager, trustee,
          rating, strategy, objective, benchmark, detail_fetched))
    conn.commit()


def batch_upsert_fund_basic(records: list[dict]):
    """批量插入/更新基金基本信息。"""
    conn = get_connection()
    conn.executemany("""
        INSERT INTO fund_basic (code, name, py_abbr, fund_type, py_full,
                                update_time, detail_fetched)
        VALUES (?, ?, ?, ?, ?, datetime('now', 'localtime'), 0)
        ON CONFLICT(code) DO UPDATE SET
            name        = excluded.name,
            py_abbr     = excluded.py_abbr,
            fund_type   = excluded.fund_type,
            py_full     = excluded.py_full,
            update_time = datetime('now', 'localtime')
    """, [(r['code'], r['name'], r.get('py_abbr', ''), r.get('fund_type', ''),
           r.get('py_full', '')) for r in records])
    conn.commit()


def search_funds(keyword: str, limit: int = 200) -> list[dict]:
    """按基金代码或名称搜索基金。"""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT code, name, fund_type, company, found_date, latest_scale
        FROM fund_basic
        WHERE code LIKE ? OR name LIKE ?
        ORDER BY CASE WHEN code = ? THEN 0 WHEN code LIKE ? THEN 1 ELSE 2 END,
                 LENGTH(code)
        LIMIT ?
    """, (f"%{keyword}%", f"%{keyword}%", keyword, f"{keyword}%", limit))
    return [dict(row) for row in cursor.fetchall()]


def get_fund_detail(code: str) -> Optional[dict]:
    """获取基金详细信息。"""
    conn = get_connection()
    cursor = conn.execute("SELECT * FROM fund_basic WHERE code = ?", (code,))
    row = cursor.fetchone()
    return dict(row) if row else None


def get_fund_types() -> list[str]:
    """获取所有基金类型列表。"""
    conn = get_connection()
    cursor = conn.execute("SELECT DISTINCT fund_type FROM fund_basic WHERE fund_type != '' ORDER BY fund_type")
    return [row[0] for row in cursor.fetchall()]


def get_fund_count_by_type() -> list[tuple]:
    """获取各类型基金数量统计。"""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT fund_type, COUNT(*) as cnt
        FROM fund_basic
        GROUP BY fund_type
        ORDER BY cnt DESC
    """)
    return cursor.fetchall()


def get_funds_by_type(fund_type: str, limit: int = 500) -> list[dict]:
    """按类型获取基金列表。"""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT code, name, fund_type, company, found_date, latest_scale
        FROM fund_basic
        WHERE fund_type = ?
        ORDER BY code
        LIMIT ?
    """, (fund_type, limit))
    return [dict(row) for row in cursor.fetchall()]


# ==================== 基金历史净值 CRUD ====================

def save_fund_nav(records: list[tuple]):
    """
    保存基金历史净值数据。
    records: [(code, nav_date, unit_nav, acc_nav, daily_chg), ...]
    使用 INSERT OR IGNORE 避免重复数据。
    """
    conn = get_connection()
    conn.executemany("""
        INSERT OR IGNORE INTO fund_nav (code, nav_date, unit_nav, acc_nav, daily_chg)
        VALUES (?, ?, ?, ?, ?)
    """, records)
    conn.commit()


def get_fund_nav(code: str, start_date: str = "", end_date: str = "") -> list[dict]:
    """
    获取基金历史净值数据。
    可选日期范围筛选。
    """
    conn = get_connection()
    if start_date and end_date:
        cursor = conn.execute("""
            SELECT nav_date, unit_nav, acc_nav, daily_chg
            FROM fund_nav
            WHERE code = ? AND nav_date >= ? AND nav_date <= ?
            ORDER BY nav_date
        """, (code, start_date, end_date))
    elif start_date:
        cursor = conn.execute("""
            SELECT nav_date, unit_nav, acc_nav, daily_chg
            FROM fund_nav
            WHERE code = ? AND nav_date >= ?
            ORDER BY nav_date
        """, (code, start_date))
    else:
        cursor = conn.execute("""
            SELECT nav_date, unit_nav, acc_nav, daily_chg
            FROM fund_nav
            WHERE code = ?
            ORDER BY nav_date
        """, (code,))
    return [dict(row) for row in cursor.fetchall()]


def get_nav_date_range(code: str) -> tuple[str, str]:
    """获取基金净值的日期范围。"""
    conn = get_connection()
    cursor = conn.execute("""
        SELECT MIN(nav_date) as min_date, MAX(nav_date) as max_date
        FROM fund_nav WHERE code = ?
    """, (code,))
    row = cursor.fetchone()
    return (row['min_date'], row['max_date']) if row else ("", "")


def get_latest_nav_date(code: str) -> str:
    """获取基金最新净值日期（用于增量下载判断）。"""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT MAX(nav_date) as max_date FROM fund_nav WHERE code = ?", (code,)
    )
    row = cursor.fetchone()
    return row['max_date'] if row else ""


def get_nav_count(code: str) -> int:
    """获取基金净值记录数。"""
    conn = get_connection()
    cursor = conn.execute("SELECT COUNT(*) as cnt FROM fund_nav WHERE code = ?", (code,))
    return cursor.fetchone()['cnt']


def get_nav_total_count() -> int:
    """获取总净值记录数。"""
    conn = get_connection()
    cursor = conn.execute("SELECT COUNT(*) as cnt FROM fund_nav")
    return cursor.fetchone()['cnt']


def get_funds_with_nav_count() -> int:
    """获取已有历史数据的基金数量。"""
    conn = get_connection()
    cursor = conn.execute("SELECT COUNT(DISTINCT code) as cnt FROM fund_nav")
    return cursor.fetchone()['cnt']


# ==================== ETF/LOF OHLC 数据 CRUD ====================

def save_fund_ohlc(records: list[tuple]):
    """
    保存 ETF/LOF 的 OHLC K线数据。
    records: [(code, date, open, close, high, low, volume, turnover,
               amplitude, change_pct, change_amt, turnover_rate), ...]
    """
    conn = get_connection()
    conn.executemany("""
        INSERT OR IGNORE INTO fund_ohlc
            (code, date, open_price, close_price, high_price, low_price,
             volume, turnover, amplitude, change_pct, change_amt, turnover_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()


def get_fund_ohlc(code: str, start_date: str = "", end_date: str = "") -> list[dict]:
    """获取 ETF/LOF 的 OHLC 数据。"""
    conn = get_connection()
    if start_date and end_date:
        cursor = conn.execute("""
            SELECT date, open_price, close_price, high_price, low_price,
                   volume, turnover, amplitude, change_pct, change_amt, turnover_rate
            FROM fund_ohlc
            WHERE code = ? AND date >= ? AND date <= ?
            ORDER BY date
        """, (code, start_date, end_date))
    elif start_date:
        cursor = conn.execute("""
            SELECT date, open_price, close_price, high_price, low_price,
                   volume, turnover, amplitude, change_pct, change_amt, turnover_rate
            FROM fund_ohlc
            WHERE code = ? AND date >= ?
            ORDER BY date
        """, (code, start_date))
    else:
        cursor = conn.execute("""
            SELECT date, open_price, close_price, high_price, low_price,
                   volume, turnover, amplitude, change_pct, change_amt, turnover_rate
            FROM fund_ohlc
            WHERE code = ?
            ORDER BY date
        """, (code,))
    return [dict(row) for row in cursor.fetchall()]


def get_ohlc_date_range(code: str) -> tuple[str, str]:
    """获取 OHLC 数据的日期范围。"""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT MIN(date) as min_date, MAX(date) as max_date FROM fund_ohlc WHERE code = ?",
        (code,)
    )
    row = cursor.fetchone()
    return (row['min_date'], row['max_date']) if row else ("", "")


def get_latest_ohlc_date(code: str) -> str:
    """获取 OHLC 最新日期（用于增量下载）。"""
    conn = get_connection()
    cursor = conn.execute(
        "SELECT MAX(date) as max_date FROM fund_ohlc WHERE code = ?", (code,)
    )
    row = cursor.fetchone()
    return row['max_date'] if row else ""


def get_ohlc_count(code: str) -> int:
    """获取 OHLC 记录数。"""
    conn = get_connection()
    cursor = conn.execute("SELECT COUNT(*) as cnt FROM fund_ohlc WHERE code = ?", (code,))
    return cursor.fetchone()['cnt']


def get_db_stats() -> dict:
    """获取数据库统计信息。"""
    conn = get_connection()
    basic_count = conn.execute("SELECT COUNT(*) as cnt FROM fund_basic").fetchone()['cnt']
    nav_count = conn.execute("SELECT COUNT(*) as cnt FROM fund_nav").fetchone()['cnt']
    funds_with_nav = conn.execute("SELECT COUNT(DISTINCT code) as cnt FROM fund_nav").fetchone()['cnt']
    ohlc_count = conn.execute("SELECT COUNT(*) as cnt FROM fund_ohlc").fetchone()['cnt']
    funds_with_ohlc = conn.execute("SELECT COUNT(DISTINCT code) as cnt FROM fund_ohlc").fetchone()['cnt']
    db_size = os.path.getsize(DB_PATH) if os.path.exists(DB_PATH) else 0
    return {
        'fund_count': basic_count,
        'nav_records': nav_count,
        'funds_with_nav': funds_with_nav,
        'ohlc_records': ohlc_count,
        'funds_with_ohlc': funds_with_ohlc,
        'db_size_mb': round(db_size / 1024 / 1024, 2),
    }
