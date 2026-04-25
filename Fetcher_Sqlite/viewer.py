"""
基金数据交互式查看器
基于 tkinter + matplotlib，支持：
- 基金搜索（按代码/名称）
- 按类型筛选
- 基金详细信息展示
- 历史净值图表（鼠标框选放大/缩小/平移）
- K线图 + 技术指标（布林带/MACD/RSI/成交量）
- 在线下载单只基金数据
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
import threading
import sys
import os
import numpy as np

import matplotlib
matplotlib.use('TkAgg')
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
from matplotlib.gridspec import GridSpec
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import platform

# ============ 配置中文字体（自动适配 Windows / Linux / macOS）============
def _setup_chinese_font():
    system = platform.system()
    candidates = {
        'Windows': ['SimHei', 'Microsoft YaHei', 'SimSun', 'KaiTi'],
        'Linux': ['Noto Sans CJK SC', 'Noto Sans SC', 'WenQuanYi Micro Hei',
                   'WenQuanYi Zen Hei', 'Droid Sans Fallback'],
        'Darwin': ['PingFang SC', 'Heiti SC', 'STHeiti', 'Apple SD Gothic Neo'],
    }
    available = {f.name for f in matplotlib.font_manager.fontManager.ttflist}
    preferred = candidates.get(system, []) + candidates.get('Linux', [])
    chosen = None
    for font_name in preferred:
        if font_name in available:
            chosen = font_name
            break
    if chosen:
        plt.rcParams['font.sans-serif'] = [chosen, 'DejaVu Sans']
    else:
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

_setup_chinese_font()

from database import (
    init_db, search_funds, get_fund_detail, get_fund_nav,
    get_nav_date_range, get_nav_count, get_fund_types,
    get_funds_by_type, get_db_stats, close_connection,
    get_fund_ohlc, get_ohlc_date_range, get_ohlc_count,
    get_latest_nav_date, get_latest_ohlc_date
)
from indicators import ma, ema, bollinger_bands, macd_calc as macd, rsi

# 配色方案
C = {
    'bg': '#1e1e2e', 'panel': '#2a2a3d', 'input': '#363650', 'card': '#313148',
    'txt': '#cdd6f4', 'txt2': '#a6adc8', 'accent': '#89b4fa',
    'green': '#a6e3a1', 'red': '#f38ba8', 'yellow': '#f9e2af',
    'purple': '#cba6f7', 'teal': '#94e2d5', 'peach': '#fab387',
    'border': '#45475a',
    'ma5': '#f38ba8', 'ma10': '#f9e2af', 'ma20': '#89b4fa', 'ma60': '#cba6f7',
    'grid': '#45475a',
}


class FundViewerApp:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("基金数据查看器 - K线/技术指标版")
        self.root.geometry("1280x860")
        self.root.minsize(1000, 700)
        self.root.configure(bg=C['bg'])

        self.current_fund_code = None
        self.has_ohlc = False  # 当前基金是否有 OHLC 数据

        self._setup_styles()
        init_db()
        self._build_ui()

        self.root.bind('<Return>', lambda e: self._on_search())
        self.root.bind('<Escape>', lambda e: self._on_reset_view())
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

    # ==================== 样式 ====================

    def _setup_styles(self):
        style = ttk.Style()
        style.theme_use('clam')
        style.configure('TFrame', background=C['bg'])
        style.configure('Panel.TFrame', background=C['panel'])
        style.configure('Card.TFrame', background=C['card'])
        style.configure('TLabel', background=C['bg'], foreground=C['txt'],
                         font=('Microsoft YaHei', 9))
        style.configure('Title.TLabel', background=C['bg'], foreground=C['accent'],
                         font=('Microsoft YaHei', 14, 'bold'))
        style.configure('Section.TLabel', background=C['panel'], foreground=C['accent'],
                         font=('Microsoft YaHei', 10, 'bold'))
        style.configure('Info.TLabel', background=C['card'], foreground=C['txt'],
                         font=('Microsoft YaHei', 9))
        style.configure('InfoV.TLabel', background=C['card'], foreground=C['yellow'],
                         font=('Microsoft YaHei', 9, 'bold'))
        style.configure('Stats.TLabel', background=C['bg'], foreground=C['txt2'],
                         font=('Consolas', 8))
        style.configure('Status.TLabel', background=C['panel'], foreground=C['txt2'],
                         font=('Microsoft YaHei', 9))
        style.configure('TEntry', fieldbackground=C['input'], foreground=C['txt'],
                         insertcolor=C['txt'])
        style.configure('TButton', background=C['accent'], foreground='#1e1e2e',
                         font=('Microsoft YaHei', 9, 'bold'), borderwidth=0, padding=(12, 6))
        style.map('TButton', background=[('active', '#b4d0fb'), ('pressed', '#6c9ce8')])
        style.configure('Green.TButton', background=C['green'])
        style.map('Green.TButton', background=[('active', '#c4f0bf')])
        style.configure('Yellow.TButton', background=C['yellow'], foreground='#1e1e2e')
        style.map('Yellow.TButton', background=[('active', '#fbecc0')])
        style.configure('Red.TButton', background=C['red'])
        style.map('Red.TButton', background=[('active', '#f7bac5')])
        style.configure('Treeview', background=C['card'], foreground=C['txt'],
                         fieldbackground=C['card'], borderwidth=0,
                         font=('Microsoft YaHei', 9), rowheight=28)
        style.configure('Treeview.Heading', background=C['panel'], foreground=C['accent'],
                         font=('Microsoft YaHei', 9, 'bold'), borderwidth=0)
        style.map('Treeview', background=[('selected', C['accent'])],
                   foreground=[('selected', '#1e1e2e')])
        style.configure('TCombobox', fieldbackground=C['input'], background=C['input'],
                         foreground=C['txt'], selectbackground=C['accent'])
        style.configure('TRadiobutton', background=C['bg'], foreground=C['txt'],
                         font=('Microsoft YaHei', 9))
        style.map('TRadiobutton', background=[('active', C['bg'])])

    # ==================== 界面构建 ====================

    def _build_ui(self):
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # ====== 搜索栏 ======
        sf = ttk.Frame(main_frame)
        sf.pack(fill=tk.X, pady=(0, 6))

        self.search_var = tk.StringVar(value="输入基金代码或名称...")
        se = ttk.Entry(sf, textvariable=self.search_var, width=30,
                        font=('Microsoft YaHei', 10))
        se.pack(side=tk.LEFT, padx=(0, 6), ipady=3)
        se.bind('<FocusIn>', lambda e: self.search_var.set('') if self.search_var.get() == '输入基金代码或名称...' else None)
        se.bind('<FocusOut>', lambda e: self.search_var.set('输入基金代码或名称...') if not self.search_var.get() else None)

        ttk.Button(sf, text="搜索", command=self._on_search).pack(side=tk.LEFT, padx=2)
        ttk.Button(sf, text="重置视图", command=self._on_reset_view,
                   style='Yellow.TButton').pack(side=tk.LEFT, padx=2)

        ttk.Label(sf, text=" 类型:").pack(side=tk.LEFT, padx=(12, 2))
        self.type_var = tk.StringVar(value="全部")
        tc = ttk.Combobox(sf, textvariable=self.type_var, width=14, state='readonly')
        tc['values'] = ["全部"] + get_fund_types()
        tc.pack(side=tk.LEFT, padx=2)
        tc.bind('<<ComboboxSelected>>', lambda e: self._on_type_filter())

        bf = ttk.Frame(sf)
        bf.pack(side=tk.RIGHT, padx=4)
        ttk.Button(bf, text="更新列表", command=self._update_fund_list,
                   style='Green.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(bf, text="下载净值", command=self._download_nav,
                   style='TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(bf, text="下载K线", command=self._download_ohlc,
                   style='Red.TButton').pack(side=tk.LEFT, padx=2)

        # ====== 主体 ======
        paned = ttk.PanedWindow(main_frame, orient=tk.VERTICAL)
        paned.pack(fill=tk.BOTH, expand=True)

        # -- 上部：列表 + 详情 --
        top = ttk.Frame(paned)
        paned.add(top, weight=1)

        # 基金列表
        tf = ttk.Frame(top)
        tf.pack(fill=tk.BOTH, expand=True, pady=(0, 3))

        cols = ('code', 'name', 'type', 'company', 'scale', 'nav', 'ohlc')
        self.tree = ttk.Treeview(tf, columns=cols, show='headings', height=7)
        for cid, text, w in [('code', '代码', 75), ('name', '名称', 180),
                              ('type', '类型', 110), ('company', '公司', 160),
                              ('scale', '规模', 70), ('nav', '净值数据', 70),
                              ('ohlc', 'K线数据', 70)]:
            self.tree.heading(cid, text=text)
            self.tree.column(cid, width=w, minwidth=60, anchor=tk.CENTER if cid in ('code', 'scale', 'nav', 'ohlc') else tk.W)

        ts = ttk.Scrollbar(tf, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=ts.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ts.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.bind('<Double-1>', lambda e: self._on_tree_select())
        self.tree.bind('<Return>', lambda e: self._on_tree_select())

        # 详情面板
        df = ttk.Frame(top, style='Card.TFrame')
        df.pack(fill=tk.X, pady=(3, 0))
        di = ttk.Frame(df, style='Card.TFrame')
        di.pack(fill=tk.X, padx=10, pady=6)
        self.info_labels = {}
        fields = [('code', '基金代码'), ('name', '基金名称'), ('type', '基金类型'),
                  ('found', '成立日期'), ('scale', '最新规模'), ('company', '基金公司'),
                  ('manager', '基金经理'), ('nav_range', '净值范围'), ('nav_cnt', '净值记录'),
                  ('ohlc_range', 'K线范围'), ('ohlc_cnt', 'K线记录')]
        for i, (k, t) in enumerate(fields):
            r, co = i // 4, (i % 4) * 2
            ttk.Label(di, text=f"{t}:", style='Info.TLabel').grid(
                row=r, column=co, sticky=tk.W, padx=(6, 3), pady=2)
            lb = ttk.Label(di, text="--", style='InfoV.TLabel', width=28)
            lb.grid(row=r, column=co + 1, sticky=tk.W, padx=(0, 12), pady=2)
            self.info_labels[k] = lb

        # -- 下部：图表 --
        chart_f = ttk.Frame(paned)
        paned.add(chart_f, weight=3)

        # 图表控制栏
        ctrl = ttk.Frame(chart_f)
        ctrl.pack(fill=tk.X, padx=4, pady=2)

        # 净值图表模式
        ttk.Label(ctrl, text="净值:").pack(side=tk.LEFT, padx=(4, 2))
        self.nav_mode_var = tk.StringVar(value='nav')
        for text, val in [('单位净值', 'nav'), ('累计净值', 'acc_nav'), ('日收益率', 'daily_chg')]:
            ttk.Radiobutton(ctrl, text=text, variable=self.nav_mode_var,
                             value=val, command=self._redraw_chart).pack(side=tk.LEFT, padx=2)

        ttk.Separator(ctrl, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)

        # K线/技术指标模式
        ttk.Label(ctrl, text="K线/指标:").pack(side=tk.LEFT, padx=(4, 2))
        self.ohlc_mode_var = tk.StringVar(value='kline')
        for text, val in [('K线+均线', 'kline'), ('布林带', 'boll'),
                          ('MACD', 'macd'), ('RSI', 'rsi'), ('成交量', 'vol')]:
            ttk.Radiobutton(ctrl, text=text, variable=self.ohlc_mode_var,
                             value=val, command=self._redraw_chart).pack(side=tk.LEFT, padx=2)

        ttk.Separator(ctrl, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)

        # 日期范围
        ttk.Label(ctrl, text="日期:").pack(side=tk.LEFT, padx=(4, 2))
        self.date_start = tk.StringVar()
        self.date_end = tk.StringVar()
        ttk.Entry(ctrl, textvariable=self.date_start, width=11).pack(side=tk.LEFT, padx=2)
        ttk.Label(ctrl, text="~").pack(side=tk.LEFT)
        ttk.Entry(ctrl, textvariable=self.date_end, width=11).pack(side=tk.LEFT, padx=2)
        ttk.Button(ctrl, text="应用", command=self._redraw_chart).pack(side=tk.LEFT, padx=4)

        # 快捷日期按钮
        for label, days in [('近3月', 90), ('近1年', 365), ('近3年', 1095), ('全部', 0)]:
            ttk.Button(ctrl, text=label,
                       command=lambda d=days: self._set_quick_date(d)).pack(side=tk.LEFT, padx=1)

        # matplotlib 图表
        self.fig = Figure(figsize=(12, 5), dpi=100, facecolor=C['bg'])
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_f)

        toolbar_f = ttk.Frame(chart_f)
        toolbar_f.pack(side=tk.BOTTOM, fill=tk.X)
        self.toolbar = NavigationToolbar2Tk(self.canvas, toolbar_f)
        self.toolbar.update()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # ====== 状态栏 ======
        sf2 = ttk.Frame(main_frame, style='Panel.TFrame')
        sf2.pack(fill=tk.X, pady=(6, 0))
        self.status_var = tk.StringVar(value="就绪 | 搜索基金或按类型筛选")
        self.stats_var = tk.StringVar(value="")
        self._update_stats()
        ttk.Label(sf2, textvariable=self.status_var, style='Status.TLabel').pack(
            side=tk.LEFT, padx=10, pady=4)
        ttk.Label(sf2, textvariable=self.stats_var, style='Stats.TLabel').pack(
            side=tk.RIGHT, padx=10, pady=4)

        self._show_placeholder()

    # ==================== 图表样式 ====================

    def _style_ax(self, ax):
        ax.set_facecolor(C['bg'])
        ax.tick_params(colors=C['txt2'], labelsize=8)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['bottom'].set_color(C['border'])
        ax.spines['left'].set_color(C['border'])
        ax.grid(True, alpha=0.12, color=C['grid'])

    def _show_placeholder(self):
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        self._style_ax(ax)
        ax.text(0.5, 0.5,
                '请搜索并选择基金查看数据\n\n'
                '支持图表:\n'
                '  净值走势 / 累计净值 / 日收益率\n'
                '  K线图+均线 / 布林带 / MACD / RSI / 成交量\n\n'
                '操作: 鼠标框选放大 | 滚轮缩放 | 右键平移 | Home键还原',
                transform=ax.transAxes, ha='center', va='center',
                fontsize=13, color=C['txt2'], alpha=0.6,
                linespacing=1.6)
        ax.set_xticks([])
        ax.set_yticks([])
        self.fig.tight_layout()
        self.canvas.draw()

    # ==================== 事件处理 ====================

    def _set_status(self, t):
        self.status_var.set(t)
        self.root.update_idletasks()

    def _update_stats(self):
        try:
            s = get_db_stats()
            parts = [f"基金: {s['fund_count']:,}"]
            if s.get('funds_with_nav'):
                parts.append(f"净值: {s['funds_with_nav']:,}只/{s['nav_records']:,}条")
            if s.get('funds_with_ohlc'):
                parts.append(f"K线: {s['funds_with_ohlc']:,}只/{s['ohlc_records']:,}条")
            parts.append(f"DB: {s['db_size_mb']}MB")
            self.stats_var.set(" | ".join(parts))
        except Exception:
            pass

    def _on_search(self):
        kw = self.search_var.get().strip()
        if kw in ('', '输入基金代码或名称...'):
            return
        self._set_status(f"搜索: {kw}...")
        results = search_funds(kw)
        if not results:
            self._set_status(f"未找到: {kw}")
            return
        for item in self.tree.get_children():
            self.tree.delete(item)
        for f in results:
            nc = get_nav_count(f['code'])
            oc = get_ohlc_count(f['code'])
            self.tree.insert('', tk.END, values=(
                f['code'], f['name'], f.get('fund_type', ''),
                f.get('company', ''), f.get('latest_scale', ''),
                f"{nc:,}" if nc > 0 else '-',
                f"{oc:,}" if oc > 0 else '-'))
        self._set_status(f"找到 {len(results)} 只基金")
        if len(results) == 1:
            self.tree.selection_set(self.tree.get_children()[0])
            self._on_tree_select()

    def _on_type_filter(self):
        ft = self.type_var.get()
        if ft == "全部":
            return
        results = get_funds_by_type(ft)
        for item in self.tree.get_children():
            self.tree.delete(item)
        for f in results:
            nc = get_nav_count(f['code'])
            oc = get_ohlc_count(f['code'])
            self.tree.insert('', tk.END, values=(
                f['code'], f['name'], f['fund_type'], f.get('company', ''),
                f.get('latest_scale', ''), f"{nc:,}" if nc > 0 else '-',
                f"{oc:,}" if oc > 0 else '-'))
        self._set_status(f"{ft}: {len(results)} 只")

    def _on_tree_select(self):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], 'values')
        code = str(vals[0])
        self.current_fund_code = code
        self._set_status(f"加载 {code}...")

        detail = get_fund_detail(code)
        if detail:
            self.info_labels['code'].config(text=code)
            self.info_labels['name'].config(text=detail.get('name', '--'))
            self.info_labels['type'].config(text=detail.get('fund_type', '--'))
            self.info_labels['found'].config(text=detail.get('found_date', '--'))
            self.info_labels['scale'].config(text=detail.get('latest_scale', '--'))
            self.info_labels['company'].config(text=detail.get('company', '--'))
            self.info_labels['manager'].config(text=detail.get('manager', '--'))

        nc = get_nav_count(code)
        nr = get_nav_date_range(code)
        self.info_labels['nav_cnt'].config(text=f"{nc:,}")
        self.info_labels['nav_range'].config(text=f"{nr[0]} ~ {nr[1]}" if nr[0] else "无数据")

        oc = get_ohlc_count(code)
        orr = get_ohlc_date_range(code)
        self.info_labels['ohlc_cnt'].config(text=f"{oc:,}")
        self.info_labels['ohlc_range'].config(text=f"{orr[0]} ~ {orr[1]}" if orr[0] else "无数据")
        self.has_ohlc = oc > 0

        # 默认日期范围
        if nr[0]:
            self.date_start.set(nr[0])
            self.date_end.set(nr[1])

        self._redraw_chart()
        data_parts = []
        if nc > 0:
            data_parts.append(f"净值 {nc:,} 条")
        if oc > 0:
            data_parts.append(f"K线 {oc:,} 条")
        self._set_status(f"{code} | {', '.join(data_parts) if data_parts else '暂无数据，请点击下载'}")

    def _set_quick_date(self, days):
        if not self.current_fund_code:
            return
        code = self.current_fund_code
        if days == 0:
            nr = get_nav_date_range(code)
            orr = get_ohlc_date_range(code)
            start = min(nr[0] or '9999', orr[0] or '9999')
            end = max(nr[1] or '0000', orr[1] or '0000')
            self.date_start.set(start if start != '9999' else '')
            self.date_end.set(end if end != '0000' else '')
        else:
            from datetime import datetime
            end = datetime.now().strftime('%Y-%m-%d')
            start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            self.date_start.set(start)
            self.date_end.set(end)
        self._redraw_chart()

    def _redraw_chart(self):
        if not self.current_fund_code:
            return
        code = self.current_fund_code
        # 判断使用哪种模式
        ohlc_mode = self.ohlc_mode_var.get()
        nav_mode = self.nav_mode_var.get()

        if self.has_ohlc and ohlc_mode != 'kline_only':
            # OHLC 模式优先
            self._draw_ohlc_chart(code, ohlc_mode)
        elif not self.has_ohlc and ohlc_mode in ('kline', 'boll', 'macd', 'rsi', 'vol'):
            # 没有 OHLC 数据时，K线相关模式使用净值数据模拟
            self._draw_nav_chart(code, nav_mode)
            self._set_status(f"{code} 无K线数据，请点击「下载K线」获取ETF/LOF数据")
        else:
            self._draw_nav_chart(code, nav_mode)

    # ==================== 净值图表 ====================

    def _draw_nav_chart(self, code, mode):
        self.fig.clear()
        gs = GridSpec(1, 1, figure=self.fig)
        ax = self.fig.add_subplot(gs[0, 0])
        self._style_ax(ax)

        start = self.date_start.get().strip()
        end = self.date_end.get().strip()
        nav_data = get_fund_nav(code, start, end)

        if not nav_data:
            ax.text(0.5, 0.5, '无历史净值数据', transform=ax.transAxes,
                    ha='center', va='center', fontsize=14, color=C['txt2'])
            self.fig.tight_layout()
            self.canvas.draw()
            return

        dates = [datetime.strptime(d['nav_date'], '%Y-%m-%d') for d in nav_data]
        detail = get_fund_detail(code)
        fname = detail.get('name', '') if detail else ''

        if mode == 'nav':
            vals = [d['unit_nav'] for d in nav_data]
            ax.fill_between(dates, vals, alpha=0.12, color=C['accent'])
            ax.plot(dates, vals, color=C['accent'], linewidth=1.2)
            ax.set_ylabel('单位净值 (元)', color=C['txt2'], fontsize=9)
            title = '单位净值走势'
        elif mode == 'acc_nav':
            vals = [d['acc_nav'] for d in nav_data if d.get('acc_nav') is not None]
            if not vals:
                vals = [d['unit_nav'] for d in nav_data]
                title = '单位净值走势（无累计净值）'
            else:
                dates = dates[:len(vals)]
                title = '累计净值走势'
            ax.fill_between(dates, vals, alpha=0.12, color=C['teal'])
            ax.plot(dates, vals, color=C['teal'], linewidth=1.2)
            ax.set_ylabel('累计净值 (元)', color=C['txt2'], fontsize=9)
        else:  # daily_chg
            vals = [d['daily_chg'] for d in nav_data if d.get('daily_chg') is not None]
            if vals:
                dates = dates[:len(vals)]
            colors = [C['green'] if v >= 0 else C['red'] for v in vals]
            ax.bar(dates, vals, width=1.5, color=colors, alpha=0.7)
            ax.axhline(y=0, color=C['txt2'], linewidth=0.5, alpha=0.5)
            ax.set_ylabel('日收益率 (%)', color=C['txt2'], fontsize=9)
            title = '日收益率分布'

        ax.set_title(f"{code} {fname} - {title}", color=C['txt'], fontsize=11,
                     fontweight='bold', pad=8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        self.fig.autofmt_xdate(rotation=25)
        self.fig.tight_layout()
        self.canvas.draw()

    # ==================== OHLC K线/技术指标图表 ====================

    def _draw_ohlc_chart(self, code, mode):
        self.fig.clear()
        start = self.date_start.get().strip()
        end = self.date_end.get().strip()
        ohlc_data = get_fund_ohlc(code, start, end)

        if not ohlc_data:
            # 回退到净值模式
            self._draw_nav_chart(code, self.nav_mode_var.get())
            return

        dates = [datetime.strptime(d['date'], '%Y-%m-%d') for d in ohlc_data]
        opens = np.array([d['open_price'] for d in ohlc_data], dtype=float)
        closes = np.array([d['close_price'] for d in ohlc_data], dtype=float)
        highs = np.array([d['high_price'] for d in ohlc_data], dtype=float)
        lows = np.array([d['low_price'] for d in ohlc_data], dtype=float)
        volumes = np.array([d['volume'] if d.get('volume') else 0 for d in ohlc_data], dtype=float)
        change_pcts = np.array([d['change_pct'] if d.get('change_pct') else 0 for d in ohlc_data], dtype=float)

        detail = get_fund_detail(code)
        fname = detail.get('name', '') if detail else ''
        n = len(dates)

        if mode == 'kline':
            self._draw_kline(dates, opens, highs, lows, closes, volumes, change_pcts,
                             code, fname)
        elif mode == 'boll':
            self._draw_boll(dates, closes, volumes, code, fname)
        elif mode == 'macd':
            self._draw_macd(dates, closes, volumes, code, fname)
        elif mode == 'rsi':
            self._draw_rsi(dates, closes, volumes, code, fname)
        elif mode == 'vol':
            self._draw_volume(dates, opens, highs, lows, closes, volumes, change_pcts,
                              code, fname)

    def _draw_kline(self, dates, opens, highs, lows, closes, volumes, change_pcts,
                    code, fname):
        """K线图 + 均线 + 成交量子图。"""
        gs = GridSpec(3, 1, height_ratios=[3, 0.3, 1], hspace=0.05, figure=self.fig)
        ax_price = self.fig.add_subplot(gs[0])
        ax_gap = self.fig.add_subplot(gs[1])
        ax_vol = self.fig.add_subplot(gs[2], sharex=ax_price)

        for ax in [ax_price, ax_gap, ax_vol]:
            self._style_ax(ax)

        x = np.arange(len(dates))
        up = closes >= opens
        down = ~up

        # --- K线 ---
        # 影线 (wicks)
        ax_price.vlines(x[up], lows[up], highs[up], color=C['green'], linewidths=0.6)
        ax_price.vlines(x[down], lows[down], highs[down], color=C['red'], linewidths=0.6)

        # 实体 (bodies)
        body_w = 0.6
        # 上涨 (空心或实心绿)
        if np.any(up):
            bottom_up = opens[up]
            h_up = np.maximum(closes[up] - opens[up], 0.003)  # 最小高度防止看不见
            ax_price.bar(x[up], h_up, bottom=bottom_up, width=body_w,
                         color=C['green'], edgecolor=C['green'], linewidth=0.3)
        # 下跌 (实心红)
        if np.any(down):
            bottom_dn = closes[down]
            h_dn = np.maximum(opens[down] - closes[down], 0.003)
            ax_price.bar(x[down], h_dn, bottom=bottom_dn, width=body_w,
                         color=C['red'], edgecolor=C['red'], linewidth=0.3)

        # 均线
        close_arr = closes
        for period, color, label in [(5, C['ma5'], 'MA5'), (10, C['ma10'], 'MA10'),
                                      (20, C['ma20'], 'MA20'), (60, C['ma60'], 'MA60')]:
            m = ma(close_arr, period)
            valid = ~np.isnan(m)
            if np.any(valid):
                ax_price.plot(x[valid], m[valid], color=color, linewidth=0.9,
                              alpha=0.85, label=label)
        ax_price.legend(loc='upper left', fontsize=8, framealpha=0.3,
                        facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])

        ax_price.set_title(f"{code} {fname} - K线图", color=C['txt'], fontsize=11,
                           fontweight='bold', pad=8)
        ax_price.set_ylabel('价格', color=C['txt2'], fontsize=9)

        # --- 涨跌幅间隔条 ---
        ax_gap.bar(x, np.abs(change_pcts), bottom=0, width=body_w,
                   color=[C['green'] if v >= 0 else C['red'] for v in change_pcts],
                   alpha=0.5)
        ax_gap.set_yticks([])
        ax_gap.set_ylabel('涨跌', color=C['txt2'], fontsize=7)

        # --- 成交量 ---
        vol_colors = [C['green'] if c >= o else C['red'] for c, o in zip(closes, opens)]
        ax_vol.bar(x, volumes, width=body_w, color=vol_colors, alpha=0.6)
        ax_vol.set_ylabel('成交量', color=C['txt2'], fontsize=9)
        ax_vol.yaxis.set_major_formatter(mticker.FuncFormatter(
            lambda v, p: f'{v / 1e6:.0f}M' if v >= 1e6 else f'{v / 1e3:.0f}K' if v >= 1e3 else f'{v:.0f}'))

        # X轴日期
        n = len(dates)
        tick_step = max(1, n // 10)
        tick_pos = list(range(0, n, tick_step))
        ax_vol.set_xticks(tick_pos)
        ax_vol.set_xticklabels([dates[i].strftime('%Y-%m-%d') for i in tick_pos],
                                rotation=30, fontsize=7)
        plt.setp(ax_price.get_xticklabels(), visible=False)
        plt.setp(ax_gap.get_xticklabels(), visible=False)

        self.fig.tight_layout()
        self.canvas.draw()

    def _draw_boll(self, dates, closes, volumes, code, fname):
        """布林带图表。"""
        gs = GridSpec(2, 1, height_ratios=[3, 1], hspace=0.08, figure=self.fig)
        ax = self.fig.add_subplot(gs[0])
        ax_vol = self.fig.add_subplot(gs[1], sharex=ax)
        self._style_ax(ax)
        self._style_ax(ax_vol)

        x = np.arange(len(dates))
        mid, upper, lower = bollinger_bands(closes, period=20, num_std=2.0)
        valid = ~np.isnan(mid)

        # 价格线
        ax.plot(x, closes, color=C['accent'], linewidth=1, alpha=0.8, label='收盘价')
        # 布林带
        if np.any(valid):
            ax.plot(x[valid], mid[valid], color=C['yellow'], linewidth=1, alpha=0.7, label='中轨(MA20)')
            ax.fill_between(x[valid], lower[valid], upper[valid],
                            alpha=0.1, color=C['purple'])
            ax.plot(x[valid], upper[valid], color=C['purple'], linewidth=0.8,
                    alpha=0.7, linestyle='--', label='上轨')
            ax.plot(x[valid], lower[valid], color=C['purple'], linewidth=0.8,
                    alpha=0.7, linestyle='--', label='下轨')

        ax.legend(loc='upper left', fontsize=8, framealpha=0.3,
                  facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])
        ax.set_title(f"{code} {fname} - 布林带 (20, 2)", color=C['txt'],
                     fontsize=11, fontweight='bold', pad=8)
        ax.set_ylabel('价格', color=C['txt2'], fontsize=9)

        # 成交量
        vc = [C['green'] if closes[i] >= (closes[i - 1] if i > 0 else closes[i]) else C['red']
              for i in range(len(closes))]
        ax_vol.bar(x, volumes, width=0.6, color=vc, alpha=0.5)
        ax_vol.set_ylabel('成交量', color=C['txt2'], fontsize=9)
        n = len(dates)
        tick_step = max(1, n // 10)
        ax_vol.set_xticks(list(range(0, n, tick_step)))
        ax_vol.set_xticklabels([dates[i].strftime('%Y-%m-%d') for i in range(0, n, tick_step)],
                                rotation=30, fontsize=7)
        plt.setp(ax.get_xticklabels(), visible=False)
        self.fig.tight_layout()
        self.canvas.draw()

    def _draw_macd(self, dates, closes, volumes, code, fname):
        """MACD 图表。"""
        gs = GridSpec(3, 1, height_ratios=[2, 1, 1], hspace=0.08, figure=self.fig)
        ax_price = self.fig.add_subplot(gs[0])
        ax_macd = self.fig.add_subplot(gs[1], sharex=ax_price)
        ax_vol = self.fig.add_subplot(gs[2], sharex=ax_price)
        for ax in [ax_price, ax_macd, ax_vol]:
            self._style_ax(ax)

        x = np.arange(len(dates))

        # 价格 + MA
        ax_price.plot(x, closes, color=C['accent'], linewidth=1, alpha=0.8, label='收盘价')
        m20 = ma(closes, 20)
        v = ~np.isnan(m20)
        if np.any(v):
            ax_price.plot(x[v], m20[v], color=C['yellow'], linewidth=0.8, alpha=0.7, label='MA20')
        ax_price.legend(loc='upper left', fontsize=8, framealpha=0.3,
                        facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])
        ax_price.set_ylabel('价格', color=C['txt2'], fontsize=9)

        # MACD
        dif, dea, histogram = macd(closes)
        valid_macd = ~np.isnan(dif)
        if np.any(valid_macd):
            xv = x[valid_macd]
            # 柱状图
            h_colors = [C['green'] if v >= 0 else C['red'] for v in histogram[valid_macd]]
            ax_macd.bar(xv, histogram[valid_macd], width=0.6, color=h_colors, alpha=0.7)
            ax_macd.plot(xv, dif[valid_macd], color=C['accent'], linewidth=1,
                         label='DIF')
            ax_macd.plot(xv, dea[valid_macd], color=C['yellow'], linewidth=1,
                         label='DEA')
            ax_macd.axhline(y=0, color=C['txt2'], linewidth=0.5, alpha=0.3)
            ax_macd.legend(loc='upper left', fontsize=8, framealpha=0.3,
                           facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])
        ax_macd.set_ylabel('MACD', color=C['txt2'], fontsize=9)
        ax_macd.set_title(f"{code} {fname} - MACD (12, 26, 9)", color=C['txt'],
                          fontsize=11, fontweight='bold', pad=8)

        # 成交量
        vc = [C['green'] if closes[i] >= (closes[i - 1] if i > 0 else closes[i]) else C['red']
              for i in range(len(closes))]
        ax_vol.bar(x, volumes, width=0.6, color=vc, alpha=0.5)
        ax_vol.set_ylabel('成交量', color=C['txt2'], fontsize=9)

        n = len(dates)
        tick_step = max(1, n // 10)
        ax_vol.set_xticks(list(range(0, n, tick_step)))
        ax_vol.set_xticklabels([dates[i].strftime('%Y-%m-%d') for i in range(0, n, tick_step)],
                                rotation=30, fontsize=7)
        plt.setp(ax_price.get_xticklabels(), visible=False)
        plt.setp(ax_macd.get_xticklabels(), visible=False)
        self.fig.tight_layout()
        self.canvas.draw()

    def _draw_rsi(self, dates, closes, volumes, code, fname):
        """RSI 图表。"""
        gs = GridSpec(3, 1, height_ratios=[2, 1.2, 1], hspace=0.08, figure=self.fig)
        ax_price = self.fig.add_subplot(gs[0])
        ax_rsi = self.fig.add_subplot(gs[1], sharex=ax_price)
        ax_vol = self.fig.add_subplot(gs[2], sharex=ax_price)
        for ax in [ax_price, ax_rsi, ax_vol]:
            self._style_ax(ax)

        x = np.arange(len(dates))

        # 价格
        ax_price.plot(x, closes, color=C['accent'], linewidth=1, alpha=0.8, label='收盘价')
        ax_price.set_ylabel('价格', color=C['txt2'], fontsize=9)
        ax_price.legend(loc='upper left', fontsize=8, framealpha=0.3,
                        facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])

        # RSI
        rsi14 = rsi(closes, 14)
        rsi6 = rsi(closes, 6)
        valid = ~np.isnan(rsi14)
        if np.any(valid):
            xv = x[valid]
            ax_rsi.plot(xv, rsi6[valid], color=C['peach'], linewidth=1, alpha=0.8, label='RSI6')
            ax_rsi.plot(xv, rsi14[valid], color=C['purple'], linewidth=1, alpha=0.8, label='RSI14')
            ax_rsi.axhline(y=70, color=C['red'], linewidth=0.8, linestyle='--', alpha=0.6)
            ax_rsi.axhline(y=30, color=C['green'], linewidth=0.8, linestyle='--', alpha=0.6)
            ax_rsi.axhline(y=50, color=C['txt2'], linewidth=0.5, linestyle=':', alpha=0.3)
            # 填充超买超卖区
            ax_rsi.fill_between(xv, 70, 100, alpha=0.05, color=C['red'])
            ax_rsi.fill_between(xv, 0, 30, alpha=0.05, color=C['green'])
            ax_rsi.set_ylim(0, 100)
            ax_rsi.legend(loc='upper left', fontsize=8, framealpha=0.3,
                          facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])
        ax_rsi.set_ylabel('RSI', color=C['txt2'], fontsize=9)
        ax_rsi.set_title(f"{code} {fname} - RSI (6, 14)", color=C['txt'],
                         fontsize=11, fontweight='bold', pad=8)

        # 成交量
        vc = [C['green'] if closes[i] >= (closes[i - 1] if i > 0 else closes[i]) else C['red']
              for i in range(len(closes))]
        ax_vol.bar(x, volumes, width=0.6, color=vc, alpha=0.5)
        ax_vol.set_ylabel('成交量', color=C['txt2'], fontsize=9)

        n = len(dates)
        tick_step = max(1, n // 10)
        ax_vol.set_xticks(list(range(0, n, tick_step)))
        ax_vol.set_xticklabels([dates[i].strftime('%Y-%m-%d') for i in range(0, n, tick_step)],
                                rotation=30, fontsize=7)
        plt.setp(ax_price.get_xticklabels(), visible=False)
        plt.setp(ax_rsi.get_xticklabels(), visible=False)
        self.fig.tight_layout()
        self.canvas.draw()

    def _draw_volume(self, dates, opens, highs, lows, closes, volumes, change_pcts,
                     code, fname):
        """成交量分析图表。"""
        gs = GridSpec(3, 1, height_ratios=[2, 1, 1], hspace=0.08, figure=self.fig)
        ax_price = self.fig.add_subplot(gs[0])
        ax_vol = self.fig.add_subplot(gs[1], sharex=ax_price)
        ax_turnover = self.fig.add_subplot(gs[2], sharex=ax_price)
        for ax in [ax_price, ax_vol, ax_turnover]:
            self._style_ax(ax)

        x = np.arange(len(dates))

        # 简化K线
        up = closes >= opens
        down = ~up
        ax_price.vlines(x[up], lows[up], highs[up], color=C['green'], linewidths=0.5)
        ax_price.vlines(x[down], lows[down], highs[down], color=C['red'], linewidths=0.5)
        if np.any(up):
            h = np.maximum(closes[up] - opens[up], 0.002)
            ax_price.bar(x[up], h, bottom=opens[up], width=0.6, color=C['green'])
        if np.any(down):
            h = np.maximum(opens[down] - closes[down], 0.002)
            ax_price.bar(x[down], h, bottom=closes[down], width=0.6, color=C['red'])
        ax_price.set_ylabel('价格', color=C['txt2'], fontsize=9)

        # 成交量 + MA5
        vc = [C['green'] if c >= o else C['red'] for c, o in zip(closes, opens)]
        ax_vol.bar(x, volumes, width=0.6, color=vc, alpha=0.6, label='成交量')
        v_ma5 = ma(volumes, 5)
        vv = ~np.isnan(v_ma5)
        if np.any(vv):
            ax_vol.plot(x[vv], v_ma5[vv], color=C['yellow'], linewidth=1, label='VOL MA5')
        ax_vol.legend(loc='upper left', fontsize=8, framealpha=0.3,
                      facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])
        ax_vol.set_ylabel('成交量', color=C['txt2'], fontsize=9)

        # 涨跌幅
        chg_colors = [C['green'] if v >= 0 else C['red'] for v in change_pcts]
        ax_turnover.bar(x, change_pcts, width=0.6, color=chg_colors, alpha=0.6)
        ax_turnover.axhline(y=0, color=C['txt2'], linewidth=0.5, alpha=0.3)
        ax_turnover.set_ylabel('涨跌幅%', color=C['txt2'], fontsize=9)

        ax_price.set_title(f"{code} {fname} - 成交量分析", color=C['txt'],
                           fontsize=11, fontweight='bold', pad=8)

        n = len(dates)
        tick_step = max(1, n // 10)
        ax_turnover.set_xticks(list(range(0, n, tick_step)))
        ax_turnover.set_xticklabels([dates[i].strftime('%Y-%m-%d') for i in range(0, n, tick_step)],
                                     rotation=30, fontsize=7)
        plt.setp(ax_price.get_xticklabels(), visible=False)
        plt.setp(ax_vol.get_xticklabels(), visible=False)
        self.fig.tight_layout()
        self.canvas.draw()

    def _on_reset_view(self):
        self.toolbar.home()
        self.canvas.draw()

    # ==================== 后台下载 ====================

    def _update_fund_list(self):
        def task():
            self._set_status("正在更新基金列表...")
            try:
                from fetcher import fetch_and_save_all_fund_basic
                fetch_and_save_all_fund_basic()
                self._set_status("基金列表已更新")
                self._update_stats()
                messagebox.showinfo("完成", "基金列表已更新")
            except Exception as e:
                self._set_status(f"更新失败: {e}")
                messagebox.showerror("错误", str(e))
        threading.Thread(target=task, daemon=True).start()

    def _download_nav(self):
        if not self.current_fund_code:
            messagebox.showinfo("提示", "请先选择基金")
            return
        code = self.current_fund_code
        def task():
            self._set_status(f"下载 {code} 净值...")
            try:
                from fetcher import fetch_single_fund_history
                fetch_single_fund_history(code)
                self.root.after(100, self._on_tree_select)
                self._update_stats()
            except Exception as e:
                self._set_status(f"失败: {e}")
        threading.Thread(target=task, daemon=True).start()

    def _download_ohlc(self):
        if not self.current_fund_code:
            messagebox.showinfo("提示", "请先选择基金")
            return
        code = self.current_fund_code
        def task():
            self._set_status(f"下载 {code} K线数据...")
            try:
                from fetcher import fetch_single_fund_ohlc
                fetch_single_fund_ohlc(code)
                self.root.after(100, self._on_tree_select)
                self._update_stats()
            except Exception as e:
                self._set_status(f"失败: {e}")
        threading.Thread(target=task, daemon=True).start()

    def _on_closing(self):
        close_connection()
        self.root.quit()
        self.root.destroy()


def run():
    init_db()
    root = tk.Tk()
    try:
        from ctypes import windll
        windll.shcore.SetProcessDpiAwareness(1)
    except Exception:
        pass
    app = FundViewerApp(root)
    root.mainloop()


if __name__ == "__main__":
    run()
