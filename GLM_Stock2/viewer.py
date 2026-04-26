"""
股票数据交互式查看器
基于 tkinter + matplotlib，支持：
- 股票搜索（按代码/名称/行业）
- K线图 + 技术指标（布林带/MACD/RSI/成交量）
- 涨跌幅 + 滚动收益率（3天/1周/1月）
- 鼠标框选放大/缩小/平移（matplotlib toolbar）
- 在线下载功能
- 按时间段查询 + 快捷日期按钮
"""

import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime, timedelta
import threading
import warnings
import numpy as np

import matplotlib
matplotlib.use('TkAgg')
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
from matplotlib.figure import Figure
from matplotlib.gridspec import GridSpec
import matplotlib.dates as mdates
import matplotlib.ticker as mticker
import matplotlib.pyplot as plt
import platform

# 中文字体
def _setup_chinese_font():
    system = platform.system()
    candidates = {
        'Windows': ['SimHei', 'Microsoft YaHei', 'SimSun'],
        'Linux': ['Noto Sans CJK SC', 'Noto Sans SC', 'WenQuanYi Micro Hei',
                   'WenQuanYi Zen Hei', 'Droid Sans Fallback'],
        'Darwin': ['PingFang SC', 'Heiti SC', 'STHeiti'],
    }
    available = {f.name for f in matplotlib.font_manager.fontManager.ttflist}
    preferred = candidates.get(system, []) + candidates.get('Linux', [])
    for name in preferred:
        if name in available:
            plt.rcParams['font.sans-serif'] = [name, 'DejaVu Sans']
            break
    else:
        plt.rcParams['font.sans-serif'] = ['DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False

_setup_chinese_font()

from database import (
    init_db, search_stocks, get_stock_detail, get_stock_daily,
    get_stock_date_range, get_stock_daily_count, get_stock_industries,
    get_stocks_by_industry, get_db_stats, close_connection,
)
from indicators import ma, ema, bollinger_bands, macd_calc as macd, rsi

# 配色
C = {
    'bg': '#1e1e2e', 'panel': '#2a2a3d', 'input': '#363650', 'card': '#313148',
    'txt': '#cdd6f4', 'txt2': '#a6adc8', 'accent': '#89b4fa',
    'green': '#a6e3a1', 'red': '#f38ba8', 'yellow': '#f9e2af',
    'purple': '#cba6f7', 'teal': '#94e2d5', 'peach': '#fab387',
    'border': '#45475a',
    'ma5': '#f38ba8', 'ma10': '#f9e2af', 'ma20': '#89b4fa', 'ma60': '#cba6f7',
    'grid': '#45475a',
}


class App:
    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("股票数据查看器 - K线 / 技术指标 / 收益分析")
        self.root.geometry("1300x900")
        self.root.minsize(1050, 720)
        self.root.configure(bg=C['bg'])

        self.current_code = None
        self.has_data = False

        self._setup_styles()
        init_db()
        self._build_ui()

        self.root.bind('<Return>', lambda e: self._on_search())
        self.root.bind('<Escape>', lambda e: self._on_reset_view())
        self.root.protocol("WM_DELETE_WINDOW", self._on_closing)

    # ==================== 样式 ====================
    def _setup_styles(self):
        s = ttk.Style()
        s.theme_use('clam')
        s.configure('TFrame', background=C['bg'])
        s.configure('Panel.TFrame', background=C['panel'])
        s.configure('Card.TFrame', background=C['card'])
        s.configure('TLabel', background=C['bg'], foreground=C['txt'], font=('Microsoft YaHei', 9))
        s.configure('Info.TLabel', background=C['card'], foreground=C['txt'], font=('Microsoft YaHei', 9))
        s.configure('InfoV.TLabel', background=C['card'], foreground=C['yellow'], font=('Microsoft YaHei', 9, 'bold'))
        s.configure('Stats.TLabel', background=C['bg'], foreground=C['txt2'], font=('Consolas', 8))
        s.configure('Status.TLabel', background=C['panel'], foreground=C['txt2'], font=('Microsoft YaHei', 9))
        s.configure('TEntry', fieldbackground=C['input'], foreground=C['txt'], insertcolor=C['txt'])
        s.configure('TButton', background=C['accent'], foreground='#1e1e2e',
                     font=('Microsoft YaHei', 9, 'bold'), borderwidth=0, padding=(10, 5))
        s.map('TButton', background=[('active', '#b4d0fb')])
        s.configure('Green.TButton', background=C['green'])
        s.map('Green.TButton', background=[('active', '#c4f0bf')])
        s.configure('Yellow.TButton', background=C['yellow'], foreground='#1e1e2e')
        s.map('Yellow.TButton', background=[('active', '#fbecc0')])
        s.configure('Red.TButton', background=C['red'])
        s.map('Red.TButton', background=[('active', '#f7bac5')])
        s.configure('Treeview', background=C['card'], foreground=C['txt'],
                     fieldbackground=C['card'], borderwidth=0, font=('Microsoft YaHei', 9), rowheight=28)
        s.configure('Treeview.Heading', background=C['panel'], foreground=C['accent'],
                     font=('Microsoft YaHei', 9, 'bold'), borderwidth=0)
        s.map('Treeview', background=[('selected', C['accent'])], foreground=[('selected', '#1e1e2e')])
        s.configure('TCombobox', fieldbackground=C['input'], background=C['input'],
                     foreground=C['txt'], selectbackground=C['accent'])
        s.configure('TRadiobutton', background=C['bg'], foreground=C['txt'], font=('Microsoft YaHei', 9))
        s.map('TRadiobutton', background=[('active', C['bg'])])

    # ==================== 界面构建 ====================
    def _build_ui(self):
        main = ttk.Frame(self.root)
        main.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)

        # -- 标题栏 --
        title_f = ttk.Frame(main)
        title_f.pack(fill=tk.X, pady=(0, 4))
        ttk.Label(title_f, text="  股票数据查看器",
                  font=('Microsoft YaHei', 14, 'bold'), foreground=C['accent']).pack(side=tk.LEFT)
        ttk.Label(title_f, text="K线 / 技术指标 / 收益分析",
                  font=('Microsoft YaHei', 10), foreground=C['txt2']).pack(side=tk.LEFT, padx=12)

        # -- 搜索栏 --
        sf = ttk.Frame(main)
        sf.pack(fill=tk.X, pady=(0, 6))
        self.search_var = tk.StringVar(value="输入股票代码或名称...")
        se = ttk.Entry(sf, textvariable=self.search_var, width=30, font=('Microsoft YaHei', 10))
        se.pack(side=tk.LEFT, padx=(0, 6), ipady=3)
        se.bind('<FocusIn>', lambda e: self.search_var.set('') if self.search_var.get() == '输入股票代码或名称...' else None)
        se.bind('<FocusOut>', lambda e: self.search_var.set('输入股票代码或名称...') if not self.search_var.get() else None)
        ttk.Button(sf, text="搜索", command=self._on_search).pack(side=tk.LEFT, padx=2)
        ttk.Button(sf, text="重置视图", command=self._on_reset_view, style='Yellow.TButton').pack(side=tk.LEFT, padx=2)

        ttk.Label(sf, text="  行业:").pack(side=tk.LEFT, padx=(12, 2))
        self.type_var = tk.StringVar(value="全部")
        self.type_combo = ttk.Combobox(sf, textvariable=self.type_var, width=14, state='readonly')
        self.type_combo.pack(side=tk.LEFT, padx=2)
        self.type_combo.bind('<<ComboboxSelected>>', lambda e: self._on_type_filter())

        bf = ttk.Frame(sf)
        bf.pack(side=tk.RIGHT, padx=4)
        ttk.Button(bf, text="更新列表", command=self._on_update_list, style='Green.TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(bf, text="下载数据", command=self._on_download_single, style='TButton').pack(side=tk.LEFT, padx=2)
        ttk.Button(bf, text="下载全部", command=self._on_download_all, style='Red.TButton').pack(side=tk.LEFT, padx=2)

        # -- 主体 --
        paned = ttk.PanedWindow(main, orient=tk.VERTICAL)
        paned.pack(fill=tk.BOTH, expand=True)

        top = ttk.Frame(paned)
        paned.add(top, weight=1)

        # 列表
        tf = ttk.Frame(top)
        tf.pack(fill=tk.BOTH, expand=True, pady=(0, 3))
        self.tree = ttk.Treeview(tf, show='headings', height=7)
        ts = ttk.Scrollbar(tf, orient=tk.VERTICAL, command=self.tree.yview)
        self.tree.configure(yscrollcommand=ts.set)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        ts.pack(side=tk.RIGHT, fill=tk.Y)
        self.tree.bind('<Double-1>', lambda e: self._on_tree_select())
        self.tree.bind('<Return>', lambda e: self._on_tree_select())

        # Treeview 列
        cols = ('code', 'name', 'industry', 'price', 'chg', 'pe', 'pb', 'daily')
        self.tree['columns'] = cols
        for cid, text, w in [('code', '代码', 75), ('name', '名称', 160), ('industry', '行业', 90),
                              ('price', '最新价', 70), ('chg', '涨跌幅', 70),
                              ('pe', 'PE', 60), ('pb', 'PB', 60), ('daily', '日线', 70)]:
            self.tree.heading(cid, text=text)
            self.tree.column(cid, width=w, minwidth=55, anchor=tk.CENTER)

        # 详情
        self.detail_frame = ttk.Frame(top, style='Card.TFrame')
        self.detail_frame.pack(fill=tk.X, pady=(3, 0))
        self.detail_inner = ttk.Frame(self.detail_frame, style='Card.TFrame')
        self.detail_inner.pack(fill=tk.X, padx=10, pady=6)
        self.info_labels = {}

        # 详情字段
        fields = [
            ('code', '股票代码'), ('name', '股票名称'), ('industry', '所属行业'),
            ('list_date', '上市日期'), ('total_cap', '总市值'), ('float_cap', '流通市值'),
            ('price', '最新价'), ('chg', '涨跌幅'), ('pe', '市盈率'), ('pb', '市净率'),
            ('daily_range', '数据范围'), ('daily_cnt', '日线记录')
        ]
        for i, (k, t) in enumerate(fields):
            r, co = i // 4, (i % 4) * 2
            ttk.Label(self.detail_inner, text=f"{t}:", style='Info.TLabel').grid(
                row=r, column=co, sticky=tk.W, padx=(6, 3), pady=2)
            lb = ttk.Label(self.detail_inner, text="--", style='InfoV.TLabel', width=28)
            lb.grid(row=r, column=co + 1, sticky=tk.W, padx=(0, 12), pady=2)
            self.info_labels[k] = lb

        # -- 图表区 --
        chart_f = ttk.Frame(paned)
        paned.add(chart_f, weight=3)

        # 图表模式选择
        ctrl = ttk.Frame(chart_f)
        ctrl.pack(fill=tk.X, padx=4, pady=2)

        # K线/指标区
        ttk.Label(ctrl, text="图表:").pack(side=tk.LEFT, padx=(4, 2))
        self.chart_mode_var = tk.StringVar(value='kline')
        for text, val in [('K线+均线', 'kline'), ('布林带', 'boll'), ('MACD', 'macd'),
                          ('RSI', 'rsi'), ('成交量', 'vol')]:
            ttk.Radiobutton(ctrl, text=text, variable=self.chart_mode_var, value=val,
                            command=self._redraw_chart).pack(side=tk.LEFT, padx=2)

        ttk.Separator(ctrl, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)

        # 收益率区
        ttk.Label(ctrl, text="收益:").pack(side=tk.LEFT, padx=(4, 2))
        self.ret_mode_var = tk.StringVar(value='')
        for text, val in [('涨跌幅', 'daily_chg'), ('近3天', 'ret_3d'), ('近1周', 'ret_1w'), ('近1月', 'ret_1m')]:
            ttk.Radiobutton(ctrl, text=text, variable=self.ret_mode_var, value=val,
                            command=self._redraw_chart).pack(side=tk.LEFT, padx=2)

        ttk.Separator(ctrl, orient=tk.VERTICAL).pack(side=tk.LEFT, fill=tk.Y, padx=8)

        # 日期范围
        ttk.Label(ctrl, text="日期:").pack(side=tk.LEFT, padx=(4, 2))
        self.date_start = tk.StringVar()
        self.date_end = tk.StringVar()
        ttk.Entry(ctrl, textvariable=self.date_start, width=11).pack(side=tk.LEFT, padx=2)
        ttk.Label(ctrl, text="~").pack(side=tk.LEFT)
        ttk.Entry(ctrl, textvariable=self.date_end, width=11).pack(side=tk.LEFT, padx=2)
        ttk.Button(ctrl, text="应用", command=self._redraw_chart).pack(side=tk.LEFT, padx=4)
        for label, days in [('近3月', 90), ('近1年', 365), ('近3年', 1095), ('全部', 0)]:
            ttk.Button(ctrl, text=label, command=lambda d=days: self._quick_date(d)).pack(side=tk.LEFT, padx=1)

        # matplotlib
        self.fig = Figure(figsize=(12, 5), dpi=100, facecolor=C['bg'])
        self.canvas = FigureCanvasTkAgg(self.fig, master=chart_f)
        tbf = ttk.Frame(chart_f)
        tbf.pack(side=tk.BOTTOM, fill=tk.X)
        self.toolbar = NavigationToolbar2Tk(self.canvas, tbf)
        self.toolbar.update()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

        # -- 状态栏 --
        sf2 = ttk.Frame(main, style='Panel.TFrame')
        sf2.pack(fill=tk.X, pady=(6, 0))
        self.status_var = tk.StringVar(value="就绪 - 输入股票代码或名称搜索")
        self.stats_var = tk.StringVar()
        self._update_stats()
        ttk.Label(sf2, textvariable=self.status_var, style='Status.TLabel').pack(side=tk.LEFT, padx=10, pady=4)
        ttk.Label(sf2, textvariable=self.stats_var, style='Stats.TLabel').pack(side=tk.RIGHT, padx=10, pady=4)

        # 初始化分类下拉
        self.type_combo['values'] = ["全部"] + get_stock_industries()

        self._show_placeholder()

    # ==================== 辅助方法 ====================
    def _set_status(self, t):
        self.status_var.set(t)
        self.root.update_idletasks()

    def _update_stats(self):
        try:
            s = get_db_stats()
            parts = [f"股票: {s['stock_count']:,}"]
            if s.get('stocks_with_daily'):
                parts.append(f"日线: {s['stocks_with_daily']:,}只/{s['daily_records']:,}条")
            if s.get('stocks_with_detail'):
                parts.append(f"详情: {s['stocks_with_detail']:,}")
            parts.append(f"DB: {s['db_size_mb']}MB")
            self.stats_var.set(" | ".join(parts))
        except Exception:
            pass

    def _clear_tree(self):
        for item in self.tree.get_children():
            self.tree.delete(item)

    def _clear_detail(self):
        for lb in self.info_labels.values():
            lb.config(text="--")
        self.date_start.set('')
        self.date_end.set('')

    def _style_ax(self, ax):
        """设置坐标轴样式（深色主题）。"""
        ax.set_facecolor(C['bg'])
        ax.tick_params(colors=C['txt2'], labelsize=8)
        ax.spines['top'].set_color(C['border'])
        ax.spines['right'].set_color(C['border'])
        ax.spines['bottom'].set_color(C['border'])
        ax.spines['left'].set_color(C['border'])
        ax.grid(True, alpha=0.15, color=C['grid'], linestyle='-')

    def _auto_layout(self, top_margin=0.92, bottom_margin=0.08, hspace=0.35):
        """安全布局：优先用 tight_layout，失败时用 subplots_adjust 兜底。"""
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", message=".*tight_layout.*")
            try:
                self.fig.tight_layout(pad=1.5)
            except Exception:
                self.fig.subplots_adjust(left=0.08, right=0.95, top=top_margin,
                                         bottom=bottom_margin, hspace=hspace)

    def _set_xticks(self, ax_bottom, dates, n_ticks=10):
        """为底部子图设置 x 轴日期刻度。"""
        n = len(dates)
        step = max(1, n // n_ticks)
        tick_pos = list(range(0, n, step))
        ax_bottom.set_xticks(tick_pos)
        ax_bottom.set_xticklabels([dates[i].strftime('%Y-%m-%d') for i in tick_pos],
                                   rotation=30, fontsize=7)

    def _show_placeholder(self):
        """显示空图表占位。"""
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        self._style_ax(ax)
        ax.text(0.5, 0.5, '请搜索并选择股票查看图表',
                transform=ax.transAxes, ha='center', va='center',
                fontsize=16, color=C['txt2'], alpha=0.6)
        self._auto_layout()
        self.canvas.draw()

    # ==================== 事件处理 ====================
    def _on_search(self):
        kw = self.search_var.get().strip()
        if kw in ('', '输入股票代码或名称...'):
            return
        self._set_status(f"搜索: {kw}...")
        self._clear_tree()

        results = search_stocks(kw)
        for f in results:
            dc = get_stock_daily_count(f['code'])
            self.tree.insert('', tk.END, values=(
                f['code'], f['name'], f.get('industry', ''),
                f"{f.get('latest_price', 0):.2f}" if f.get('latest_price') else '-',
                f"{f.get('change_pct', 0):.2f}%" if f.get('change_pct') else '-',
                f"{f.get('pe_ttm', 0):.1f}" if f.get('pe_ttm') else '-',
                f"{f.get('pb', 0):.2f}" if f.get('pb') else '-',
                f"{dc:,}" if dc > 0 else '-'))

        self._set_status(f"找到 {len(results)} 条")
        if len(results) == 1:
            self.tree.selection_set(self.tree.get_children()[0])
            self._on_tree_select()

    def _on_type_filter(self):
        val = self.type_var.get()
        if val == "全部":
            return
        self._clear_tree()
        results = get_stocks_by_industry(val)
        for f in results:
            dc = get_stock_daily_count(f['code'])
            self.tree.insert('', tk.END, values=(
                f['code'], f['name'], f.get('industry', ''),
                f"{f.get('latest_price', 0):.2f}" if f.get('latest_price') else '-',
                f"{f.get('change_pct', 0):.2f}%" if f.get('change_pct') else '-',
                f"{f.get('pe_ttm', 0):.1f}" if f.get('pe_ttm') else '-',
                f"{f.get('pb', 0):.2f}" if f.get('pb') else '-',
                f"{dc:,}" if dc > 0 else '-'))
        self._set_status(f"{val}: {len(results)} 条")

    def _on_tree_select(self):
        sel = self.tree.selection()
        if not sel:
            return
        vals = self.tree.item(sel[0], 'values')
        code = str(vals[0])
        self.current_code = code
        self._set_status(f"加载 {code}...")

        # 填充详情
        detail = get_stock_detail(code)
        if detail:
            self.info_labels['code'].config(text=code)
            self.info_labels['name'].config(text=detail.get('name', '--'))
            self.info_labels['industry'].config(text=detail.get('industry', '--'))
            ld = detail.get('list_date', '--')
            if ld and len(ld) == 8:
                ld = f"{ld[:4]}-{ld[4:6]}-{ld[6:8]}"
            self.info_labels['list_date'].config(text=ld)
            tc = detail.get('total_market_cap', 0) or 0
            self.info_labels['total_cap'].config(text=f"{tc/1e8:.0f} 亿" if tc >= 1e8 else f"{tc/1e4:.0f} 万")
            fc = detail.get('float_market_cap', 0) or 0
            self.info_labels['float_cap'].config(text=f"{fc/1e8:.0f} 亿" if fc >= 1e8 else f"{fc/1e4:.0f} 万")
            lp = detail.get('latest_price', 0) or 0
            self.info_labels['price'].config(text=f"{lp:.2f}" if lp else '--')
            cp = detail.get('change_pct', 0) or 0
            self.info_labels['chg'].config(text=f"{cp:.2f}%" if cp else '--')
            pe = detail.get('pe_ttm', 0) or 0
            self.info_labels['pe'].config(text=f"{pe:.2f}" if pe else '--')
            pb = detail.get('pb', 0) or 0
            self.info_labels['pb'].config(text=f"{pb:.2f}" if pb else '--')

        dc = get_stock_daily_count(code)
        dr = get_stock_date_range(code)
        self.info_labels['daily_cnt'].config(text=f"{dc:,}")
        self.info_labels['daily_range'].config(text=f"{dr[0]} ~ {dr[1]}" if dr[0] else "无数据")
        self.has_data = dc > 0
        if dr[0]:
            self.date_start.set(dr[0])
            self.date_end.set(dr[1])
        self._set_status(f"{code} | 日线 {dc:,} 条")
        self._redraw_chart()

    def _quick_date(self, days):
        if not self.current_code:
            return
        if days == 0:
            dr = get_stock_date_range(self.current_code)
            self.date_start.set(dr[0] if dr[0] else '')
            self.date_end.set(dr[1] if dr[1] else '')
        else:
            self.date_end.set(datetime.now().strftime('%Y-%m-%d'))
            self.date_start.set((datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'))
        self._redraw_chart()

    def _on_reset_view(self):
        """重置图表视图（恢复默认缩放）。"""
        self.ret_mode_var.set('')
        self.chart_mode_var.set('kline')
        self._redraw_chart()

    def _on_update_list(self):
        """后台更新股票列表。"""
        def worker():
            self._set_status("正在更新股票列表...")
            try:
                from fetcher import fetch_and_save_stock_list
                ok = fetch_and_save_stock_list()
                if ok:
                    self.type_combo['values'] = ["全部"] + get_stock_industries()
                    self._update_stats()
                    self._set_status("股票列表更新完成")
                else:
                    self._set_status("股票列表更新失败")
            except Exception as e:
                self._set_status(f"更新失败: {e}")

        threading.Thread(target=worker, daemon=True).start()

    def _on_download_single(self):
        """下载当前选中股票的数据。"""
        if not self.current_code:
            messagebox.showinfo("提示", "请先选择一只股票")
            return
        code = self.current_code

        def worker():
            self._set_status(f"正在下载 {code} 数据...")
            try:
                from fetcher import fetch_single_stock_all
                fetch_single_stock_all(code)
                self._update_stats()
                # 刷新详情和图表
                self.root.after(0, self._on_tree_select)
            except Exception as e:
                self._set_status(f"下载失败: {e}")

        threading.Thread(target=worker, daemon=True).start()

    def _on_download_all(self):
        """后台下载所有股票数据。"""
        def worker():
            self._set_status("正在下载全部股票数据（后台运行）...")
            try:
                from fetcher import fetch_all_stock_daily
                fetch_all_stock_daily(max_workers=4)
                self.root.after(0, self._update_stats)
                self.root.after(0, lambda: self._set_status("全部下载完成"))
            except Exception as e:
                self.root.after(0, lambda: self._set_status(f"下载失败: {e}"))

        threading.Thread(target=worker, daemon=True).start()

    def _on_closing(self):
        close_connection()
        self.root.destroy()

    # ==================== 图表绘制 ====================
    def _redraw_chart(self):
        if not self.current_code:
            return
        code = self.current_code

        # 确定模式：收益模式优先
        ret_mode = self.ret_mode_var.get()
        if ret_mode:
            # 收益率图表
            self._draw_return_chart(code, ret_mode)
            return

        # K线/指标模式
        if self.has_data:
            data = get_stock_daily(code, self.date_start.get().strip(), self.date_end.get().strip())
            if data:
                self._draw_ohlc_chart(code, data)
                return

        self._show_placeholder()

    def _draw_return_chart(self, code, mode):
        """绘制收益率图表。"""
        self.fig.clear()
        ax = self.fig.add_subplot(111)
        self._style_ax(ax)

        data = get_stock_daily(code, self.date_start.get().strip(), self.date_end.get().strip())
        if not data:
            ax.text(0.5, 0.5, '无数据', transform=ax.transAxes, ha='center', va='center',
                    fontsize=14, color=C['txt2'])
            self._auto_layout()
            self.canvas.draw()
            return

        dates = [datetime.strptime(d['date'], '%Y-%m-%d') for d in data]
        closes = [d['close_price'] for d in data if d.get('close_price')]
        if not closes:
            ax.text(0.5, 0.5, '无收盘价数据', transform=ax.transAxes, ha='center', va='center',
                    fontsize=14, color=C['txt2'])
            self._auto_layout()
            self.canvas.draw()
            return

        detail = get_stock_detail(code)
        fname = detail.get('name', '') if detail else ''

        if mode == 'daily_chg':
            # 日涨跌幅
            chgs = [d['change_pct'] for d in data if d.get('change_pct') is not None]
            if chgs:
                dates = dates[:len(chgs)]
            colors = [C['green'] if v >= 0 else C['red'] for v in chgs]
            ax.bar(dates, chgs, width=1.5, color=colors, alpha=0.7)
            ax.axhline(y=0, color=C['txt2'], linewidth=0.5, alpha=0.5)
            ax.set_ylabel('涨跌幅 (%)', color=C['txt2'], fontsize=9)
            title = '日涨跌幅'
            if chgs:
                arr = np.array(chgs)
                st = (f'平均: {np.mean(arr):.2f}% | 最大: {np.max(arr):.2f}% | '
                      f'最小: {np.min(arr):.2f}% | 正收益: {np.sum(arr > 0) / len(arr) * 100:.1f}%')
                ax.text(0.01, 0.97, st, transform=ax.transAxes, fontsize=8, color=C['txt2'], va='top',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor=C['card'], edgecolor=C['border'], alpha=0.8))
        else:
            # 滚动收益率
            wm = {'ret_3d': 3, 'ret_1w': 5, 'ret_1m': 22}
            tm = {'ret_3d': '近3天', 'ret_1w': '近1周', 'ret_1m': '近1月'}
            w = wm[mode]
            title = tm[mode] + '滚动收益率'
            rets = [None] * w + [
                (closes[i] / closes[i - w] - 1) * 100 if closes[i - w] else None
                for i in range(w, len(closes))
            ]
            vals = [r for r in rets if r is not None]
            pd_ = dates[len(dates) - len(vals):] if vals else []
            colors = [C['green'] if v >= 0 else C['red'] for v in vals]
            ax.bar(pd_, vals, width=2, color=colors, alpha=0.7)
            ax.axhline(y=0, color=C['txt2'], linewidth=0.5, alpha=0.5)
            ax.set_ylabel(f'{w}日收益率 (%)', color=C['txt2'], fontsize=9)
            if vals:
                arr = np.array(vals)
                st = (f'平均: {np.mean(arr):.2f}% | 最大: {np.max(arr):.2f}% | '
                      f'最小: {np.min(arr):.2f}% | 正收益: {np.sum(arr > 0) / len(arr) * 100:.1f}%')
                ax.text(0.01, 0.97, st, transform=ax.transAxes, fontsize=8, color=C['txt2'], va='top',
                        bbox=dict(boxstyle='round,pad=0.3', facecolor=C['card'], edgecolor=C['border'], alpha=0.8))

        ax.set_title(f"{code} {fname} - {title}", color=C['txt'], fontsize=11, fontweight='bold', pad=8)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax.xaxis.set_major_locator(mdates.AutoDateLocator())
        self.fig.autofmt_xdate(rotation=25)
        self._auto_layout()
        self.canvas.draw()

    def _draw_ohlc_chart(self, code, data):
        """从 OHLC 数据绘制图表。"""
        self.fig.clear()
        if not data:
            self._show_placeholder()
            return

        dates = [datetime.strptime(d['date'], '%Y-%m-%d') for d in data]
        opens = np.array([d['open_price'] for d in data], dtype=float)
        closes = np.array([d['close_price'] for d in data], dtype=float)
        highs = np.array([d['high_price'] for d in data], dtype=float)
        lows = np.array([d['low_price'] for d in data], dtype=float)
        volumes = np.array([d['volume'] if d.get('volume') else 0 for d in data], dtype=float)
        change_pcts = np.array([d['change_pct'] if d.get('change_pct') else 0 for d in data], dtype=float)

        detail = get_stock_detail(code)
        fname = detail.get('name', '') if detail else ''
        mode = self.chart_mode_var.get()

        if mode == 'kline':
            self._draw_kline(code, fname, dates, opens, highs, lows, closes, volumes, change_pcts)
        elif mode == 'boll':
            self._draw_boll(code, fname, dates, closes, volumes)
        elif mode == 'macd':
            self._draw_macd(code, fname, dates, closes, volumes)
        elif mode == 'rsi':
            self._draw_rsi(code, fname, dates, closes, volumes)
        elif mode == 'vol':
            self._draw_vol(code, fname, dates, opens, highs, lows, closes, volumes, change_pcts)

    # --- K线 + 均线 ---
    def _draw_kline(self, code, fname, dates, opens, highs, lows, closes, volumes, chg):
        gs = GridSpec(3, 1, height_ratios=[3, 0.3, 1], hspace=0.05, figure=self.fig)
        ax_p = self.fig.add_subplot(gs[0])
        ax_g = self.fig.add_subplot(gs[1])
        ax_v = self.fig.add_subplot(gs[2], sharex=ax_p)
        for ax in (ax_p, ax_g, ax_v):
            self._style_ax(ax)

        x = np.arange(len(dates))
        up = closes >= opens
        down = ~up  # 修复: 用 ~up 代替 ~closes

        # K线
        ax_p.vlines(x[up], lows[up], highs[up], color=C['green'], linewidths=0.6)
        ax_p.vlines(x[down], lows[down], highs[down], color=C['red'], linewidths=0.6)
        if np.any(up):
            ax_p.bar(x[up], np.maximum(closes[up] - opens[up], 0.002),
                     bottom=opens[up], width=0.6, color=C['green'])
        if np.any(down):
            ax_p.bar(x[down], np.maximum(opens[down] - closes[down], 0.002),
                     bottom=closes[down], width=0.6, color=C['red'])

        # 均线
        for p, c, l in [(5, C['ma5'], 'MA5'), (10, C['ma10'], 'MA10'),
                         (20, C['ma20'], 'MA20'), (60, C['ma60'], 'MA60')]:
            m = ma(closes, p)
            v = ~np.isnan(m)
            if np.any(v):
                ax_p.plot(x[v], m[v], color=c, linewidth=0.9, alpha=0.85, label=l)

        ax_p.legend(loc='upper left', fontsize=8, framealpha=0.3,
                    facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])
        ax_p.set_title(f"{code} {fname} - K线+均线", color=C['txt'],
                       fontsize=11, fontweight='bold', pad=8)
        ax_p.set_ylabel('价格', color=C['txt2'], fontsize=9)

        # 涨跌颜色条
        ax_g.bar(x, np.abs(chg), width=0.6,
                 color=[C['green'] if v >= 0 else C['red'] for v in chg], alpha=0.5)
        ax_g.set_yticks([])

        # 成交量
        vc = [C['green'] if c >= o else C['red'] for c, o in zip(closes, opens)]
        ax_v.bar(x, volumes, width=0.6, color=vc, alpha=0.5)
        ax_v.set_ylabel('成交量', color=C['txt2'], fontsize=9)
        ax_v.yaxis.set_major_formatter(
            mticker.FuncFormatter(lambda v, p: f'{v / 1e6:.0f}M' if v >= 1e6 else f'{v / 1e3:.0f}K'))

        self._set_xticks(ax_v, dates)
        plt.setp(ax_p.get_xticklabels(), visible=False)
        plt.setp(ax_g.get_xticklabels(), visible=False)
        self._auto_layout(hspace=0.25)
        self.canvas.draw()

    # --- 布林带 ---
    def _draw_boll(self, code, fname, dates, closes, volumes):
        gs = GridSpec(2, 1, height_ratios=[3, 1], hspace=0.08, figure=self.fig)
        ax = self.fig.add_subplot(gs[0])
        ax_v = self.fig.add_subplot(gs[1], sharex=ax)
        self._style_ax(ax)
        self._style_ax(ax_v)

        x = np.arange(len(dates))
        mid, up, lo = bollinger_bands(closes, 20)
        v = ~np.isnan(mid)

        ax.plot(x, closes, color=C['accent'], linewidth=1, alpha=0.8, label='收盘')
        if np.any(v):
            ax.plot(x[v], mid[v], color=C['yellow'], linewidth=1, label='MA20')
            ax.fill_between(x[v], lo[v], up[v], alpha=0.1, color=C['purple'])
            ax.plot(x[v], up[v], color=C['purple'], linewidth=0.8, ls='--', label='上轨')
            ax.plot(x[v], lo[v], color=C['purple'], linewidth=0.8, ls='--', label='下轨')

        ax.legend(fontsize=8, framealpha=0.3, facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])
        ax.set_title(f"{code} {fname} - 布林带(20,2)", color=C['txt'],
                     fontsize=11, fontweight='bold', pad=8)
        ax.set_ylabel('价格', color=C['txt2'], fontsize=9)

        vc = [C['green'] if closes[i] >= (closes[i - 1] if i > 0 else closes[i]) else C['red']
              for i in range(len(closes))]
        ax_v.bar(x, volumes, width=0.6, color=vc, alpha=0.5)
        ax_v.set_ylabel('成交量', color=C['txt2'], fontsize=9)

        self._set_xticks(ax_v, dates)
        plt.setp(ax.get_xticklabels(), visible=False)
        self._auto_layout(hspace=0.2)
        self.canvas.draw()

    # --- MACD ---
    def _draw_macd(self, code, fname, dates, closes, volumes):
        gs = GridSpec(3, 1, height_ratios=[2, 1, 1], hspace=0.08, figure=self.fig)
        ax_p = self.fig.add_subplot(gs[0])
        ax_m = self.fig.add_subplot(gs[1], sharex=ax_p)
        ax_v = self.fig.add_subplot(gs[2], sharex=ax_p)
        for a in (ax_p, ax_m, ax_v):
            self._style_ax(a)

        x = np.arange(len(dates))

        # 价格 + MA20
        ax_p.plot(x, closes, color=C['accent'], linewidth=1)
        ax_p.set_ylabel('价格', color=C['txt2'], fontsize=9)
        m20 = ma(closes, 20)
        vm = ~np.isnan(m20)
        if np.any(vm):
            ax_p.plot(x[vm], m20[vm], color=C['yellow'], linewidth=0.8, label='MA20')
        ax_p.legend(fontsize=8, framealpha=0.3, facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])

        # MACD
        dif, dea, hist = macd(closes)
        vm = ~np.isnan(dif)
        if np.any(vm):
            ax_m.bar(x[vm], hist[vm], width=0.6,
                     color=[C['green'] if v >= 0 else C['red'] for v in hist[vm]], alpha=0.7)
            ax_m.plot(x[vm], dif[vm], color=C['accent'], linewidth=1, label='DIF')
            ax_m.plot(x[vm], dea[vm], color=C['yellow'], linewidth=1, label='DEA')
            ax_m.axhline(y=0, color=C['txt2'], linewidth=0.5, alpha=0.3)
            ax_m.legend(fontsize=8, framealpha=0.3, facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])
        ax_m.set_ylabel('MACD', color=C['txt2'], fontsize=9)

        ax_p.set_title(f"{code} {fname} - MACD(12,26,9)", color=C['txt'],
                       fontsize=11, fontweight='bold', pad=8)

        # 成交量
        vc = [C['green'] if closes[i] >= (closes[i - 1] if i > 0 else closes[i]) else C['red']
              for i in range(len(closes))]
        ax_v.bar(x, volumes, width=0.6, color=vc, alpha=0.5)
        ax_v.set_ylabel('成交量', color=C['txt2'], fontsize=9)

        self._set_xticks(ax_v, dates)
        plt.setp(ax_p.get_xticklabels(), visible=False)
        plt.setp(ax_m.get_xticklabels(), visible=False)
        self._auto_layout(hspace=0.3)
        self.canvas.draw()

    # --- RSI ---
    def _draw_rsi(self, code, fname, dates, closes, volumes):
        gs = GridSpec(3, 1, height_ratios=[2, 1.2, 1], hspace=0.08, figure=self.fig)
        ax_p = self.fig.add_subplot(gs[0])
        ax_r = self.fig.add_subplot(gs[1], sharex=ax_p)
        ax_v = self.fig.add_subplot(gs[2], sharex=ax_p)
        for a in (ax_p, ax_r, ax_v):
            self._style_ax(a)

        x = np.arange(len(dates))
        ax_p.plot(x, closes, color=C['accent'], linewidth=1)
        ax_p.set_ylabel('价格', color=C['txt2'], fontsize=9)

        r6, r14 = rsi(closes, 6), rsi(closes, 14)
        v = ~np.isnan(r14)
        if np.any(v):
            ax_r.plot(x[v], r6[v], color=C['peach'], linewidth=1, label='RSI6')
            ax_r.plot(x[v], r14[v], color=C['purple'], linewidth=1, label='RSI14')
            ax_r.axhline(y=70, color=C['red'], linewidth=0.8, ls='--')
            ax_r.axhline(y=30, color=C['green'], linewidth=0.8, ls='--')
            ax_r.fill_between(x[v], 70, 100, alpha=0.05, color=C['red'])
            ax_r.fill_between(x[v], 0, 30, alpha=0.05, color=C['green'])
            ax_r.set_ylim(0, 100)
            ax_r.legend(fontsize=8, framealpha=0.3, facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])
        ax_r.set_ylabel('RSI', color=C['txt2'], fontsize=9)

        ax_p.set_title(f"{code} {fname} - RSI(6,14)", color=C['txt'],
                       fontsize=11, fontweight='bold', pad=8)

        vc = [C['green'] if closes[i] >= (closes[i - 1] if i > 0 else closes[i]) else C['red']
              for i in range(len(closes))]
        ax_v.bar(x, volumes, width=0.6, color=vc, alpha=0.5)
        ax_v.set_ylabel('成交量', color=C['txt2'], fontsize=9)

        self._set_xticks(ax_v, dates)
        plt.setp(ax_p.get_xticklabels(), visible=False)
        plt.setp(ax_r.get_xticklabels(), visible=False)
        self._auto_layout(hspace=0.3)
        self.canvas.draw()

    # --- 成交量分析 ---
    def _draw_vol(self, code, fname, dates, opens, highs, lows, closes, volumes, chg):
        gs = GridSpec(3, 1, height_ratios=[2, 1, 1], hspace=0.08, figure=self.fig)
        ax_p = self.fig.add_subplot(gs[0])
        ax_v = self.fig.add_subplot(gs[1], sharex=ax_p)
        ax_c = self.fig.add_subplot(gs[2], sharex=ax_p)
        for a in (ax_p, ax_v, ax_c):
            self._style_ax(a)

        x = np.arange(len(dates))
        up = closes >= opens
        down = ~up  # 修复: 用 ~up 代替 ~closes

        # K线
        ax_p.vlines(x[up], lows[up], highs[up], color=C['green'], linewidths=0.5)
        ax_p.vlines(x[down], lows[down], highs[down], color=C['red'], linewidths=0.5)
        if np.any(up):
            ax_p.bar(x[up], np.maximum(closes[up] - opens[up], 0.002),
                     bottom=opens[up], width=0.6, color=C['green'])
        if np.any(down):
            ax_p.bar(x[down], np.maximum(opens[down] - closes[down], 0.002),
                     bottom=closes[down], width=0.6, color=C['red'])
        ax_p.set_ylabel('价格', color=C['txt2'], fontsize=9)

        # 成交量 + MA5
        vc = [C['green'] if c >= o else C['red'] for c, o in zip(closes, opens)]
        ax_v.bar(x, volumes, width=0.6, color=vc, alpha=0.6)
        ax_v.set_ylabel('成交量', color=C['txt2'], fontsize=9)
        vm = ma(volumes, 5)
        vv = ~np.isnan(vm)
        if np.any(vv):
            ax_v.plot(x[vv], vm[vv], color=C['yellow'], linewidth=1, label='VOL MA5')
        ax_v.legend(fontsize=8, framealpha=0.3, facecolor=C['bg'], edgecolor=C['border'], labelcolor=C['txt'])

        # 涨跌幅
        cc = [C['green'] if v >= 0 else C['red'] for v in chg]
        ax_c.bar(x, chg, width=0.6, color=cc, alpha=0.6)
        ax_c.axhline(y=0, color=C['txt2'], linewidth=0.5, alpha=0.3)
        ax_c.set_ylabel('涨跌幅%', color=C['txt2'], fontsize=9)

        ax_p.set_title(f"{code} {fname} - 成交量分析", color=C['txt'],
                       fontsize=11, fontweight='bold', pad=8)

        self._set_xticks(ax_c, dates)
        plt.setp(ax_p.get_xticklabels(), visible=False)
        plt.setp(ax_v.get_xticklabels(), visible=False)
        self._auto_layout(hspace=0.35)
        self.canvas.draw()


def run():
    """启动图形界面。"""
    root = tk.Tk()
    app = App(root)
    root.mainloop()
    close_connection()


if __name__ == "__main__":
    run()
