import akshare as ak
import pandas as pd
import matplotlib
matplotlib.use('Agg')  # 非交互式后端，GUI用display
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.figure import Figure
import sys
import os
from datetime import datetime, timedelta
from io import BytesIO

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QComboBox, QPushButton, QLabel, QTextBrowser, QTabWidget,
    QGroupBox, QFormLayout, QScrollArea, QTableWidget, QTableWidgetItem,
    QSplashScreen, QMessageBox, QDateEdit, QLineEdit, QCompleter,
    QButtonGroup, QRadioButton
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QDate, QStringListModel
from PyQt5.QtGui import QFont, QPixmap, QIcon

# 设置matplotlib中文字体
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

# 全局样式
STYLE_SHEET = """
QMainWindow { background-color: #f5f5f5; }
QGroupBox {
    font-weight: bold;
    border: 2px solid #dcdcdc;
    border-radius: 8px;
    margin-top: 10px;
    padding-top: 10px;
}
QGroupBox::title {
    subcontrol-origin: margin;
    left: 10px;
    padding: 0 5px;
    color: #2c3e50;
}
QPushButton {
    background-color: #3498db;
    color: white;
    border: none;
    border-radius: 5px;
    padding: 8px 16px;
    font-weight: bold;
}
QPushButton:hover { background-color: #2980b9; }
QPushButton:pressed { background-color: #21618c; }
QPushButton:disabled { background-color: #bdc3c7; }
QPushButton#activeBtn { background-color: #27ae60; }
QPushButton#activeBtn:hover { background-color: #219a52; }
QLineEdit {
    border: 1px solid #bdc3c7;
    border-radius: 5px;
    padding: 8px 10px;
    background-color: white;
}
QLineEdit:focus { border-color: #3498db; }
QTabWidget::pane { border: 1px solid #dcdcdc; border-radius: 5px; background-color: white; }
QTabBar::tab {
    background-color: #ecf0f1;
    padding: 8px 20px;
    border-top-left-radius: 5px;
    border-top-right-radius: 5px;
}
QTabBar::tab:selected { background-color: white; color: #3498db; font-weight: bold; }
QTableWidget {
    border: 1px solid #dcdcdc;
    border-radius: 5px;
    background-color: white;
    gridline-color: #ecf0f1;
}
QHeaderView::section {
    background-color: #3498db;
    color: white;
    padding: 8px;
    border: none;
    font-weight: bold;
}
QRadioButton { padding: 5px; }
"""


class DataLoadingThread(QThread):
    """数据加载线程"""
    finished = pyqtSignal(dict)
    error = pyqtSignal(str)
    progress = pyqtSignal(str)

    def run(self):
        try:
            self.progress.emit("正在获取基金列表...")
            fund_list = ak.fund_name_em()

            self.progress.emit("正在获取A股列表...")
            stock_list = ak.stock_info_a_code_name()

            self.progress.emit(f"获取完成: {len(fund_list)}只基金, {len(stock_list)}只股票")
            self.finished.emit({'fund_list': fund_list, 'stock_list': stock_list})
        except Exception as e:
            self.error.emit(str(e))


class InteractiveChart(FigureCanvas):
    """交互式matplotlib图表，支持缩放和平移"""
    def __init__(self, parent=None, width=10, height=4, dpi=100):
        self.fig = Figure(figsize=(width, height), dpi=dpi)
        self.axes = self.fig.add_subplot(111)
        super().__init__(self.fig)
        self.setParent(parent)
        self.fig.tight_layout()

        # 启用交互功能
        self.fig.canvas.mpl_connect('scroll_event', self.on_scroll)

    def on_scroll(self, event):
        """鼠标滚轮缩放"""
        if event.inaxes != self.axes:
            return
        scale_factor = 1.1 if event.button == 'up' else 0.9
        xlim = self.axes.get_xlim()
        x_range = (xlim[1] - xlim[0]) * scale_factor
        x_center = (xlim[0] + xlim[1]) / 2
        self.axes.set_xlim([x_center - x_range/2, x_center + x_range/2])
        self.draw()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.fund_list = None
        self.stock_list = None
        self.current_code = None
        self.current_type = None  # 'fund' or 'stock'
        self.nav_data = None
        self.nav_figure = None

        self.init_ui()
        self.load_data_list()

    def init_ui(self):
        """初始化UI"""
        self.setWindowTitle("基金/股票查询工具 - 净值走势与收益率计算")
        self.setGeometry(100, 100, 1300, 850)

        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(15, 15, 15, 15)
        main_layout.setSpacing(10)

        # 标题
        title = QLabel("基金/股票查询工具")
        title.setFont(QFont("Microsoft YaHei", 18, QFont.Bold))
        title.setAlignment(Qt.AlignCenter)
        title.setStyleSheet("color: #2c3e50; padding: 10px;")
        main_layout.addWidget(title)

        # 查询区域
        search_group = QGroupBox("代码查询")
        search_layout = QHBoxLayout()

        # 类型选择
        self.type_group = QButtonGroup()
        self.fund_radio = QRadioButton("基金")
        self.fund_radio.setChecked(True)
        self.stock_radio = QRadioButton("股票")
        self.type_group.addButton(self.fund_radio, 1)
        self.type_group.addButton(self.stock_radio, 2)
        self.type_group.buttonClicked.connect(self.on_type_changed)
        search_layout.addWidget(QLabel("类型:"))
        search_layout.addWidget(self.fund_radio)
        search_layout.addWidget(self.stock_radio)
        search_layout.addSpacing(20)

        # 搜索框
        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("输入代码或名称搜索...")
        self.search_input.setMinimumWidth(400)
        self.search_input.setFont(QFont("Microsoft YaHei", 10))
        self.search_input.returnPressed.connect(self.query)
        self.search_input.textChanged.connect(self.on_search_changed)

        # 自动补全
        self.completer = QCompleter()
        self.completer.setCaseSensitivity(Qt.CaseInsensitive)
        self.completer.setFilterMode(Qt.MatchContains)
        self.search_input.setCompleter(self.completer)
        self.completer.activated.connect(self.on_completer_selected)

        search_layout.addWidget(self.search_input)

        # 查询按钮
        self.query_btn = QPushButton("查询")
        self.query_btn.setFixedWidth(100)
        self.query_btn.clicked.connect(self.query)
        search_layout.addWidget(self.query_btn)

        # 刷新按钮
        self.refresh_btn = QPushButton("刷新")
        self.refresh_btn.setFixedWidth(80)
        self.refresh_btn.clicked.connect(self.load_data_list)
        search_layout.addWidget(self.refresh_btn)

        search_layout.addStretch()
        search_group.setLayout(search_layout)
        main_layout.addWidget(search_group)

        # 选项卡
        self.tabs = QTabWidget()

        # 选项卡1: 基础信息
        self.info_tab = QWidget()
        self.init_info_tab()
        self.tabs.addTab(self.info_tab, "基本信息")

        # 选项卡2: 净值走势(可缩放)
        self.chart_tab = QWidget()
        self.init_chart_tab()
        self.tabs.addTab(self.chart_tab, "净值走势(可缩放)")

        # 选项卡3: 收益率计算
        self.return_tab = QWidget()
        self.init_return_tab()
        self.tabs.addTab(self.return_tab, "收益率计算")

        # 选项卡4: 历史数据
        self.history_tab = QWidget()
        self.init_history_tab()
        self.tabs.addTab(self.history_tab, "历史数据")

        main_layout.addWidget(self.tabs)

        # 状态栏
        self.status_label = QLabel("就绪")
        self.status_label.setStyleSheet("padding: 5px; color: #7f8c8d;")
        main_layout.addWidget(self.status_label)

        self.setStyleSheet(STYLE_SHEET)

    def init_info_tab(self):
        """基本信息选项卡"""
        layout = QVBoxLayout(self.info_tab)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll.setStyleSheet("QScrollArea { border: none; }")

        container = QWidget()
        container_layout = QVBoxLayout(container)

        # 基本信息
        info_group = QGroupBox("基本信息")
        info_layout = QVBoxLayout()

        self.code_label = QLabel("代码: -")
        self.name_label = QLabel("名称: -")
        self.type_label = QLabel("类型: -")

        for label in [self.code_label, self.name_label, self.type_label]:
            label.setFont(QFont("Microsoft YaHei", 10))
            info_layout.addWidget(label)

        info_group.setLayout(info_layout)
        container_layout.addWidget(info_group)

        # 详细信息表格
        detail_group = QGroupBox("详细数据")
        detail_layout = QVBoxLayout()

        self.detail_table = QTableWidget()
        self.detail_table.setColumnCount(2)
        self.detail_table.setHorizontalHeaderLabels(["指标", "数值"])
        self.detail_table.horizontalHeader().setStretchLastSection(True)
        self.detail_table.setEditTriggers(QTableWidget.NoEditTriggers)
        detail_layout.addWidget(self.detail_table)

        detail_group.setLayout(detail_layout)
        container_layout.addWidget(detail_group)
        container_layout.addStretch()

        scroll.setWidget(container)
        layout.addWidget(scroll)

    def init_chart_tab(self):
        """净值走势选项卡 - 可缩放"""
        layout = QVBoxLayout(self.chart_tab)

        # 时间范围快捷按钮
        range_group = QGroupBox("时间范围")
        range_layout = QHBoxLayout()

        self.week_chart_btn = QPushButton("近一周")
        self.week_chart_btn.setObjectName("activeBtn")
        self.week_chart_btn.setFixedSize(90, 35)
        self.week_chart_btn.clicked.connect(lambda: self.update_chart_range('week'))

        self.month_chart_btn = QPushButton("近一月")
        self.month_chart_btn.setFixedSize(90, 35)
        self.month_chart_btn.clicked.connect(lambda: self.update_chart_range('month'))

        self.quarter_chart_btn = QPushButton("近三月")
        self.quarter_chart_btn.setFixedSize(90, 35)
        self.quarter_chart_btn.clicked.connect(lambda: self.update_chart_range('quarter'))

        self.year_chart_btn = QPushButton("近一年")
        self.year_chart_btn.setFixedSize(90, 35)
        self.year_chart_btn.clicked.connect(lambda: self.update_chart_range('year'))

        self.all_chart_btn = QPushButton("全部")
        self.all_chart_btn.setFixedSize(90, 35)
        self.all_chart_btn.clicked.connect(lambda: self.update_chart_range('all'))

        hint_label = QLabel("提示: 鼠标滚轮可缩放图表，按住左键可拖动")
        hint_label.setStyleSheet("color: #7f8c8d; font-size: 11px;")

        range_layout.addWidget(self.week_chart_btn)
        range_layout.addWidget(self.month_chart_btn)
        range_layout.addWidget(self.quarter_chart_btn)
        range_layout.addWidget(self.year_chart_btn)
        range_layout.addWidget(self.all_chart_btn)
        range_layout.addWidget(hint_label)
        range_layout.addStretch()

        range_group.setLayout(range_layout)
        layout.addWidget(range_group)

        # 交互式图表
        self.chart_widget = QWidget()
        chart_layout = QVBoxLayout(self.chart_widget)

        self.chart_canvas = InteractiveChart(self.chart_widget, width=12, height=5, dpi=100)
        chart_layout.addWidget(self.chart_canvas)

        layout.addWidget(self.chart_widget)

        # 数据表格
        self.nav_table = QTableWidget()
        self.nav_table.setColumnCount(5)
        self.nav_table.setHorizontalHeaderLabels(["日期", "开盘", "收盘", "最高", "最低"])
        self.nav_table.horizontalHeader().setStretchLastSection(True)
        self.nav_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.nav_table.setMinimumHeight(180)
        layout.addWidget(self.nav_table)

        self.current_chart_range = 'quarter'  # 默认近三月

    def init_return_tab(self):
        """收益率计算选项卡"""
        layout = QVBoxLayout(self.return_tab)

        # 日期选择
        date_group = QGroupBox("日期选择")
        date_layout = QHBoxLayout()

        date_layout.addWidget(QLabel("买入日期:"))
        self.buy_date_edit = QDateEdit()
        self.buy_date_edit.setCalendarPopup(True)
        self.buy_date_edit.setDate(QDate.currentDate().addMonths(-1))
        date_layout.addWidget(self.buy_date_edit)

        date_layout.addSpacing(30)
        date_layout.addWidget(QLabel("卖出日期:"))
        self.sell_date_edit = QDateEdit()
        self.sell_date_edit.setCalendarPopup(True)
        self.sell_date_edit.setDate(QDate.currentDate())
        date_layout.addWidget(self.sell_date_edit)

        date_layout.addStretch()
        date_group.setLayout(date_layout)
        layout.addWidget(date_group)

        # 快捷计算按钮
        quick_group = QGroupBox("快捷收益率计算")
        quick_layout = QHBoxLayout()

        self.week_btn = QPushButton("周收益率")
        self.week_btn.setFixedSize(100, 40)
        self.week_btn.clicked.connect(lambda: self.calculate_return('week'))

        self.month_btn = QPushButton("月收益率")
        self.month_btn.setFixedSize(100, 40)
        self.month_btn.clicked.connect(lambda: self.calculate_return('month'))

        self.quarter_btn = QPushButton("近三月收益率")
        self.quarter_btn.setFixedSize(110, 40)
        self.quarter_btn.clicked.connect(lambda: self.calculate_return('quarter'))

        self.year_btn = QPushButton("年收益率")
        self.year_btn.setFixedSize(100, 40)
        self.year_btn.clicked.connect(lambda: self.calculate_return('year'))

        self.custom_btn = QPushButton("自定义计算")
        self.custom_btn.setFixedSize(110, 40)
        self.custom_btn.clicked.connect(self.calculate_custom_return)

        for btn in [self.week_btn, self.month_btn, self.quarter_btn, self.year_btn, self.custom_btn]:
            quick_layout.addWidget(btn)

        quick_layout.addStretch()
        quick_group.setLayout(quick_layout)
        layout.addWidget(quick_group)

        # 结果显示
        result_group = QGroupBox("计算结果")
        result_layout = QVBoxLayout()

        self.return_result_label = QLabel("请查询后点击计算按钮")
        self.return_result_label.setFont(QFont("Microsoft YaHei", 14))
        self.return_result_label.setAlignment(Qt.AlignCenter)
        self.return_result_label.setStyleSheet("color: #27ae60; padding: 15px;")
        result_layout.addWidget(self.return_result_label)

        # 收益率表格
        self.return_table = QTableWidget()
        self.return_table.setColumnCount(4)
        self.return_table.setHorizontalHeaderLabels(["日期", "价格", "份额", "累计收益率"])
        self.return_table.horizontalHeader().setStretchLastSection(True)
        self.return_table.setMinimumHeight(150)
        result_layout.addWidget(self.return_table)

        result_group.setLayout(result_layout)
        layout.addWidget(result_group)

    def init_history_tab(self):
        """历史数据选项卡"""
        layout = QVBoxLayout(self.history_tab)

        btn_layout = QHBoxLayout()
        self.history_query_btn = QPushButton("查询历史数据")
        self.history_query_btn.clicked.connect(self.query_history)
        btn_layout.addWidget(self.history_query_btn)
        btn_layout.addStretch()
        layout.addLayout(btn_layout)

        self.history_table = QTableWidget()
        self.history_table.setColumnCount(6)
        self.history_table.setHorizontalHeaderLabels(["日期", "开盘", "收盘", "最高", "最低", "成交量"])
        self.history_table.horizontalHeader().setStretchLastSection(True)
        self.history_table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self.history_table)

    def load_data_list(self):
        """加载基金和股票列表"""
        self.status_label.setText("正在加载数据列表...")
        self.search_input.setEnabled(False)

        self.loading_thread = DataLoadingThread()
        self.loading_thread.finished.connect(self.on_data_loaded)
        self.loading_thread.error.connect(self.on_load_error)
        self.loading_thread.start()

    def on_data_loaded(self, result):
        """数据加载完成"""
        try:
            self.fund_list = result['fund_list']
            self.stock_list = result['stock_list']
            self.update_completer()
            self.search_input.setEnabled(True)
            self.status_label.setText(f"已加载 {len(self.fund_list)}只基金, {len(self.stock_list)}只股票")
        except Exception as e:
            self.status_label.setText(f"加载失败: {str(e)}")
            self.search_input.setEnabled(True)

    def on_load_error(self, error_msg):
        """加载失败"""
        self.status_label.setText(f"加载失败: {error_msg}")
        self.search_input.setEnabled(True)
        QMessageBox.warning(self, "错误", f"加载失败:\n{error_msg}")

    def update_completer(self):
        """更新自动补全列表"""
        fund_items = [f"{r['基金代码']} - {r['基金简称']} (基金)"
                     for _, r in self.fund_list.iterrows()]
        stock_items = [f"{r['code']} - {r['name']} (股票)"
                      for _, r in self.stock_list.iterrows()]

        self.all_items = fund_items + stock_items
        self.completer_model = QStringListModel(self.all_items)
        self.completer.setModel(self.completer_model)

    def on_type_changed(self):
        """类型切换"""
        self.search_input.clear()
        self.current_code = None

    def on_search_changed(self, text):
        """搜索框变化"""
        if " - " in text and ("基金)" in text or "股票)" in text):
            code = text.split(" - ")[0].strip()
            self.current_code = code

    def on_completer_selected(self, text):
        """自动补全选中"""
        if " - " in text:
            code = text.split(" - ")[0].strip()
            self.current_code = code

    def query(self):
        """查询"""
        search_text = self.search_input.text().strip()

        # 从输入提取代码
        if search_text:
            if search_text.isdigit() and len(search_text) == 6:
                self.current_code = search_text
            elif " - " in search_text:
                code = search_text.split(" - ")[0].strip()
                if code.isdigit():
                    self.current_code = code

        if not self.current_code:
            QMessageBox.warning(self, "提示", "请输入代码进行查询")
            return

        # 判断类型
        if self.fund_radio.isChecked():
            self.current_type = 'fund'
        else:
            self.current_type = 'stock'

        self.status_label.setText(f"正在查询 {self.current_code}...")

        try:
            if self.current_type == 'fund':
                self.query_fund()
            else:
                self.query_stock()

            self.status_label.setText(f"{self.current_code} 查询完成")
            self.tabs.setCurrentIndex(0)

        except Exception as e:
            self.status_label.setText(f"查询失败: {str(e)}")
            QMessageBox.warning(self, "错误", f"查询失败:\n{str(e)}")

    def query_fund(self):
        """查询基金"""
        # 基本信息
        fund_info = ak.fund_individual_basic_info_xq(symbol=self.current_code)

        self.code_label.setText(f"代码: {self.current_code}")

        info_dict = {}
        for _, row in fund_info.iterrows():
            if len(row) >= 2:
                key = str(row.iloc[0]) if pd.notna(row.iloc[0]) else ""
                value = str(row.iloc[1]) if pd.notna(row.iloc[1]) else ""
                if key:
                    info_dict[key] = value

        name = info_dict.get('基金简称', info_dict.get('基金全称', '-'))
        self.name_label.setText(f"名称: {name}")
        self.type_label.setText(f"类型: 基金")

        # 更新详情表格
        self.detail_table.setRowCount(len(info_dict))
        for i, (k, v) in enumerate(info_dict.items()):
            self.detail_table.setItem(i, 0, QTableWidgetItem(k))
            self.detail_table.setItem(i, 1, QTableWidgetItem(v))

        # 净值数据
        self.nav_data = ak.fund_open_fund_info_em(
            symbol=self.current_code, indicator="单位净值走势"
        )
        self.nav_data['日期'] = pd.to_datetime(self.nav_data['净值日期'])
        self.nav_data = self.nav_data.sort_values('日期')

        self.update_chart()
        self.update_nav_table()

    def query_stock(self):
        """查询股票"""
        # 股票信息
        try:
            stock_info = ak.stock_individual_info_em(symbol=self.current_code)
            info_dict = {str(row.iloc[0]): str(row.iloc[1]) for _, row in stock_info.iterrows()}
        except:
            info_dict = {}

        self.code_label.setText(f"代码: {self.current_code}")
        self.name_label.setText(f"名称: {info_dict.get('股票简称', '-')}")
        self.type_label.setText(f"类型: 股票")

        self.detail_table.setRowCount(len(info_dict))
        for i, (k, v) in enumerate(info_dict.items()):
            self.detail_table.setItem(i, 0, QTableWidgetItem(k))
            self.detail_table.setItem(i, 1, QTableWidgetItem(v))

        # 股票行情数据
        self.nav_data = ak.stock_zh_a_hist(
            symbol=self.current_code, period="daily",
            start_date="20200101", end_date=datetime.now().strftime('%Y%m%d'),
            adjust="hfq"
        )
        self.nav_data['日期'] = pd.to_datetime(self.nav_data['日期'])
        self.nav_data = self.nav_data.sort_values('日期')

        self.update_chart()
        self.update_stock_table()

    def update_chart(self):
        """更新图表"""
        if self.nav_data is None or self.nav_data.empty:
            return

        self.update_chart_range(self.current_chart_range)

    def update_chart_range(self, period):
        """根据时间范围更新图表"""
        self.current_chart_range = period

        # 更新按钮样式
        for btn in [self.week_chart_btn, self.month_chart_btn, self.quarter_chart_btn,
                   self.year_chart_btn, self.all_chart_btn]:
            btn.setStyleSheet("")

        btn_map = {'week': self.week_chart_btn, 'month': self.month_chart_btn,
                   'quarter': self.quarter_chart_btn, 'year': self.year_chart_btn,
                   'all': self.all_chart_btn}
        btn_map.get(period, self.all_chart_btn).setStyleSheet("background-color: #27ae60; color: white;")

        # 筛选数据
        df = self.nav_data.copy()
        today = datetime.now()

        if period == 'week':
            start_date = today - timedelta(days=7)
        elif period == 'month':
            start_date = today - timedelta(days=30)
        elif period == 'quarter':
            start_date = today - timedelta(days=90)
        elif period == 'year':
            start_date = today - timedelta(days=365)
        else:
            start_date = df['日期'].min()

        df = df[df['日期'] >= start_date]

        # 绘制
        self.chart_canvas.axes.clear()

        if self.current_type == 'fund':
            self.chart_canvas.axes.plot(df['日期'], df['单位净值'],
                                       color='#3498db', linewidth=2, label='单位净值')
            ylabel = '净值'
        else:
            self.chart_canvas.axes.plot(df['日期'], df['收盘'],
                                       color='#3498db', linewidth=2, label='收盘价')
            self.chart_canvas.axes.fill_between(df['日期'], df['最低'], df['最高'],
                                               alpha=0.3, color='#3498db')
            ylabel = '价格'

        title = f"{self.current_code} {'净值' if self.current_type == 'fund' else '行情'}走势"
        self.chart_canvas.axes.set_title(title, fontsize=14, pad=10)
        self.chart_canvas.axes.set_xlabel('日期', fontsize=11)
        self.chart_canvas.axes.set_ylabel(ylabel, fontsize=11)
        self.chart_canvas.axes.grid(True, linestyle='--', alpha=0.7)
        self.chart_canvas.axes.legend()
        self.chart_canvas.fig.tight_layout()
        self.chart_canvas.draw()

    def update_nav_table(self):
        """更新基金净值表格"""
        if self.nav_data is None:
            return

        df = self.nav_data.tail(30).sort_values('日期', ascending=False)

        self.nav_table.setColumnCount(5)
        self.nav_table.setHorizontalHeaderLabels(["日期", "单位净值", "累计净值", "日增长率", "申购状态"])
        self.nav_table.setRowCount(len(df))

        for i, (_, row) in enumerate(df.iterrows()):
            self.nav_table.setItem(i, 0, QTableWidgetItem(row['日期'].strftime('%Y-%m-%d')))
            self.nav_table.setItem(i, 1, QTableWidgetItem(f"{row['单位净值']:.4f}"))
            self.nav_table.setItem(i, 2, QTableWidgetItem(f"{row.get('累计净值', '-'):.4f}" if '累计净值' in row else "-"))
            self.nav_table.setItem(i, 3, QTableWidgetItem(f"{row['日增长率']:.2f}%" if '日增长率' in row else "-"))
            self.nav_table.setItem(i, 4, QTableWidgetItem("-"))

    def update_stock_table(self):
        """更新股票行情表格"""
        if self.nav_data is None:
            return

        df = self.nav_data.tail(30).sort_values('日期', ascending=False)

        self.nav_table.setColumnCount(6)
        self.nav_table.setHorizontalHeaderLabels(["日期", "开盘", "收盘", "最高", "最低", "成交量"])
        self.nav_table.setRowCount(len(df))

        for i, (_, row) in enumerate(df.iterrows()):
            self.nav_table.setItem(i, 0, QTableWidgetItem(row['日期'].strftime('%Y-%m-%d')))
            self.nav_table.setItem(i, 1, QTableWidgetItem(f"{row['开盘']:.2f}"))
            self.nav_table.setItem(i, 2, QTableWidgetItem(f"{row['收盘']:.2f}"))
            self.nav_table.setItem(i, 3, QTableWidgetItem(f"{row['最高']:.2f}"))
            self.nav_table.setItem(i, 4, QTableWidgetItem(f"{row['最低']:.2f}"))
            self.nav_table.setItem(i, 5, QTableWidgetItem(f"{row['成交量']:.0f}"))

    def calculate_return(self, period_type):
        """计算收益率"""
        if not self.current_code:
            QMessageBox.warning(self, "提示", "请先查询")
            return

        try:
            today = datetime.now()
            days_map = {'week': 7, 'month': 30, 'quarter': 90, 'year': 365}
            days_back = days_map.get(period_type, 30)
            period_name = {'week': '近一周', 'month': '近一月', 'quarter': '近三月', 'year': '近一年'}.get(period_type, period_type)

            start_date = (today - timedelta(days=days_back)).strftime('%Y-%m-%d')
            end_date = today.strftime('%Y-%m-%d')

            df = self.nav_data.copy()
            mask = (df['日期'] >= start_date) & (df['日期'] <= end_date)
            df_filtered = df[mask]

            if len(df_filtered) < 2:
                QMessageBox.warning(self, "提示", "数据不足")
                return

            # 获取价格列
            if self.current_type == 'fund':
                price_col = '单位净值'
            else:
                price_col = '收盘'

            start_price = df_filtered.iloc[0][price_col]
            end_price = df_filtered.iloc[-1][price_col]
            start_date_disp = df_filtered.iloc[0]['日期'].strftime('%Y-%m-%d')
            end_date_disp = df_filtered.iloc[-1]['日期'].strftime('%Y-%m-%d')

            return_rate = ((end_price - start_price) / start_price) * 100

            color = '#27ae60' if return_rate >= 0 else '#e74c3c'
            result_text = f"""
<div style="font-size: 16px; text-align: center;">
<p style="color: #2c3e50;">{period_name}收益率</p>
<p style="font-size: 28px; color: {color};">
{'+' if return_rate >= 0 else ''}{return_rate:.2f}%
</p>
<p style="color: #7f8c8d;">
买入: {start_date_disp} @ {start_price:.4f}<br/>
卖出: {end_date_disp} @ {end_price:.4f}
</p>
</div>
"""
            self.return_result_label.setText(result_text)

            # 更新表格
            self.return_table.setRowCount(len(df_filtered))
            for i, (_, row) in enumerate(df_filtered.iterrows()):
                price = row[price_col]
                shares = 10000 / start_price
                period_ret = ((price - start_price) / start_price) * 100

                self.return_table.setItem(i, 0, QTableWidgetItem(row['日期'].strftime('%Y-%m-%d')))
                self.return_table.setItem(i, 1, QTableWidgetItem(f"{price:.4f}"))
                self.return_table.setItem(i, 2, QTableWidgetItem(f"{shares:.2f}份"))
                self.return_table.setItem(i, 3, QTableWidgetItem(f"{'+' if period_ret >= 0 else ''}{period_ret:.2f}%"))

            self.tabs.setCurrentIndex(2)

        except Exception as e:
            QMessageBox.warning(self, "错误", f"计算失败:\n{str(e)}")

    def calculate_custom_return(self):
        """自定义日期收益率"""
        if not self.current_code:
            QMessageBox.warning(self, "提示", "请先查询")
            return

        try:
            buy_date = self.buy_date_edit.date().toPyDate()
            sell_date = self.sell_date_edit.date().toPyDate()

            if buy_date >= sell_date:
                QMessageBox.warning(self, "提示", "买入日期必须早于卖出日期")
                return

            df = self.nav_data.copy()
            mask = (df['日期'] >= buy_date.strftime('%Y-%m-%d')) & (df['日期'] <= sell_date.strftime('%Y-%m-%d'))
            df_filtered = df[mask]

            if len(df_filtered) < 2:
                QMessageBox.warning(self, "提示", "所选范围内数据不足")
                return

            price_col = '单位净值' if self.current_type == 'fund' else '收盘'
            start_price = df_filtered.iloc[0][price_col]
            end_price = df_filtered.iloc[-1][price_col]

            return_rate = ((end_price - start_price) / start_price) * 100
            days = (sell_date - buy_date).days

            color = '#27ae60' if return_rate >= 0 else '#e74c3c'
            self.return_result_label.setText(f"""
<div style="font-size: 16px; text-align: center;">
<p style="color: #2c3e50;">自定义收益率 ({days}天)</p>
<p style="font-size: 28px; color: {color};">
{'+' if return_rate >= 0 else ''}{return_rate:.2f}%
</p>
<p style="color: #7f8c8d;">
买入日期: {buy_date} @ {start_price:.4f}<br/>
卖出日期: {sell_date} @ {end_price:.4f}
</p>
</div>
""")

            self.return_table.setRowCount(len(df_filtered))
            for i, (_, row) in enumerate(df_filtered.iterrows()):
                price = row[price_col]
                shares = 10000 / start_price
                period_ret = ((price - start_price) / start_price) * 100
                self.return_table.setItem(i, 0, QTableWidgetItem(row['日期'].strftime('%Y-%m-%d')))
                self.return_table.setItem(i, 1, QTableWidgetItem(f"{price:.4f}"))
                self.return_table.setItem(i, 2, QTableWidgetItem(f"{shares:.2f}份"))
                self.return_table.setItem(i, 3, QTableWidgetItem(f"{'+' if period_ret >= 0 else ''}{period_ret:.2f}%"))

            self.tabs.setCurrentIndex(2)

        except Exception as e:
            QMessageBox.warning(self, "错误", f"计算失败:\n{str(e)}")

    def query_history(self):
        """查询历史数据"""
        if not self.current_code:
            QMessageBox.warning(self, "提示", "请先查询")
            return

        df = self.nav_data.sort_values('日期', ascending=False)

        self.history_table.setRowCount(min(len(df), 100))

        for i, (_, row) in enumerate(df.head(100).iterrows()):
            self.history_table.setItem(i, 0, QTableWidgetItem(row['日期'].strftime('%Y-%m-%d')))
            self.history_table.setItem(i, 1, QTableWidgetItem(f"{row.get('开盘', row.get('单位净值', '-')):.2f}"))
            self.history_table.setItem(i, 2, QTableWidgetItem(f"{row.get('收盘', '-'):.2f}" if '收盘' in row else "-"))
            self.history_table.setItem(i, 3, QTableWidgetItem(f"{row.get('最高', '-'):.2f}" if '最高' in row else "-"))
            self.history_table.setItem(i, 4, QTableWidgetItem(f"{row.get('最低', '-'):.2f}" if '最低' in row else "-"))
            self.history_table.setItem(i, 5, QTableWidgetItem(f"{row.get('成交量', '-'):.0f}" if '成交量' in row else "-"))


def main():
    app = QApplication(sys.argv)
    app.setStyle('Fusion')
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
