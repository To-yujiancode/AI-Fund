import sys
import time
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QLabel, QComboBox, QPushButton,
                             QDateEdit, QGroupBox, QMessageBox, QFormLayout,
                             QProgressBar)
from PyQt5.QtCore import Qt, QDate, QThread, pyqtSignal
from PyQt5.QtGui import QFont
import pyqtgraph as pg

# 设置 pyqtgraph 样式
pg.setConfigOptions(antialias=True)
pg.setConfigOption('background', 'w')
pg.setConfigOption('foreground', 'k')


# --- 后台线程类，防止加载数据时界面卡死 ---
class LoadStockThread(QThread):
    finished = pyqtSignal(pd.DataFrame)
    error = pyqtSignal(str)

    def __init__(self):
        super().__init__()

    def run(self):
        max_retries = 3
        for i in range(max_retries):
            try:
                # 尝试获取数据
                df = ak.stock_zh_a_spot_em()
                if df is not None and not df.empty:
                    self.finished.emit(df)
                    return
                else:
                    raise Exception("数据为空")
            except Exception as e:
                if i == max_retries - 1:
                    self.error.emit(f"加载失败 (重试{max_retries}次后放弃): {str(e)}")
                else:
                    time.sleep(2)  # 等待2秒后重试


class StockApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("AKShare 股票分析工具 (增强版)")
        self.resize(1000, 800)

        self.stock_data = None
        self.stock_info = {}
        self.load_thread = None

        self.init_ui()
        # 启动后台加载线程
        self.start_loading()

    def init_ui(self):
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)

        # 顶部状态栏
        self.status_bar = QLabel("状态: 正在加载股票列表...")
        self.status_bar.setStyleSheet("color: blue; font-weight: bold; padding: 5px;")
        main_layout.addWidget(self.status_bar)

        # 1. 股票选择区
        top_group = QGroupBox("股票/基金选择")
        top_layout = QHBoxLayout(top_group)

        self.code_label = QLabel("代码:")
        self.code_combo = QComboBox()
        self.code_combo.setEditable(True)
        self.code_combo.currentTextChanged.connect(self.on_code_changed)
        self.code_combo.setEnabled(False)  # 加载完成前禁用

        self.name_label = QLabel("名称:")
        self.name_label.setText("等待数据加载...")
        self.name_label.setFont(QFont("Arial", 10, QFont.Bold))

        self.search_btn = QPushButton("查询详情")
        self.search_btn.setEnabled(False)
        self.search_btn.clicked.connect(self.fetch_stock_details)

        top_layout.addWidget(self.code_label)
        top_layout.addWidget(self.code_combo, 2)
        top_layout.addWidget(self.name_label)
        top_layout.addWidget(self.search_btn)

        # 2. 图表区
        self.graph_widget = pg.PlotWidget(title="股价走势图")
        self.graph_widget.setLabel('left', '价格')
        self.graph_widget.setLabel('bottom', '日期')
        self.graph_widget.showGrid(x=True, y=True, alpha=0.3)

        self.ma5_line = self.graph_widget.plot(pen=pg.mkPen('r', width=2), name='MA5')
        self.ma20_line = self.graph_widget.plot(pen=pg.mkPen('b', width=2), name='MA20')

        # 3. 底部：收益率计算区
        bottom_group = QGroupBox("收益率计算")
        bottom_layout = QFormLayout(bottom_group)

        self.buy_date_edit = QDateEdit()
        self.buy_date_edit.setDate(QDate.currentDate().addDays(-30))
        self.buy_date_edit.setCalendarPopup(True)

        self.sell_date_edit = QDateEdit()
        self.sell_date_edit.setDate(QDate.currentDate())
        self.sell_date_edit.setCalendarPopup(True)

        btn_layout = QHBoxLayout()
        self.btn_week = QPushButton("近一周")
        self.btn_month = QPushButton("近一月")
        self.btn_year = QPushButton("近一年")
        self.btn_week.setEnabled(False)
        self.btn_month.setEnabled(False)
        self.btn_year.setEnabled(False)

        self.btn_week.clicked.connect(lambda: self.set_date_range(7))
        self.btn_month.clicked.connect(lambda: self.set_date_range(30))
        self.btn_year.clicked.connect(lambda: self.set_date_range(365))

        btn_layout.addWidget(self.btn_week)
        btn_layout.addWidget(self.btn_month)
        btn_layout.addWidget(self.btn_year)
        btn_layout.addStretch()

        self.calc_btn = QPushButton("计算收益率")
        self.calc_btn.setEnabled(False)
        self.calc_btn.clicked.connect(self.calculate_return)
        self.result_label = QLabel("收益率: --")
        self.result_label.setFont(QFont("Arial", 12, QFont.Bold))
        self.result_label.setStyleSheet("color: gray;")

        bottom_layout.addRow("买入日期:", self.buy_date_edit)
        bottom_layout.addRow("卖出日期:", self.sell_date_edit)
        bottom_layout.addRow(btn_layout)
        bottom_layout.addRow("操作:", self.calc_btn)
        bottom_layout.addRow("结果:", self.result_label)

        main_layout.addWidget(top_group)
        main_layout.addWidget(self.graph_widget, 1)
        main_layout.addWidget(bottom_group)

    def start_loading(self):
        """启动后台加载线程"""
        self.load_thread = LoadStockThread()
        self.load_thread.finished.connect(self.on_load_finished)
        self.load_thread.error.connect(self.on_load_error)
        self.load_thread.start()

    def on_load_finished(self, df):
        """加载成功回调"""
        try:
            df['display'] = df['代码'] + ' - ' + df['名称']
            self.code_combo.clear()
            self.code_combo.addItems(df['display'].tolist())

            # 启用控件
            self.code_combo.setEnabled(True)
            self.search_btn.setEnabled(True)
            self.status_bar.setText(f"状态: 加载成功，共 {len(df)} 只股票")
            self.status_bar.setStyleSheet("color: green;")
            print("股票列表加载完成")
        except Exception as e:
            self.on_load_error(f"处理数据失败: {e}")

    def on_load_error(self, msg):
        """加载失败回调"""
        self.status_bar.setText(f"状态: {msg}")
        self.status_bar.setStyleSheet("color: red;")
        QMessageBox.critical(self, "加载失败",
                             f"无法获取股票列表:\n{msg}\n\n建议:\n1. 检查网络\n2. 运行 'pip install --upgrade akshare'\n3. 稍后重试")

    def on_code_changed(self, text):
        if " - " in text:
            code, name = text.split(" - ", 1)
            self.name_label.setText(name)
        else:
            self.name_label.setText("请输入完整代码或从列表选择")

    def fetch_stock_details(self):
        code_text = self.code_combo.currentText()
        if " - " not in code_text:
            QMessageBox.warning(self, "提示", "请选择或输入有效的股票代码")
            return

        code = code_text.split(" - ")[0]

        self.status_bar.setText(f"状态: 正在获取 {code} 的历史数据...")
        self.search_btn.setEnabled(False)

        try:
            # 获取数据
            df = ak.stock_zh_a_hist(symbol=code, period="daily", adjust="qfq")

            if df is None or df.empty:
                raise Exception("未找到历史数据")

            self.stock_data = df
            self.stock_info = {'code': code, 'name': code_text.split(" - ")[1]}

            df['日期'] = pd.to_datetime(df['日期'])
            df = df.sort_values('日期')

            self.plot_chart(df)
            self.status_bar.setText(f"状态: {code} 数据加载成功")

            # 设置日期范围
            if not df.empty:
                max_date = df['日期'].max().to_pydatetime()
                min_date = df['日期'].min().to_pydatetime()
                self.sell_date_edit.setDate(QDate.fromPyDate(max_date))
                self.buy_date_edit.setDate(QDate.fromPyDate(min_date))

                # 启用计算功能
                self.btn_week.setEnabled(True)
                self.btn_month.setEnabled(True)
                self.btn_year.setEnabled(True)
                self.calc_btn.setEnabled(True)

        except Exception as e:
            self.status_bar.setText(f"状态: 获取数据失败 - {str(e)}")
            QMessageBox.critical(self, "错误", f"获取历史数据失败:\n{str(e)}")
        finally:
            self.search_btn.setEnabled(True)

    def plot_chart(self, df):
        self.graph_widget.clear()
        dates = df['日期'].tolist()
        close_prices = df['收盘'].tolist()

        df['MA5'] = df['收盘'].rolling(window=5).mean()
        df['MA20'] = df['收盘'].rolling(window=20).mean()

        ma5_prices = df['MA5'].tolist()
        ma20_prices = df['MA20'].tolist()

        curve = self.graph_widget.plot(dates, close_prices, pen=pg.mkPen('k', width=2), name='收盘价')
        self.ma5_line.setData(dates, ma5_prices)
        self.ma20_line.setData(dates, ma20_prices)

        legend = self.graph_widget.addLegend()
        legend.addItem(curve, "收盘价")
        legend.addItem(self.ma5_line, "MA5")
        legend.addItem(self.ma20_line, "MA20")

    def set_date_range(self, days):
        if self.stock_data is None:
            return
        sell_date = self.stock_data['日期'].max()
        buy_date = sell_date - timedelta(days=days)
        if buy_date < self.stock_data['日期'].min():
            buy_date = self.stock_data['日期'].min()

        self.sell_date_edit.setDate(QDate.fromPyDate(sell_date.to_pydatetime()))
        self.buy_date_edit.setDate(QDate.fromPyDate(buy_date.to_pydatetime()))
        self.calculate_return()

    def calculate_return(self):
        if self.stock_data is None:
            return

        buy_qdate = self.buy_date_edit.date()
        sell_qdate = self.sell_date_edit.date()
        buy_date = buy_qdate.toPyDate()
        sell_date = sell_qdate.toPyDate()

        if buy_date >= sell_date:
            QMessageBox.warning(self, "提示", "买入日期必须早于卖出日期")
            return

        mask = (self.stock_data['日期'].dt.date >= buy_date) & (self.stock_data['日期'].dt.date <= sell_date)
        subset = self.stock_data[mask]

        if subset.empty:
            QMessageBox.warning(self, "提示", "选定日期范围内无交易数据")
            return

        buy_price = subset.iloc[0]['收盘']
        sell_price = subset.iloc[-1]['收盘']

        if buy_price == 0:
            return_val = 0
        else:
            return_val = ((sell_price - buy_price) / buy_price) * 100

        color = "green" if return_val >= 0 else "red"
        self.result_label.setText(f"收益率: {return_val:.2f}%")
        self.result_label.setStyleSheet(f"color: {color}; font-size: 14px;")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = StockApp()
    window.show()
    sys.exit(app.exec_())