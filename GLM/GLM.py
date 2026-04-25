import os

os.environ['NO_PROXY'] = '*'

import re
import akshare as ak
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.backends.backend_qt5agg import FigureCanvasQTAgg as FigureCanvas
from matplotlib.backends.backend_qt5agg import NavigationToolbar2QT as NavigationToolbar
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout,
                             QHBoxLayout, QComboBox, QPushButton, QTableWidget,
                             QTableWidgetItem, QLabel, QGroupBox, QHeaderView,
                             QMessageBox, QDateEdit, QGridLayout, QRadioButton, QButtonGroup)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QDate, QSortFilterProxyModel, QStringListModel
import datetime
import time

plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False


def contains_chinese(s):
    return bool(re.search(r'[\u4e00-\u9fa5]', str(s)))


class CustomFilterProxyModel(QSortFilterProxyModel):
    def filterAcceptsRow(self, sourceRow, sourceParent):
        pattern = self.filterRegExp().pattern()
        if not pattern:
            return True
        source_model = self.sourceModel()
        index = source_model.index(sourceRow, 0)
        text = source_model.data(index, Qt.DisplayRole)
        if pattern.lower() in str(text).lower():
            return True
        return False


class LoadAssetThread(QThread):
    finished = pyqtSignal(dict)

    def __init__(self, asset_type):
        super().__init__()
        self.asset_type = asset_type

    def run(self):
        asset_dict = {}
        try:
            if self.asset_type == 'fund':
                df = ak.fund_name_em()
            else:
                df = ak.stock_info_a_code_name()
            for _, row in df.iterrows():
                code = str(row.iloc[0]).strip()
                name = ""
                if self.asset_type == 'fund':
                    for i in range(1, len(row)):
                        val = str(row.iloc[i]).strip()
                        if contains_chinese(val):
                            name = val
                            break
                    if not name:
                        name = str(row.iloc[1]).strip()
                else:
                    name = str(row.iloc[1]).strip()
                if code and name and code != 'nan' and name != 'nan':
                    asset_dict[code] = name
        except Exception as e:
            print(f"加载列表失败: {e}")
        self.finished.emit(asset_dict)


class AssetChartCanvas(FigureCanvas):
    def __init__(self, parent=None):
        self.fig, self.ax = plt.subplots(figsize=(8, 5))
        super().__init__(self.fig)
        self.setParent(parent)

    def plot_asset(self, df, code, name):
        self.ax.clear()
        df['日期'] = pd.to_datetime(df['日期'])
        df = df.sort_values('日期')
        self.ax.plot(df['日期'], df['收盘价'], label=f'{name} ({code})', color='blue', linewidth=2)
        self.ax.set_title(f'{name} 走势', fontsize=14, pad=15)
        self.ax.set_xlabel('日期', fontsize=11)
        self.ax.set_ylabel('价格 (前复权)' if len(code) == 6 else '单位净值', fontsize=11)
        self.ax.grid(True, linestyle='--', alpha=0.7)
        self.ax.legend(fontsize=10)
        self.fig.autofmt_xdate()
        self.draw()


class AssetToolApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("股基查询与分析小工具")
        self.resize(1200, 700)
        self.current_type = 'fund'
        self.current_price_df = pd.DataFrame()
        self.init_ui()
        self.load_assets()

    def init_ui(self):
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        main_layout = QVBoxLayout(main_widget)
        main_layout.setSpacing(10)

        top_layout = QHBoxLayout()
        self.radio_fund = QRadioButton("基金")
        self.radio_stock = QRadioButton("股票")
        self.radio_fund.setChecked(True)
        self.radio_group = QButtonGroup()
        self.radio_group.addButton(self.radio_fund, 1)
        self.radio_group.addButton(self.radio_stock, 2)
        self.radio_group.buttonClicked[int].connect(self.on_type_changed)

        self.asset_combo = QComboBox()
        self.asset_combo.setEnabled(False)
        self.asset_combo.addItem("正在加载...")
        self.asset_combo.setEditable(True)
        self.asset_combo.setInsertPolicy(QComboBox.NoInsert)

        self.proxy_model = CustomFilterProxyModel(self)
        self.asset_combo.setModel(self.proxy_model)
        self.asset_combo.lineEdit().textEdited.connect(self.on_combo_text_changed)

        self.query_btn = QPushButton("查询")
        self.query_btn.setFixedWidth(80)
        self.query_btn.clicked.connect(self.query_asset_info)
        self.query_btn.setEnabled(False)

        top_layout.addWidget(self.radio_fund)
        top_layout.addWidget(self.radio_stock)
        top_layout.addWidget(self.asset_combo)
        top_layout.addWidget(self.query_btn)
        main_layout.addLayout(top_layout)

        bottom_layout = QHBoxLayout()
        left_panel = QWidget()
        left_panel.setFixedWidth(400)
        left_layout = QVBoxLayout(left_panel)
        left_layout.setContentsMargins(0, 0, 0, 0)

        info_group = QGroupBox("基础信息")
        info_layout = QVBoxLayout()
        self.info_table = QTableWidget()
        self.info_table.setColumnCount(2)
        self.info_table.setHorizontalHeaderLabels(["项目", "内容"])
        self.info_table.horizontalHeader().setSectionResizeMode(QHeaderView.Stretch)
        self.info_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.info_table.setFixedHeight(150)
        info_layout.addWidget(self.info_table)
        info_group.setLayout(info_layout)
        left_layout.addWidget(info_group)

        auto_ret_group = QGroupBox("区间收益率 (自动计算)")
        auto_ret_layout = QGridLayout()
        self.label_week = QLabel("--")
        self.label_month = QLabel("--")
        self.label_3month = QLabel("--")
        for label in [self.label_week, self.label_month, self.label_3month]:
            label.setAlignment(Qt.AlignCenter)
            label.setStyleSheet("font-size: 16px; font-weight: bold;")
        auto_ret_layout.addWidget(QLabel("近一周:"), 0, 0)
        auto_ret_layout.addWidget(self.label_week, 0, 1)
        auto_ret_layout.addWidget(QLabel("近一月:"), 0, 2)
        auto_ret_layout.addWidget(self.label_month, 0, 3)
        auto_ret_layout.addWidget(QLabel("近三月:"), 0, 4)
        auto_ret_layout.addWidget(self.label_3month, 0, 5)
        auto_ret_group.setLayout(auto_ret_layout)
        left_layout.addWidget(auto_ret_group)

        custom_ret_group = QGroupBox("自定义买卖收益")
        custom_ret_layout = QVBoxLayout()
        date_layout = QHBoxLayout()
        self.date_start = QDateEdit()
        self.date_end = QDateEdit()
        self.date_end.setDate(QDate.currentDate())
        self.date_start.setDate(QDate.currentDate().addMonths(-1))
        self.date_start.setCalendarPopup(True)
        self.date_end.setCalendarPopup(True)
        self.date_start.setDisplayFormat("yyyy-MM-dd")
        self.date_end.setDisplayFormat("yyyy-MM-dd")
        self.btn_calc_custom = QPushButton("计算")
        self.btn_calc_custom.clicked.connect(self.calc_custom_return)
        date_layout.addWidget(QLabel("买入:"))
        date_layout.addWidget(self.date_start)
        date_layout.addWidget(QLabel("卖出:"))
        date_layout.addWidget(self.date_end)
        date_layout.addWidget(self.btn_calc_custom)
        custom_ret_layout.addLayout(date_layout)
        self.label_custom_ret = QLabel("--")
        self.label_custom_ret.setStyleSheet("font-size: 18px; font-weight: bold; color: black;")
        self.label_custom_ret.setAlignment(Qt.AlignCenter)
        custom_ret_layout.addWidget(self.label_custom_ret)
        custom_ret_group.setLayout(custom_ret_layout)
        left_layout.addWidget(custom_ret_group)

        bottom_layout.addWidget(left_panel)

        right_panel = QWidget()
        right_layout = QVBoxLayout(right_panel)
        right_layout.setContentsMargins(0, 0, 0, 0)
        self.chart_canvas = AssetChartCanvas(right_panel)
        self.toolbar = NavigationToolbar(self.chart_canvas, right_panel)
        right_layout.addWidget(self.toolbar)
        right_layout.addWidget(self.chart_canvas)
        bottom_layout.addWidget(right_panel, 1)
        main_layout.addLayout(bottom_layout)

    # ---------------------- 核心：修复搜索框逻辑 ----------------------
    def on_combo_text_changed(self, text):
        # 关键修复：记录当前光标位置（比如你输到第3个字，光标在3后面）
        cursor_pos = self.asset_combo.lineEdit().cursorPosition()

        # 执行过滤
        self.proxy_model.setFilterFixedString(text)

        # 关键修复：强制把输入框的内容恢复成你正在打的字，并把光标放回原位
        # 这样就阻止了下拉框自动把第一项替换掉你正在输入的文字
        self.asset_combo.lineEdit().setText(text)
        self.asset_combo.lineEdit().setCursorPosition(cursor_pos)

        # 弹出下拉框展示结果
        self.asset_combo.showPopup()

    def find_col_index(self, df, keywords):
        for i, col in enumerate(df.columns):
            for kw in keywords:
                if kw in str(col):
                    return i
        return None

    def on_type_changed(self, btn_id):
        new_type = 'fund' if btn_id == 1 else 'stock'
        if new_type != self.current_type:
            self.current_type = new_type
            self.reset_ui()
            self.load_assets()

    def load_assets(self):
        self.asset_combo.setEnabled(False)
        self.query_btn.setEnabled(False)
        self.asset_combo.lineEdit().setText("正在加载列表...")
        self.thread = LoadAssetThread(self.current_type)
        self.thread.finished.connect(self.on_assets_loaded)
        self.thread.start()

    def on_assets_loaded(self, asset_dict):
        items = []
        if not asset_dict:
            items.append("加载失败，请重试")
        else:
            for code, name in asset_dict.items():
                items.append(f"{code} - {name}")
        string_list_model = QStringListModel(items)
        self.proxy_model.setSourceModel(string_list_model)
        self.asset_combo.lineEdit().setText("")
        self.asset_combo.setEnabled(True)
        self.query_btn.setEnabled(True)

    def query_asset_info(self):
        current_text = self.asset_combo.currentText()
        if " - " in current_text:
            code = current_text.split(" - ")[0].strip()
        else:
            code = current_text.strip()

        if not code or code == "正在加载列表..." or code == "加载失败，请重试":
            QMessageBox.warning(self, "提示", "请从下拉列表中选择有效标的")
            return

        self.query_btn.setText("请求中...")
        self.query_btn.setEnabled(False)
        QApplication.processEvents()

        try:
            time.sleep(1)
            self.current_price_df = self.get_standard_price_data(code)
            if self.current_price_df.empty:
                raise Exception("未返回任何数据！可能是反爬限流(等10秒重试) 或 次新股无数据")
            time.sleep(0.5)
            info_dict = self.get_basic_info(code)
            self.update_table_with_dict(info_dict)
            asset_name = current_text.split(" - ")[1] if " - " in current_text else code
            self.chart_canvas.plot_asset(self.current_price_df, code, asset_name)
            self.calc_auto_returns()
        except Exception as e:
            QMessageBox.critical(self, "查询错误", f"错误详情：\n{str(e)}")
            self.current_price_df = pd.DataFrame()
        finally:
            self.query_btn.setText("查询")
            self.query_btn.setEnabled(True)

    def get_standard_price_data(self, code):
        code = str(code).strip()
        if self.current_type == 'fund':
            raw_df = ak.fund_open_fund_info_em(symbol=code, indicator="单位净值走势")
            if raw_df.empty:
                raise Exception("未获取到基金净值数据。")
            new_df = pd.DataFrame({'日期': raw_df.iloc[:, 0], '收盘价': raw_df.iloc[:, 1]})
            return new_df
        else:
            time.sleep(2)
            raw_df = pd.DataFrame()
            for attempt in range(2):
                try:
                    raw_df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date="20200101", adjust="hfq")
                    break
                except Exception as e:
                    if attempt == 0:
                        print(f"股票数据获取被中断，等待重试... ({e})")
                        time.sleep(2)
                    else:
                        raise Exception(f"股票数据获取失败。建议关闭VPN后重试。错误: {e}")
            if raw_df.empty:
                raise Exception("未获取到股票K线数据。")
            date_idx = self.find_col_index(raw_df, ['日期'])
            close_idx = self.find_col_index(raw_df, ['收盘', '收盘价'])
            if date_idx is None or close_idx is None:
                raise Exception(f"股票列名解析失败。当前列名:{raw_df.columns.tolist()}")
            new_df = pd.DataFrame({'日期': raw_df.iloc[:, date_idx], '收盘价': raw_df.iloc[:, close_idx]})
            return new_df

    def get_basic_info(self, code):
        info_dict = {}
        code = str(code).strip()
        try:
            if self.current_type == 'fund':
                df = ak.fund_individual_basic_info_xq(symbol=code)
                if df.shape[1] >= 2:
                    for _, row in df.iterrows():
                        info_dict[str(row.iloc[0])] = str(row.iloc[1])
                try:
                    time.sleep(0.5)
                    ach_df = ak.fund_individual_achievement_xq(symbol=code)
                    if ach_df.shape[1] >= 2:
                        for _, row in ach_df.iterrows():
                            info_dict[str(row.iloc[0])] = str(row.iloc[1])
                except:
                    pass
            else:
                time.sleep(0.5)
                df = ak.stock_individual_info_em(symbol=code)
                if 'item' in df.columns and 'value' in df.columns:
                    for _, row in df.iterrows():
                        info_dict[str(row['item'])] = str(row['value'])
                elif df.shape[1] >= 2:
                    for _, row in df.iterrows():
                        info_dict[str(row.iloc[0])] = str(row.iloc[1])
        except Exception as e:
            info_dict["提示"] = "信息获取失败"
        return info_dict

    def update_table_with_dict(self, info_dict):
        self.info_table.setRowCount(0)
        for key, value in info_dict.items():
            row_count = self.info_table.rowCount()
            self.info_table.insertRow(row_count)
            self.info_table.setItem(row_count, 0, QTableWidgetItem(str(key)))
            self.info_table.setItem(row_count, 1, QTableWidgetItem(str(value)))

    def find_price_by_date_offset(self, days_offset):
        if self.current_price_df.empty: return None, None
        df = self.current_price_df.copy()
        df['日期'] = pd.to_datetime(df['日期']).dt.date
        df = df.sort_values('日期')
        today = datetime.date.today()
        end_data = df[df['日期'] <= today]
        if end_data.empty: return None, None
        end_val = end_data.iloc[-1]['收盘价']
        end_date = end_data.iloc[-1]['日期']
        target_date = today - datetime.timedelta(days=days_offset + 10)
        start_data = df[(df['日期'] >= target_date) & (df['日期'] < end_date)]
        if start_data.empty: return None, None
        start_val = start_data.iloc[0]['收盘价']
        return start_val, end_val

    def calc_auto_returns(self):
        results = []
        for days in [7, 30, 90]:
            start_val, end_val = self.find_price_by_date_offset(days)
            if start_val and end_val and start_val != 0:
                ret = (end_val - start_val) / start_val * 100
                results.append(ret)
            else:
                results.append(None)
        self.update_label_style(self.label_week, results[0])
        self.update_label_style(self.label_month, results[1])
        self.update_label_style(self.label_3month, results[2])

    def calc_custom_return(self):
        if self.current_price_df.empty:
            self.label_custom_ret.setText("请先查询标的")
            return
        start_dt = self.date_start.date().toPyDate()
        end_dt = self.date_end.date().toPyDate()
        if start_dt >= end_dt:
            QMessageBox.warning(self, "错误", "卖出日期必须晚于买入日期")
            return
        df = self.current_price_df.copy()
        df['日期'] = pd.to_datetime(df['日期']).dt.date
        df = df.sort_values('日期')
        buy_data = df[df['日期'] >= start_dt]
        if buy_data.empty:
            self.label_custom_ret.setText("买入日无数据")
            self.label_custom_ret.setStyleSheet("font-size: 18px; font-weight: bold; color: gray;")
            return
        buy_price = buy_data.iloc[0]['收盘价']
        sell_data = df[df['日期'] <= end_dt]
        if sell_data.empty:
            self.label_custom_ret.setText("卖出日无数据")
            self.label_custom_ret.setStyleSheet("font-size: 18px; font-weight: bold; color: gray;")
            return
        sell_price = sell_data.iloc[-1]['收盘价']
        if buy_price == 0: return
        ret = (sell_price - buy_price) / buy_price * 100
        sign = "+" if ret >= 0 else ""
        self.label_custom_ret.setText(f"区间收益率: {sign}{ret:.2f}%")
        color = "red" if ret >= 0 else "green"
        self.label_custom_ret.setStyleSheet(f"font-size: 18px; font-weight: bold; color: {color};")

    def update_label_style(self, label, ret_value):
        if ret_value is None:
            label.setText("数据不足")
            label.setStyleSheet("font-size: 16px; font-weight: bold; color: gray;")
        else:
            sign = "+" if ret_value >= 0 else ""
            label.setText(f"{sign}{ret_value:.2f}%")
            color = "red" if ret_value >= 0 else "green"
            label.setStyleSheet(f"font-size: 16px; font-weight: bold; color: {color};")

    def reset_ui(self):
        self.info_table.setRowCount(0)
        self.chart_canvas.ax.clear()
        self.chart_canvas.draw()
        for label in [self.label_week, self.label_month, self.label_3month, self.label_custom_ret]:
            label.setText("--")
            label.setStyleSheet("font-size: 16px; font-weight: bold; color: black;")


if __name__ == "__main__":
    app = QApplication([])
    font = app.font()
    font.setFamily("Microsoft YaHei")
    app.setFont(font)
    window = AssetToolApp()
    window.show()
    app.exec_()