import sys
import akshare as ak
import pandas as pd
from PyQt5.QtWidgets import (QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
                             QLabel, QLineEdit, QPushButton, QDateEdit, QTextEdit, QComboBox)
from PyQt5.QtCore import QDate, Qt
from PyQt5.QtGui import QFont, QPalette, QColor


class FundReturnCalculator(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("基金收益率计算器")
        self.setGeometry(300, 300, 800, 600)

        # 设置主窗口样式
        self.setStyleSheet("""
            QMainWindow {
                background-color: #f0f5ff;
            }
            QLabel {
                font-size: 14px;
                font-weight: bold;
                color: #2c3e50;
            }
            QPushButton {
                background-color: #3498db;
                color: white;
                border-radius: 5px;
                padding: 8px 16px;
                font-size: 14px;
                font-weight: bold;
            }
            QPushButton:hover {
                background-color: #2980b9;
            }
            QTextEdit {
                background-color: white;
                border: 1px solid #bdc3c7;
                border-radius: 5px;
                font-size: 13px;
            }
            QLineEdit, QDateEdit {
                background-color: white;
                border: 1px solid #bdc3c7;
                border-radius: 3px;
                padding: 5px;
                font-size: 14px;
            }
        """)

        # 创建中央部件
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(15)
        main_layout.setContentsMargins(20, 20, 20, 20)

        # 标题
        title_label = QLabel("基金收益率计算器")
        title_label.setFont(QFont("Arial", 18, QFont.Bold))
        title_label.setAlignment(Qt.AlignCenter)
        title_label.setStyleSheet("color: #2c3e50; margin-bottom: 20px;")
        main_layout.addWidget(title_label)

        # 输入区域
        input_layout = QHBoxLayout()

        # 基金代码输入
        fund_code_layout = QVBoxLayout()
        fund_code_label = QLabel("基金代码:")
        self.fund_code_input = QLineEdit()
        self.fund_code_input.setPlaceholderText("例如: 000001")
        fund_code_layout.addWidget(fund_code_label)
        fund_code_layout.addWidget(self.fund_code_input)
        input_layout.addLayout(fund_code_layout)

        # 基金名称示例
        fund_examples_layout = QVBoxLayout()
        fund_examples_label = QLabel("热门基金示例:")
        self.fund_examples_combo = QComboBox()
        self.fund_examples_combo.addItems([
            "110011 - 易方达中小盘混合",
            "000001 - 华夏成长混合",
            "001632 - 天弘中证食品饮料",
            "161725 - 招商中证白酒",
            "005827 - 易方达蓝筹精选"
        ])
        self.fund_examples_combo.currentIndexChanged.connect(self.select_fund_example)
        fund_examples_layout.addWidget(fund_examples_label)
        fund_examples_layout.addWidget(self.fund_examples_combo)
        input_layout.addLayout(fund_examples_layout)

        main_layout.addLayout(input_layout)

        # 日期选择区域
        date_layout = QHBoxLayout()

        # 开始日期选择
        start_date_layout = QVBoxLayout()
        start_date_label = QLabel("开始日期:")
        self.start_date_edit = QDateEdit()
        self.start_date_edit.setDate(QDate.currentDate().addMonths(-6))
        self.start_date_edit.setCalendarPopup(True)
        start_date_layout.addWidget(start_date_label)
        start_date_layout.addWidget(self.start_date_edit)
        date_layout.addLayout(start_date_layout)

        # 结束日期选择
        end_date_layout = QVBoxLayout()
        end_date_label = QLabel("结束日期:")
        self.end_date_edit = QDateEdit()
        self.end_date_edit.setDate(QDate.currentDate())
        self.end_date_edit.setCalendarPopup(True)
        end_date_layout.addWidget(end_date_label)
        end_date_layout.addWidget(self.end_date_edit)
        date_layout.addLayout(end_date_layout)

        main_layout.addLayout(date_layout)

        # 按钮区域
        button_layout = QHBoxLayout()

        # 计算按钮
        self.calculate_button = QPushButton("计算收益率")
        self.calculate_button.clicked.connect(self.calculate_return)
        self.calculate_button.setMinimumHeight(40)
        button_layout.addWidget(self.calculate_button)

        # 重置按钮
        self.reset_button = QPushButton("重置")
        self.reset_button.clicked.connect(self.reset_fields)
        self.reset_button.setMinimumHeight(40)
        button_layout.addWidget(self.reset_button)

        main_layout.addLayout(button_layout)

        # 结果显示区域
        result_layout = QVBoxLayout()
        result_label = QLabel("计算结果:")
        self.result_text = QTextEdit()
        self.result_text.setReadOnly(True)
        self.result_text.setMinimumHeight(200)
        result_layout.addWidget(result_label)
        result_layout.addWidget(self.result_text)

        main_layout.addLayout(result_layout)

        # 净值数据展示区域
        data_layout = QVBoxLayout()
        data_label = QLabel("基金净值数据:")
        self.data_text = QTextEdit()
        self.data_text.setReadOnly(True)
        data_layout.addWidget(data_label)
        data_layout.addWidget(self.data_text)

        main_layout.addLayout(data_layout)

        # 状态栏
        self.status_bar = self.statusBar()
        self.status_bar.setStyleSheet("background-color: #e0e7ff; color: #2c3e50; font-size: 12px;")

        # 初始化示例基金代码
        self.select_fund_example(0)

    def select_fund_example(self, index):
        """当用户选择基金示例时自动填充基金代码"""
        fund_text = self.fund_examples_combo.currentText()
        fund_code = fund_text.split(" - ")[0].strip()
        self.fund_code_input.setText(fund_code)

    def calculate_return(self):
        """计算基金收益率"""
        fund_code = self.fund_code_input.text().strip()
        start_date = self.start_date_edit.date().toString("yyyy-MM-dd")
        end_date = self.end_date_edit.date().toString("yyyy-MM-dd")

        # 验证输入
        if not fund_code:
            self.show_result("错误：请输入基金代码！", "error")
            return

        try:
            # 获取基金数据
            self.status_bar.showMessage("正在获取基金数据...")
            QApplication.processEvents()  # 更新UI

            fund_data = ak.fund_open_fund_info_em(symbol=fund_code, indicator="单位净值走势")

            if fund_data.empty:
                self.show_result(f"错误：未找到基金代码 '{fund_code}' 的数据！", "error")
                return

            # 处理数据
            fund_data['净值日期'] = pd.to_datetime(fund_data['净值日期'])
            fund_data = fund_data.sort_values('净值日期')

            # 过滤日期范围
            filtered_data = fund_data[
                (fund_data['净值日期'] >= pd.to_datetime(start_date)) &
                (fund_data['净值日期'] <= pd.to_datetime(end_date))
                ]

            if filtered_data.empty:
                self.show_result(f"错误：在 {start_date} 到 {end_date} 期间没有可用的净值数据！", "error")
                return

            # 获取开始和结束净值
            start_value = filtered_data.iloc[0]['单位净值']
            end_value = filtered_data.iloc[-1]['单位净值']

            # 计算收益率
            return_rate = (end_value - start_value) / start_value * 100

            # 显示结果
            result_text = (
                f"基金代码: {fund_code}\n"
                f"开始日期: {start_date}  单位净值: {start_value:.4f}\n"
                f"结束日期: {end_date}  单位净值: {end_value:.4f}\n"
                f"持有期收益率: {return_rate:.2f}%\n"
                f"持有天数: {len(filtered_data)} 天"
            )

            # 显示详细数据
            data_text = filtered_data.to_string(index=False)

            self.show_result(result_text, "success")
            self.data_text.setText(data_text)
            self.status_bar.showMessage("计算完成！", 3000)

        except Exception as e:
            self.show_result(f"错误：{str(e)}", "error")
            self.status_bar.showMessage("发生错误！", 3000)

    def show_result(self, text, status="info"):
        """显示结果并根据状态设置颜色"""
        self.result_text.setText(text)

        palette = self.result_text.palette()
        if status == "success":
            palette.setColor(QPalette.Text, QColor("#27ae60"))  # 绿色
        elif status == "error":
            palette.setColor(QPalette.Text, QColor("#e74c3c"))  # 红色
        else:
            palette.setColor(QPalette.Text, QColor("#2c3e50"))  # 深蓝色

        self.result_text.setPalette(palette)

    def reset_fields(self):
        """重置所有输入字段"""
        self.fund_code_input.clear()
        self.start_date_edit.setDate(QDate.currentDate().addMonths(-6))
        self.end_date_edit.setDate(QDate.currentDate())
        self.result_text.clear()
        self.data_text.clear()
        self.status_bar.clearMessage()
        self.select_fund_example(0)  # 重置示例选择


if __name__ == "__main__":
    app = QApplication(sys.argv)

    # 设置应用样式
    app.setStyle("Fusion")

    # 创建主窗口
    calculator = FundReturnCalculator()
    calculator.show()

    sys.exit(app.exec_())