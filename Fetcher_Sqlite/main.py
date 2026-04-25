"""
基金数据查看器 - 主入口
提供命令行菜单和图形界面启动功能。
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db, get_db_stats


def print_banner():
    print()
    print("=" * 56)
    print("       基金数据查看器 v2.0")
    print("       Fund Viewer + K-Line + Technical Indicators")
    print("=" * 56)
    print()


def show_stats():
    """显示数据库统计信息。"""
    init_db()
    stats = get_db_stats()
    print(f"\n  数据库统计:")
    print(f"    基金总数:       {stats['fund_count']:,}")
    print(f"    已有净值基金:   {stats['funds_with_nav']:,}")
    print(f"    净值记录总数:   {stats['nav_records']:,}")
    print(f"    已有K线基金:   {stats['funds_with_ohlc']:,}")
    print(f"    K线记录总数:    {stats['ohlc_records']:,}")
    print(f"    数据库大小:     {stats['db_size_mb']} MB")
    print()


def main():
    print_banner()
    init_db()

    while True:
        print("  操作菜单:")
        print("    1. 启动图形界面查看器（K线/技术指标）")
        print("    2. 下载所有基金基本信息")
        print("    3. 增量更新所有基金历史净值")
        print("    4. 增量更新所有 ETF/LOF K线数据")
        print("    5. 一键增量更新（净值 + K线）")
        print("    6. 下载单只基金净值 / K线")
        print("    7. 查看数据库统计")
        print("    0. 退出")
        print()

        choice = input("  请选择 (0-7): ").strip()

        if choice == "1":
            print("\n  正在启动图形界面...")
            try:
                from viewer import run
                run()
            except Exception as e:
                print(f"\n  启动失败: {e}")
            break

        elif choice == "2":
            from fetcher import fetch_and_save_all_fund_basic
            fetch_and_save_all_fund_basic()

        elif choice == "3":
            w = input("  并发线程数 (默认 4): ").strip()
            from fetcher import fetch_all_fund_history
            fetch_all_fund_history(max_workers=int(w) if w.isdigit() and int(w) > 0 else 4)

        elif choice == "4":
            w = input("  并发线程数 (默认 4): ").strip()
            from fetcher import fetch_all_etf_lof_ohlc
            fetch_all_etf_lof_ohlc(max_workers=int(w) if w.isdigit() and int(w) > 0 else 4)

        elif choice == "5":
            w = input("  并发线程数 (默认 4): ").strip()
            from fetcher import update_all
            update_all(max_workers=int(w) if w.isdigit() and int(w) > 0 else 4)

        elif choice == "6":
            code = input("  输入基金代码: ").strip()
            if code:
                print("  1=净值  2=K线  3=全部")
                sub = input("  选择: ").strip()
                from fetcher import fetch_single_fund_history, fetch_single_fund_ohlc
                if sub == "2":
                    fetch_single_fund_ohlc(code)
                elif sub == "3":
                    fetch_single_fund_history(code)
                    fetch_single_fund_ohlc(code)
                else:
                    fetch_single_fund_history(code)

        elif choice == "7":
            show_stats()

        elif choice == "0":
            print("\n  再见！\n")
            break
        else:
            print("\n  无效选择\n")

    from database import close_connection
    close_connection()


if __name__ == "__main__":
    main()
