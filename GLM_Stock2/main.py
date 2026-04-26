"""
股票数据查看器 - 主入口
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db, get_db_stats


def print_banner():
    print()
    print("=" * 56)
    print("       股票数据查看器 v1.0")
    print("       Stock Viewer - K-Line / Indicators / Returns")
    print("=" * 56)
    print()


def show_stats():
    init_db()
    stats = get_db_stats()
    print(f"\n  数据库统计:")
    print(f"    股票总数:       {stats['stock_count']:,}")
    if stats.get('stocks_with_detail'):
        print(f"    已获取详情:     {stats['stocks_with_detail']:,}")
    if stats.get('stocks_with_daily'):
        print(f"    日线记录:       {stats['stocks_with_daily']:,} 只 / {stats['daily_records']:,} 条")
    print(f"    数据库大小:     {stats['db_size_mb']} MB")
    print()


def main():
    print_banner()
    init_db()

    while True:
        print("  操作菜单:")
        print("    1. 启动图形界面")
        print("    2. 下载股票列表（代码+名称）")
        print("    3. 增量下载所有股票日线行情")
        print("    4. 批量获取股票详细信息（行业、市值等）")
        print("    5. 下载单只股票数据")
        print("    6. 查看数据库统计")
        print("    0. 退出")
        print()

        choice = input("  请选择 (0-6): ").strip()

        if choice == "1":
            print("\n  正在启动图形界面...")
            try:
                from viewer import run
                run()
            except Exception as e:
                print(f"\n  启动失败: {e}")
            break

        elif choice == "2":
            from fetcher import fetch_and_save_stock_list
            fetch_and_save_stock_list()

        elif choice == "3":
            w = input("  并发数(默认4): ").strip()
            from fetcher import fetch_all_stock_daily
            import signal
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            fetch_all_stock_daily(max_workers=int(w) if w.isdigit() and int(w) > 0 else 4)

        elif choice == "4":
            w = input("  并发数(默认4): ").strip()
            from fetcher import fetch_all_stock_details
            import signal
            signal.signal(signal.SIGINT, signal.SIG_DFL)
            fetch_all_stock_details(max_workers=int(w) if w.isdigit() and int(w) > 0 else 4)

        elif choice == "5":
            code = input("  输入股票代码: ").strip()
            if code:
                from fetcher import fetch_single_stock_all
                fetch_single_stock_all(code)

        elif choice == "6":
            show_stats()

        elif choice == "0":
            print("\n  再见！\n")
            break

        else:
            print("  无效选择")

    from database import close_connection
    close_connection()


if __name__ == "__main__":
    main()
