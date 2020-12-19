import pymysql
import pandas as pd
import config.config as cf
from datetime import datetime

class backtester():
    def __init__(self):
        """생성자"""
        self.conn = pymysql.connect(
            host=cf.db_ip,
            port=int(cf.db_port),
            user=cf.db_id,
            password=cf.db_pw,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        self.cur = self.conn.cursor()
        self.now = datetime.now().strftime('%Y-%m-%d %H:%M')
        self.today = datetime.today().strftime('%Y-%m-%d')
        # 데이터프레임 조작시 SettingWithCopyWarning 에러 안 뜨게 처리
        pd.options.mode.chained_assignment = None
        # DB초기화
        self.initialize_db()

    def initialize_db(self):
        """DB초기화"""
        # backtest_book 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'backtest_book'"
        if self.cur.execute(sql):
            print(f"[{self.now}] backtest_book 스키마 존재")
        else:
            sql = "CREATE DATABASE backtest_book"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] backtest_book 스키마 생성")

        # backtest_portfolio 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'backtest_portfolio'"
        if self.cur.execute(sql):
            print(f"[{self.now}] backtest_portfolio 스키마 존재")
        else:
            sql = "CREATE DATABASE backtest_portfolio"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] backtest_portfolio 스키마 생성")

        # backtest_result 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'backtest_result'"
        if self.cur.execute(sql):
            print(f"[{self.now}] backtest_result 스키마 존재")
        else:
            sql = "CREATE DATABASE backtest_result"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] backtest_result 스키마 생성")

    def create_book_table(self, strategy):
        pass

    def create_portfolio_table(self, strategy):
        pass

    def get_universe(self):
        pass

    def backtest_book(self, strategy):
        pass

    def backtest_portfolio(self, strategy):
        pass

    def backtest_result(self, strategy):
        pass

if __name__=="__main__":
    backtester = backtester()
    backtester.backtest_book()
    backtester.backtest_portfolio()
    backtester.backtest_result()
