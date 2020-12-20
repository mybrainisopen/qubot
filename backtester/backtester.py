import pymysql
import pandas as pd
import config.config as cf
from datetime import datetime

class backtester():
    def __init__(self):
        '''생성자'''
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
        # 변수 설정
        self.tax = cf.tax_rate
        self.fee = cf.fee_rate
        self.slippage = cf.slippage

    def initialize_db(self):
        '''DB초기화'''
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

        # backtest_result 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'backtest_result' and table_name = 'backtest_result'"
        if self.cur.execute(sql):
            print(f"[{self.now}] backtest_result.backtest_result 테이블 존재")
        else:
            sql = """
                CREATE TABLE IF NOT EXISTS backtest_result.backtest_result (
                strategy CHAR(10), 
                start DATE, 
                end DATE, 
                initial BIGINT(20), 
                final BIGINT(20), 
                Return FLOAT, 
                Volatility FLOAT, 
                MDD FLOAT, 
                Sharpe FLOAT, 
                Remark VARCHAR(200), 
                PRIMARY KEY (strategy))
            """
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] backtest_result.backtest_result 테이블 생성")

    def create_book_table(self, strategy):
        '''종목별 모멘텀 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtest_book' and table_name = '{strategy}'"
        if self.cur.execute(sql):
            print(f"[{self.now}] backtest_book.{strategy} 테이블 존재함")
        else:
            sql = f"CREATE TABLE IF NOT EXISTS backtest_book.`{strategy}` (" \
                  f"date DATE," \
                  f"trading CHAR(10), " \
                  f"stock VARCHAR(50), " \
                  f"price BIGINT(20, " \
                  f"quantity BIGINT(20), " \
                  f"investment BIGINT(20), " \
                  f"fee BIGINT(20), " \
                  f"tax BIGINT(20), " \
                  f"slippage BIGINT(20))"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] backtest_book.{strategy} 테이블 생성 완료")

    def create_portfolio_table(self, strategy):
        '''종목별 모멘텀 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtest_portfolio' and table_name = '{strategy}'"
        if self.cur.execute(sql):
            print(f"[{self.now}] backtest_portfolio.{strategy} 테이블 존재함")
        else:
            sql = f"CREATE TABLE IF NOT EXISTS backtest_portfolio.`{strategy}` (" \
                  f"date DATE," \
                  f"cash BIGINT(20), " \
                  f"investment BIGINT(20), " \
                  f"total BIGINT(20), " \
                  f"daily_return FLOAT, " \
                  f"total_return FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] backtest_portfolio.{strategy} 테이블 생성 완료")

    def get_universe(self):
        pass

    def backtest_book(self, strategy, initial_cash):
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
