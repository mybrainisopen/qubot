import pymysql
import pandas as pd
import numpy as np
import math
import datetime
import common.config as cf
import common.db_sql as dbl
from matplotlib import pyplot as plt
from common import logger as logger

class TestInvestment():
    def __init__(self):
        '''생성자'''
        self.logger = logger.logger
        self.conn = pymysql.connect(
            host=cf.db_ip,
            port=int(cf.db_port),
            user=cf.db_id,
            password=cf.db_pw,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        self.cur = self.conn.cursor()
        self.now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        self.today = datetime.datetime.today().strftime('%Y-%m-%d')
        # 데이터프레임 조작시 SettingWithCopyWarning 에러 안 뜨게 처리
        pd.options.mode.chained_assignment = None
        # DB초기화
        self.initialize_db()
        # 변수 설정
        self.tax_rate = cf.tax_rate
        self.fee_rate = cf.fee_rate
        self.slippage = cf.slippage

    def initialize_db(self):
        '''DB초기화'''
        # test_book 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'test_book'"
        if self.cur.execute(sql):
            self.logger.info("test_book 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE test_book"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("test_book 스키마 생성")

        # test_portfolio 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'test_portfolio'"
        if self.cur.execute(sql):
            self.logger.info("test_portfolio 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE test_portfolio"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("test_portfolio 스키마 생성")

    def create_book_table(self, strategy):
        '''종목별 모멘텀 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'test_book' and table_name = '{strategy}'"
        if self.cur.execute(sql):
            self.logger.info(f"test_book.{strategy} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS test_book.`{strategy}` (" \
                  f"date DATE," \
                  f"trading CHAR(10), " \
                  f"stock VARCHAR(50), " \
                  f"price BIGINT(20), " \
                  f"quantity BIGINT(20), " \
                  f"investment BIGINT(20), " \
                  f"fee BIGINT(20), " \
                  f"tax BIGINT(20), " \
                  f"slippage BIGINT(20))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"test_book.{strategy} 테이블 생성 완료")

    def create_portfolio_table(self, strategy):
        '''종목별 모멘텀 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'test_portfolio' and table_name = '{strategy}'"
        if self.cur.execute(sql):
            self.logger.info(f"test_portfolio.{strategy} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS test_portfolio.`{strategy}` (" \
                  f"date DATE," \
                  f"cash BIGINT(20), " \
                  f"investment BIGINT(20), " \
                  f"total BIGINT(20), " \
                  f"daily_return FLOAT, " \
                  f"total_return FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"test_portfolio.{strategy} 테이블 생성 완료")

    def backtest_book(self, strategy, initial, universe, start, end):
        '''전략에 맞는 거래장부 테이블 생성'''
        # # 거래장부 테이블이 있었다면 삭제
        # sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'test_book' and table_name = '{strategy}'"
        # if self.cur.execute(sql):
        #     sql = f"DROP TABLE test_book.`{strategy}`"
        #     self.cur.execute(sql)
        #     self.conn.commit()

        # 데이터프레임 생성
        book = pd.DataFrame(columns=['date', 'trading', 'stock', 'price', 'quantity', 'investment', 'fee', 'tax', 'slippage'])
        # 매수 장부
        for stock in universe:
            sql = f"SELECT close FROM daily_price.`{stock}` WHERE date={start}"
            self.cur.execute(sql)
            price = self.cur.fetchone()['close']
            slot = math.floor(initial/len(universe))
            trading = 'buy'
            quantity = math.floor(slot/price)
            investment = price * quantity
            fee = math.floor(investment * self.fee_rate)
            if investment + fee > slot:
                quantity -= 1
            investment = price * quantity
            fee = math.floor(investment * self.fee_rate)
            tax = 0
            slippage = 0
            book = book.append({'date': start, 'trading': trading, 'stock': stock, 'price': price, 'quantity': quantity,
                                'investment': investment, 'fee': fee, 'tax': tax, 'slippage': slippage}, ignore_index=True)
        # 매도 장부
        for stock in universe:
            sql = f"SELECT close FROM daily_price.`{stock}` WHERE date={end}"
            self.cur.execute(sql)
            price = self.cur.fetchone()['close']
            # slot = math.floor(initial/len(universe))
            trading = 'sell'
            quantity = int(book[(book['stock'] == stock) & (book['trading'] == 'buy')]['quantity'])
            investment = price * quantity
            fee = math.floor(investment * self.fee_rate)
            tax = math.floor(investment * self.tax_rate)
            slippage = 0
            # print(end, trading, stock, price, quantity, investment, fee, tax, slippage)
            book = book.append({'date': end, 'trading': trading, 'stock': stock, 'price': price, 'quantity': quantity,
                                'investment': investment, 'fee': fee, 'tax': tax, 'slippage': slippage}, ignore_index=True)
        # DB 저장
        # book.to_csv(f'C:\\Project\\qubot\\csv\\{strategy}_book.csv', encoding='euc-kr')
        self.create_book_table(strategy)
        for row in book.itertuples():
            sql = f"INSERT INTO test_book.`{strategy}` (date, trading, stock, price, quantity, investment, fee, tax, slippage) " \
                  f"VALUES ({row.date}, '{row.trading}', '{row.stock}', {row.price}, {row.quantity}, {row.investment}, {row.fee}, {row.tax}, {row.slippage})"
            self.cur.execute(sql)
            self.conn.commit()
        return book


if __name__=="__main__":
    test_investment = test_investment()

"""
21.04.06 매수
퍼스텍 : 2,487 / 90
금호석유 : 245,496 / 2
계룡건설 : 31,632 / 15
대림제지 : 13,614 / 35
삼보판지 : 14,907 / 32
삼천리자전거 : 13,213 / 35
KPX케미칼 : 65,365 / 7
우진플라임 : 3,974 / 120
STX엔진 : 8,327 / 55
인터지스 : 4,555 / 100

"""