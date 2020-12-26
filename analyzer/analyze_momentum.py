import pymysql
import pandas as pd
import config.config as cf
import datetime
from datetime import timedelta
from dateutil.relativedelta import relativedelta

class analyze_momentum():
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
        self.now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        self.today = datetime.datetime.today().strftime('%Y%m%d')
        # 데이터프레임 조작시 SettingWithCopyWarning 에러 안 뜨게 처리
        pd.options.mode.chained_assignment = None
        # DB초기화
        self.initialize_db()

    def initialize_db(self):
        '''DB초기화'''
        # momentum 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'momentum'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] momentum 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE momentum"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] momentum 스키마 생성")

    def create_table(self, stock):
        '''종목별 모멘텀 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'momentum' and table_name = '{stock}'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] momentum.{stock} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS momentum.`{stock}` (" \
                  f"date DATE," \
                  f"1MRM FLOAT, " \
                  f"3MRM FLOAT, " \
                  f"6MRM FLOAT, " \
                  f"12MRM FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] momentum.{stock} 테이블 생성 완료")

    def analyze_momentum_by_date_stock(self, start_date, end_date, stock):
        ''' 1MRM = 1개월 모멘텀 (21거래일)
            3MRM = 3개월 모멘텀 (63거래일)
            6MRM = 6개월 모멘텀 (126거래일)
            12MRM = 1년 모멘텀 (252거래일)'''
        # 밸류에이션 테이블 생성
        self.create_table(stock=stock)

        # 필요한 날짜 계산
        start_date = datetime.datetime.strptime(start_date, '%Y%m%d')  # date type
        year_before = datetime.datetime.strftime(start_date - relativedelta(months=12) - timedelta(days=30), '%Y%m%d')
        start_date = datetime.datetime.strftime(start_date, '%Y%m%d')

        # 주가 리스트 가져오기
        sql = f"SELECT date, close FROM daily_price.`{stock}` WHERE date BETWEEN {year_before} AND {end_date}"
        self.cur.execute(sql)
        price_list = self.cur.fetchall()
        price_list = pd.DataFrame(price_list)

        # 모멘텀 계산하기
        start_date = datetime.datetime.strptime(start_date, '%Y%m%d').date()
        price_list['1MRM'] = price_list['close'].pct_change(periods=21)
        price_list['3MRM'] = price_list['close'].pct_change(periods=63)
        price_list['6MRM'] = price_list['close'].pct_change(periods=126)
        price_list['12MRM'] = price_list['close'].pct_change(periods=252)
        price_list = price_list[price_list['date'] >= start_date]
        price_list.dropna(inplace=True)
        price_list.reset_index(inplace=True, drop=True)

        # DB입력하기
        for idx in range(len(price_list)):
            date = price_list['date'][idx]
            MRM_1 = price_list['1MRM'][idx]
            MRM_3 = price_list['3MRM'][idx]
            MRM_6 = price_list['6MRM'][idx]
            MRM_12 = price_list['12MRM'][idx]
            sql = f"REPLACE INTO momentum.`{stock}` (date, 1MRM, 3MRM, 6MRM, 12MRM) " \
                  f"VALUES ('{date}','{MRM_1}','{MRM_3}','{MRM_6}','{MRM_12}')"
            self.cur.execute(sql)
            self.conn.commit()

    def analyze_momentum(self):
        '''전체 종목의 모멘텀 계산'''
        # 종목 리스트(stock_list) 가져오기
        sql = "SELECT a.stock, a.daily_price_scraped, b.momentum_analyzed " \
              "FROM status.scrap_stock_status a " \
              "INNER JOIN status.analyze_stock_status b ON a.stock=b.stock"
        self.cur.execute(sql)
        stock_list = self.cur.fetchall()
        stock_list = pd.DataFrame(stock_list)

        print(f"[{self.now}] (전종목) 모멘텀 계산 시작")
        for idx in range(len(stock_list)):
            stock = stock_list['stock'][idx]
            check_price = stock_list['daily_price_scraped'][idx]
            check_momentum = stock_list['momentum_analyzed'][idx]

            # 종목별, 스크랩 상태별 스크랩 실행
            if check_price is None:
                print(f"[{self.now}] ({idx+1}/{stock}) 일일주가 스크랩 아직 안됨")
                continue
            elif check_momentum is None:
                self.analyze_momentum_by_date_stock(start_date='20170101', end_date=self.today, stock=stock)
                sql = f"UPDATE status.analyze_stock_status SET momentum_analyzed='{self.today}' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                print(f"[{self.now}] ({idx+1}/{stock}) 모멘텀 계산 완료")
            elif check_momentum != self.today:
                start_date = datetime.datetime.strftime(check_momentum, '%Y%m%d')
                self.analyze_momentum_by_date_stock(start_date=start_date, end_date=self.today, stock=stock)
                sql = f"UPDATE status.analyze_stock_status SET momentum_analyzed='{self.today}' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                print(f"[{self.now}] ({idx+1}/{stock}) 모멘텀 계산 완료")
        print(f"[{self.now}] (전종목) 모멘텀 계산 완료")

if __name__=="__main__":
    analyze_momentum = analyze_momentum()
    analyze_momentum.analyze_momentum()