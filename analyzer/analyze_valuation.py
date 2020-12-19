import pymysql
import pandas as pd
import config.config as cf
import datetime

class analyze_valuation():
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
        # DB초기화
        self.initialize_db()

    def initialize_db(self):
        '''DB초기화'''
        # valuation 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'valuation'"
        if self.cur.execute(sql):
            print(f"[{self.now}] valuation 스키마 존재")
        else:
            sql = "CREATE DATABASE valuation"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] valuation 스키마 생성")

    def create_table(self, stock):
        '''종목별 밸류에이션 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'valuation' and table_name = '{stock}'"
        if self.cur.execute(sql):
            print(f"[{self.now}] valuation.{stock} 테이블 존재함")
        else:
            sql = f"CREATE TABLE IF NOT EXISTS valuation.`{stock}` (" \
                  f"date DATE," \
                  f"PER FLOAT, " \
                  f"PBR FLOAT, " \
                  f"PSR FLOAT, " \
                  f"PCR FLOAT, " \
                  f"PEG FLOAT, " \
                  f"EVEBIT FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] valuation.{stock} 테이블 생성 완료")

    def analyze_valuation_by_date_stock(self, start_date, end_date, stock):
        ''' PER = 주가/EPS
            PBR = 주가/BPS
            PSR = 주가/SPS
            PCR = 주가/CPS
            PEG = PER/EPS증가율 * 100
            EV/EBIT = (시가총액+순차입금)/영업이익TTM, 순차입금 = 이자발생부채 - 현금'''

        # 밸류에이션 테이블 생성
        self.create_table(stock=stock)

        # 주가 리스트 가져오기
        sql = f"SELECT date, close FROM daily_price.`{stock}` WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        self.cur.execute(sql)
        price_list = self.cur.fetchall()
        price_list = pd.DataFrame(price_list)

        # 상장주식수 가져오기
        sql = f"SELECT shares FROM stock_info.stock_info WHERE stock='{stock}'"
        self.cur.execute(sql)
        shares = int(self.cur.fetchone()['shares'])

        for idx in range(len(price_list)):
            date = price_list['date'][idx]
            price = price_list['close'][idx]
            sql = f"SELECT 현금, 이자발생부채, 영업이익TTM, EPS, BPS, SPS, CPS, EPS증가율 FROM fundamental.`{stock}` " \
                  f"WHERE date = (SELECT max(date) FROM fundamental.`동화약품` WHERE date <= '{date}')"
            self.cur.execute(sql)
            res = self.cur.fetchone()
            cash = int(res['현금'])
            debt = int(res['이자발생부채'])
            op = int(res['영업이익TTM'])
            EPS = int(res['EPS'])
            BPS = int(res['BPS'])
            SPS = int(res['SPS'])
            CPS = int(res['CPS'])
            EPSG = float(res['EPS증가율'])
            if EPS <= 0 :
                PER = 0
            else:
                PER = price/EPS
            if BPS <= 0:
                PBR = 0
            else:
                PBR = price/BPS
            if SPS <= 0:
                PSR = 0
            else:
                PSR = price/SPS
            if CPS <= 0:
                PCR = 0
            else:
                PCR = price/CPS
            if EPSG <= 0:
                PEG = 0
            else:
                PEG = PER/(EPSG*100)
            if op <= 0:
                EVEBIT = 0
            else:
                EVEBIT = (price*shares+debt-cash)/op
            sql = f"REPLACE INTO valuation.`{stock}` (date, PER, PBR, PSR, PCR, PEG, EVEBIT) " \
                  f"VALUES ('{date}','{PER}','{PBR}','{PSR}','{PCR}','{PEG}','{EVEBIT}')"
            self.cur.execute(sql)
            self.conn.commit()

    def analyze_valuation(self):
        '''전체 종목의 가치지표 계산'''
        # 종목 리스트(stock_list) 가져오기
        sql = "SELECT a.stock, a.daily_price_scraped, a.financial_statements_scraped, b.fundamental_analyzed, b.valuation_analyzed " \
              "FROM status.scrap_stock_status a " \
              "INNER JOIN status.analyze_stock_status b ON a.stock=b.stock"
        self.cur.execute(sql)
        stock_list = self.cur.fetchall()
        stock_list = pd.DataFrame(stock_list)

        print(f"[{self.now}] (전종목) 밸류에이션 계산 시작")
        for idx in range(len(stock_list)):
            stock = stock_list['stock'][idx]
            check_price = stock_list['daily_price_scraped'][idx]
            check_financial = stock_list['financial_statements_scraped'][idx]
            check_fundamental = stock_list['fundamental_analyzed'][idx]
            check_valuation = stock_list['valuation_analyzed'][idx]

            # 종목별, 스크랩 상태별 스크랩 실행
            if check_price is None:
                print(f"[{self.now}] ({idx+1}/{stock}) 일일주가 스크랩 아직 안됨")
                continue
            elif check_financial is None:
                print(f"[{self.now}] ({idx+1}/{stock}) 재무제표 스크랩 아직 안됨")
                continue
            elif check_fundamental is None:
                print(f"[{self.now}] ({idx+1}/{stock}) 펀더멘털 계산 아직 안됨")
                continue
            elif check_fundamental == datetime.date(9000, 1, 1):  # 펀더멘털 계산 비대상 종목(90000101)은 밸류에이션 계산 비대상 종목(90000101)으로 처리
                sql = f"UPDATE status.analyze_stock_status SET valuation_analyzed='90000101' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                print(f"[{self.now}] ({idx+1}/{stock}) 밸류에이션 계산 비대상 종목")
                continue
            elif (check_valuation is None) & (check_fundamental == datetime.date(1000, 1, 1)):
                # 아직 밸류에이션 계산이 안됐고, 펀더멘털 계산 에러가 났던 종목(10000101)은 밸류에이션 계산하되 밸류에이션 계산 에러 종목(10000101)로 처리
                try:
                    self.analyze_valuation_by_date_stock(start_date='20170101', end_date=self.today, stock=stock)
                except Exception as e:
                    print(f"[{self.now}] ({idx+1}/{stock}) 밸류에이션 계산 미완료:", str(e))
                sql = f"UPDATE status.analyze_stock_status SET valuation_analyzed='10000101' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                print(f"[{self.now}] ({idx+1}/{stock}) 밸류에이션 계산 미완료")
                continue
            elif (check_valuation is None) & (check_fundamental != datetime.date(1000, 1, 1)):
                # 아직 밸류에이션 계산이 안됐고, 에러나지 않은 종목은 정상적으로 처리
                self.analyze_valuation_by_date_stock(start_date='20170101', end_date=self.today, stock=stock)
                sql = f"UPDATE status.analyze_stock_status SET valuation_analyzed='{self.today}' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                print(f"[{self.now}] ({idx+1}/{stock}) 밸류에이션 계산 완료")
                continue
            elif check_valuation.strftime('%Y%m') == self.today:  # 오늘 계산이 이미 됐다면 그냥 넘어감
                print(f"[{self.now}] ({idx+1}/{stock}) 밸류에이션 계산 이미 완료됨")
                continue
            elif check_valuation.strftime('%Y%m') != self.today:  # 마지막 계산이 오늘이 아니면 마지막 계산일 이후로 다시 계산함
                self.analyze_valuation_by_date_stock(start_date=check_valuation, end_date=self.today, stock=stock)
                sql = f"UPDATE status.analyze_stock_status SET valuation_analyzed='{self.today}' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                print(f"[{self.now}] ({idx+1}/{stock}) 밸류에이션 계산 완료")
                continue
        print(f"[{self.now}] (전종목) 밸류에이션 계산 완료")

if __name__=="__main__":
    analyze_valuation = analyze_valuation()
    analyze_valuation.analyze_valuation()
