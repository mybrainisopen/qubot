import pymysql
import requests
import time
import pandas as pd
import config.config as cf
from datetime import datetime
from bs4 import BeautifulSoup

class scrap_daily_price():
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
        self.headers = {'User-Agent': cf.user_agent}
        # DB초기화
        self.initialize_db()

    def initialize_db(self):
        '''DB초기화'''
        # daily_price 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'daily_price'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] daily_price 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE daily_price"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] daily_price 스키마 생성")

    def create_tbl(self, stock):
        '''종목별 주가 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'daily_price' and table_name = '{stock}'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] daily_price.{stock} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS daily_price.`{stock}` (" \
                  f"date DATE," \
                  f"open BIGINT(20), " \
                  f"high BIGINT(20), " \
                  f"low BIGINT(20), " \
                  f"close BIGINT(20), " \
                  f"diff BIGINT(20), " \
                  f"volume BIGINT(20), " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] daily_price.{stock} 테이블 생성 완료")

    def scrap_daily_price(self):
        '''daily_price 스크랩'''
        # 종목 리스트 가져오기
        sql = "SELECT code, stock from status.scrap_stock_status"
        self.cur.execute(sql)
        stock_list = self.cur.fetchall()
        stock_list = pd.DataFrame(stock_list)

        # 종목별 스크랩
        for idx in range(len(stock_list)):
            code = stock_list['code'][idx]
            stock = stock_list['stock'][idx]

            self.create_tbl(stock=stock)  # 종목별 테이블 생성
            sql = f"SELECT max(date) From daily_price.`{stock}`"
            self.cur.execute(sql)
            max_date = self.cur.fetchone()
            max_date = list(max_date.values())[0]

            url = f"http://finance.naver.com/item/sise_day.nhn?code={code}"
            webpage = requests.get(url, headers=self.headers)
            soup = BeautifulSoup(webpage.content, 'html.parser')
            pgrr = soup.find("td", class_="pgRR")
            if pgrr is None:
                return None
            s = str(pgrr.a["href"]).split('=')
            lastpage = s[-1]
            df = pd.DataFrame()

            # 기준일자에 따라 최대 스크랩 분량 결정
            if max_date is None:
                pages = int(lastpage)
            elif str(max_date) == self.today:
                pages = 1
            else:
                pages = 10

            # 스크랩 실행
            try:
                for page in range(1, pages + 1):
                    pg_url = '{}&page={}'.format(url, page)
                    df = df.append(pd.read_html(pg_url, header=0)[0])
                    print(f'[{self.now}] ({idx+1:04d}/{len(stock_list)}) {stock} {page}/{pages} 주가 스크랩 중')
                df = df.rename(columns={'날짜': 'date', '종가': 'close', '전일비': 'diff', '시가': 'open', '고가': 'high', '저가': 'low', '거래량': 'volume'})
                df['date'] = df['date'].replace('.', '-')
                df = df.dropna()
                df[['close', 'diff', 'open', 'high', 'low', 'volume']] = df[['close', 'diff', 'open', 'high', 'low', 'volume']].astype(int)
                df = df[['date', 'open', 'high', 'low', 'close', 'diff', 'volume']]
                for r in df.itertuples():
                    sql = f"REPLACE INTO daily_price.`{stock}` VALUES ('{r.date}','{r.open}','{r.high}','{r.low}','{r.close}','{r.diff}','{r.volume}')"
                    self.cur.execute(sql)
                sql = f"UPDATE status.scrap_stock_status SET daily_price_scraped='{self.today}' WHERE code='{code}';"
                self.cur.execute(sql)
                self.conn.commit()
                time.sleep(0.1)
                print(f"[{self.now}] ({stock}) 주가 스크랩 완료")
            except Exception as e:
                print(f"[{self.now}] ({stock}) 주가 스크랩 에러:", str(e))

if __name__ == "__main__":
    scrap_daily_price = scrap_daily_price()
    scrap_daily_price.scrap_daily_price()