import pymysql
import requests
import time
import pandas as pd
import config.config as cf
import datetime
from bs4 import BeautifulSoup

class scrap_market_index():
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
        self.now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        self.today = datetime.date.today()
        self.headers = {'User-Agent': cf.user_agent}

        # DB초기화
        self.initialize_db()

    def initialize_db(self):
        # market_index 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'market_index'"
        if self.cur.execute(sql):
            print(f"[{self.now}] market_index 스키마 존재")
        else:
            sql = "CREATE DATABASE market_index"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] market_index 스키마 생성")

        # market_index.kospi 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'market_index' and table_name = 'kospi'"
        if self.cur.execute(sql):
            print(f"[{self.now}] market_index.kospi 테이블 존재")
        else:
            sql = """
                        CREATE TABLE IF NOT EXISTS market_index.kospi (
                        date DATE, 
                        close BIGINT(20), 
                        volume BIGINT(20), 
                        transaction BIGINT(20),  
                        PRIMARY KEY (date))
                    """
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] market_index.kospi 테이블 생성")

        # market_index.kosdaq 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'market_index' and table_name = 'kosdaq'"
        if self.cur.execute(sql):
            print(f"[{self.now}] market_index.kosdaq 테이블 존재")
        else:
            sql = """
                        CREATE TABLE IF NOT EXISTS market_index.kosdaq (
                        date DATE, 
                        close BIGINT(20), 
                        volume BIGINT(20), 
                        transaction BIGINT(20),  
                        PRIMARY KEY (date))
                    """
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] market_index.kosdaq 테이블 생성")


    def scrap_kospi(self):
        print(f"[{self.now}] (KOSPI지수) 스크랩 시작")
        sql = "SELECT max(date) FROM market_index.kospi"
        self.cur.execute(sql)
        max_date = self.cur.fetchone()['max(date)']

        url = f'https://finance.naver.com/sise/sise_index_day.nhn?code=KOSPI'
        webpage = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(webpage.content, 'html.parser')
        pgrr = soup.find("td", class_="pgRR")
        s = str(pgrr.a["href"]).split('=')
        if pgrr is None:
            return None
        lastpage = s[-1]

        if max_date is None:
            pages = int(lastpage)
        elif max_date == self.today:
            pages = 1
        else:
            pages = 10

        df = pd.DataFrame()
        for page in range(1, pages + 1):
            pg_url = '{}&page={}'.format(url, page)
            df = df.append(pd.read_html(pg_url, header=0)[0])
            time.sleep(0.01)
            print(f'[{self.now}] (KOSPI지수) {page}/{pages} pages are now downloading')
        df = df.rename(columns={'날짜': 'date', '체결가': 'close', '거래량(천주)': 'volume', '거래대금(백만)': 'transaction'})
        df = df.dropna()
        df = df.reset_index(drop=True)
        df = df[['date', 'close', 'volume', 'transaction']]

        df['volume'] = df['volume'].replace(',', '') * 1000
        df['transaction'] = df['transaction'].replace(',', '') * 1000000

        for r in df.itertuples():
            sql = f"REPLACE INTO market_index.`kospi` VALUES ('{r.date.replace('.', '-')}','{r.close}','{r.volume}','{r.transaction}')"
            self.cur.execute(sql)
            self.conn.commit()
        print(f"[{self.now}] (KOSPI지수) 스크랩 완료")
        pass

    def scrap_kosdaq(self):
        print(f"[{self.now}] (KOSDAQ지수) 스크랩 시작")
        sql = "SELECT max(date) FROM market_index.kosdaq"
        self.cur.execute(sql)
        max_date = self.cur.fetchone()['max(date)']

        url = f'https://finance.naver.com/sise/sise_index_day.nhn?code=KOSDAQ'
        webpage = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(webpage.content, 'html.parser')
        pgrr = soup.find("td", class_="pgRR")
        s = str(pgrr.a["href"]).split('=')
        if pgrr is None:
            return None
        lastpage = s[-1]

        if max_date is None:
            pages = int(lastpage)
        elif max_date == self.today:
            pages = 1
        else:
            pages = 10

        df = pd.DataFrame()
        for page in range(1, pages + 1):
            pg_url = '{}&page={}'.format(url, page)
            df = df.append(pd.read_html(pg_url, header=0)[0])
            time.sleep(0.01)
            print(f'[{self.now}] (KOSDAQ지수) {page}/{pages} pages are now downloading')
        df = df.rename(columns={'날짜': 'date', '체결가': 'close', '거래량(천주)': 'volume', '거래대금(백만)': 'transaction'})
        df = df.dropna()
        df = df.reset_index(drop=True)
        df = df[['date', 'close', 'volume', 'transaction']]

        df['volume'] = df['volume'].replace(',', '') * 1000
        df['transaction'] = df['transaction'].replace(',', '') * 1000000

        for r in df.itertuples():
            sql = f"REPLACE INTO market_index.`kosdaq` VALUES ('{r.date.replace('.', '-')}','{r.close}','{r.volume}','{r.transaction}')"
            self.cur.execute(sql)
            self.conn.commit()
        print(f"[{self.now}] (KOSDAQ지수) 스크랩 완료")
        pass


if __name__ == '__main__':
    scrap_market_index = scrap_market_index()
    scrap_market_index.scrap_kospi()
    scrap_market_index.scrap_kosdaq()