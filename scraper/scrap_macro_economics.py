import pymysql
import requests
import time
import pandas as pd
import config.config as cf
from datetime import datetime
from bs4 import BeautifulSoup

class scrap_macro_economics():
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
        # macro_economics 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'macro_economics'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] macro_economics 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE macro_economics"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] macro_economics 스키마 생성")

        # global_index 테이블 생성
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = 'global_index'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] macro_economics.global_index 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`global_index` (" \
                  f"date DATE," \
                  f"다우산업 FLOAT, " \
                  f"나스닥종합 FLOAT, " \
                  f"SNP500 FLOAT, " \
                  f"니케이225 FLOAT, " \
                  f"상해종합 FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] macro_economics.global_index 테이블 생성 완료")

        # oil_gold_exchange_rate 테이블 생성
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = 'oil_gold_exchange_rate'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] macro_economics.oil_gold_exchange_rate 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`oil_gold_exchange_rate` (" \
                  f"date DATE," \
                  f"유가WTI FLOAT, " \
                  f"원달러환율 FLOAT, " \
                  f"국제금 FLOAT, " \
                  f"국내금 FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] macro_economics.oil_gold_exchange_rate 테이블 생성 완료")

        # raw_materials 테이블 생성
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = 'raw_materials'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] macro_economics.raw_materials 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`raw_materials` (" \
                  f"date DATE," \
                  f"가스오일 FLOAT, " \
                  f"난방유 FLOAT, " \
                  f"천연가스 FLOAT, " \
                  f"구리 FLOAT, " \
                  f"납 FLOAT, " \
                  f"아연 FLOAT, " \
                  f"니켈 FLOAT, " \
                  f"알루미늄합금 FLOAT, " \
                  f"주석 FLOAT, " \
                  f"옥수수 FLOAT, " \
                  f"설탕 FLOAT, " \
                  f"대두 FLOAT, " \
                  f"대두박 FLOAT, " \
                  f"대두유 FLOAT, " \
                  f"면화 FLOAT, " \
                  f"소맥 FLOAT, " \
                  f"쌀 FLOAT, " \
                  f"오렌지주스 FLOAT, " \
                  f"커피 FLOAT, " \
                  f"코코아 FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] macro_economics.raw_materials 테이블 생성 완료")

        # interest_rate 테이블 생성
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = 'interest_rate'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] macro_economics.interest_rate 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`interest_rate` (" \
                  f"date DATE, " \
                  f"CD금리91일 DATE," \
                  f"콜금리 FLOAT, " \
                  f"국고채3년 FLOAT, " \
                  f"회사채3년 FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] macro_economics.interest_rate 테이블 생성 완료")

    def scrap_global_index(self):

        url = 'https://finance.naver.com/world/worldDayListJson.nhn?symbol=DJI@DJI&fdtc=0&page=2'
        # url = 'https://finance.naver.com/world/worldDayListJson.nhn?symbol=DJI@DJI'
        webpage = requests.get(url, headers=self.headers)
        soup = BeautifulSoup(webpage.content, 'lxml')
        print(soup)
        pgrr = soup.find("td", class_="pgRR")
        print(pgrr)

        # s = str(pgrr.a["href"]).split('=')
        # if pgrr is None:
        #     return None
        # lastpage = s[-1]

        # if max_date is None:
        #     pages = int(lastpage)
        # elif max_date == self.today:
        #     pages = 1
        # else:
        #     pages = 10
        #
        # df = pd.DataFrame()
        # for page in range(1, pages + 1):
        #     pg_url = '{}&page={}'.format(url, page)
        #     df = df.append(pd.read_html(pg_url, header=0)[0])
        #     time.sleep(0.01)
        #     print(f'[{self.now}] (KOSPI지수) {page}/{pages} pages are now downloading')
        # df = df.rename(columns={'날짜': 'date', '체결가': 'close', '거래량(천주)': 'volume', '거래대금(백만)': 'transaction'})
        # df = df.dropna()
        # df = df.reset_index(drop=True)
        # df = df[['date', 'close', 'volume', 'transaction']]
        #
        # df['volume'] = df['volume'].replace(',', '') * 1000
        # df['transaction'] = df['transaction'].replace(',', '') * 1000000
        #
        # for r in df.itertuples():
        #     sql = f"REPLACE INTO market_index.`kospi` VALUES ('{r.date.replace('.', '-')}','{r.close}','{r.volume}','{r.transaction}')"
        #     self.cur.execute(sql)
        #     self.conn.commit()
        # print(f"[{self.now}] (KOSPI지수) 스크랩 완료")
        pass

    def scrap_oil_gold_exchange_rate(self):
        pass

    def scrap_raw_materials(self):
        pass

    def scrap_interest_rate(self):
        pass



    def scrap_macro_economics(self):
        pass

if __name__ == "__main__":
    scrap_macro_economics = scrap_macro_economics()
    # scrap_macro_economics.scrap_macro_economics()
    scrap_macro_economics.scrap_global_index()

'''
global_index
- 다우산업
- 나스닥종합
- S&P500
- 니케이225
- 상해종합

raw_materials
- 가스오일
- 난방유
- 천연가스
- 구리
- 납
- 아연
- 니켈
- 알루미늄합금
- 주석
- 옥수수
- 설탕
- 대두
- 대두박
- 대두유
- 면화
- 소맥
- 쌀
- 오렌지주스
- 커피
- 코코아

interest_rate
- CD금리(91일)
- 콜금리
- 국고채(3년)
- 회사채(3년)
'''

