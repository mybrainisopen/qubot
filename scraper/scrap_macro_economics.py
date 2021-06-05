import pymysql
import requests
import time
import pandas as pd
from config import setting as cf
from datetime import datetime
from bs4 import BeautifulSoup, NavigableString
from config import logger as logger
from config.selenium_process import Selenium_process

class scrap_macro_economics():
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
            self.logger.info("macro_economics 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE macro_economics"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("macro_economics 스키마 생성")

    def create_index_table(self, name):
        '''글로벌 주가지수 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = '{name}'"
        if self.cur.execute(sql):
            self.logger.info(f"macro_economics.{name} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`{name}` (" \
                  f"date DATE," \
                  f"close FLOAT, " \
                  f"open FLOAT, " \
                  f"high FLOAT, " \
                  f"low FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"macro_economics.{name} 테이블 생성 완료")

    def create_exchange_table(self, name):
        '''국제 환율 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = '{name}'"
        if self.cur.execute(sql):
            self.logger.info(f"macro_economics.{name} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`{name}` (" \
                  f"date DATE," \
                  f"rate FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"macro_economics.{name} 테이블 생성 완료")

    def create_interest_table(self, name):
        '''국내 금리 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = '{name}'"
        if self.cur.execute(sql):
            self.logger.info(f"macro_economics.{name} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`{name}` (" \
                  f"date DATE," \
                  f"interest FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"macro_economics.{name} 테이블 생성 완료")

    def create_commodity_table(self, name):
        '''국제 원자재 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = '{name}'"
        if self.cur.execute(sql):
            self.logger.info(f"macro_economics.{name} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`{name}` (" \
                  f"date DATE," \
                  f"close FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"macro_economics.{name} 테이블 생성 완료")

    def scrap_global_index(self, name, url, page):
        print("글로벌지수")
        pass

    def scrap_exchange_rate(self, name, url, page):
        print("환율")
        pass
    
    def scrap_interest_rate(self, name, url, page):
        print("금리")
        pass
    
    def scrap_commodity(self, name, url, page):
        print("원자재")
        pass


    def scrap_macro_economics(self):
        print("거시경제전체")

        pass


if __name__ == "__main__":
    sme = scrap_macro_economics()
    sme.scrap_macro_economics()

'''
다우산업 :  https://finance.naver.com/world/sise.nhn?symbol=DJI@DJI // 일자, 종가, 시가, 고가, 저가
나스닥종합: https://finance.naver.com/world/sise.nhn?symbol=NAS@IXIC // 일자, 종가, 시가, 고가, 저가
S&P500 : https://finance.naver.com/world/sise.nhn?symbol=SPI@SPX // 일자, 종가, 시가, 고가, 저가
상해종합 : https://finance.naver.com/world/sise.nhn?symbol=SHS@000001 // 일자, 종가, 시가, 고가, 저가
니케이225 : https://finance.naver.com/world/sise.nhn?symbol=NII@NI225 // 일자, 종가, 시가, 고가, 저가

원달러 : https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_USDKRW   // 날짜, 매매기준율
원유로 : https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_EURKRW   // 날짜, 매매기준율
원엔 : https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_JPYKRW     // 날짜, 매매기준율

CD금리 : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CD91&page=1   // 날짜, 종가
콜금리 : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CALL   // 날짜, 종가
국고채3년 : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_GOVT03Y   // 날짜, 종가
회사채3년 : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CORP03Y   // 날짜, 종가

국내금 : https://finance.naver.com/marketindex/goldDailyQuote.nhn?  // 날짜, 종가(매매기준율)
국제금 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=CMDT_GC&fdtc=2   // 날짜, 종가
두바이유 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_DU&fdtc=2  // 날짜, 종가
유가WTI : https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_CL&fdtc=2  // 날짜, 종가

구리 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_CDY&page=1   // 날짜, 종가
납 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_PDY&page=1   // 날짜, 종가
아연 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_ZDY&page=1   // 날짜, 종가
니켈 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_NDY&page=1   // 날짜, 종가
알루미늄합금 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_AAY&page=1   // 날짜, 종가
주석 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SDY&page=1   // 날짜, 종가

옥수수 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_C&page=1    // 날짜, 종가
대두 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_S&page=1   // 날짜, 종가
대두박 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SM&page=1   // 날짜, 종가
대두유 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_BO&page=1    // 날짜, 종가
소맥 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_W&page=1   // 날짜, 종가
쌀 : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_RR&page=1   // 날짜, 종가



'''


