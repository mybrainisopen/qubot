import pymysql
import datetime
from config import config as cf
from scraper import scrap_stock_info as ssi
from scraper import scrap_market_index as smi
from scraper import scrap_macro_economics as sme
from scraper import scrap_daily_price as sdp
from scraper import scrap_financial_statements as sfs

class scrap_all():
    def __init__(self):
        '''생성자 : 기본 변수 생성'''
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
        # DB초기화
        self.initialize_db()
        # 스크랩 모듈 불러오기
        self.ssi = ssi.scrap_stock_info()
        self.smi = smi.scrap_market_index()
        self.sme = sme.scrap_macro_economics()
        self.sdp = sdp.scrap_daily_price()
        self.sfs = sfs.scrap_financial_statements()

    def initialize_db(self):
        '''DB초기화'''

        # status 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'status'"
        if self.cur.execute(sql):
            print(f"[{self.now}] status 스키마 존재")
        else:
            sql = "CREATE DATABASE status"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] status 스키마 생성")

        # status.scrap_all_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'scrap_all_status'"
        if self.cur.execute(sql):
            print(f"[{self.now}] status.scrap_all_status 테이블 존재")
        else:
            sql = """
                CREATE TABLE IF NOT EXISTS status.scrap_all_status (
                stock_info_scraped DATE, 
                market_index_scraped DATE, 
                macro_economics_scraped DATE, 
                daily_price_scraped DATE,
                financial_statements_scraped DATE)
            """
            self.cur.execute(sql)
            self.conn.commit()
            # 더미 데이터 세팅
            sql = """INSERT INTO status.scrap_all_status VALUES 
                    ('2020-01-02', '2000-01-02', '2000-01-03', '2000-01-04', '2000-01-05')"""
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] status.scrap_all_status 테이블 생성")
            
        # status.scrap_stock_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'scrap_stock_status'"
        if self.cur.execute(sql):
            print(f"[{self.now}] status.scrap_stock_status 테이블 존재")
        else:
            sql = """
                CREATE TABLE IF NOT EXISTS status.scrap_stock_status (
                code CHAR(10), 
                stock VARCHAR(50), 
                stock_info_scraped DATE, 
                daily_price_scraped DATE,
                financial_statements_scraped DATE, 
                PRIMARY KEY (code, stock))
            """
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] status.scrap_stock_status 테이블 생성")

        # status.analyze_all_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'analyze_all_status'"
        if self.cur.execute(sql):
            print(f"[{self.now}] status.analyze_all_status 테이블 존재")
        else:
            sql = """
                CREATE TABLE IF NOT EXISTS status.analyze_all_status (
                fundamental_analyzed DATE, 
                valuation_analyzed DATE, 
                momentum_analyzed DATE, 
                universe_analyzed DATE)
            """
            self.cur.execute(sql)
            self.conn.commit()
            # 더미 데이터 세팅
            sql = """INSERT INTO status.analyze_all_status VALUES 
                    ('2020-01-02', '2000-01-02', '2000-01-03', '2000-01-04')"""
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] status.analyze_all_status 테이블 생성")

        # status.analyze_stock_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'analyze_stock_status'"
        if self.cur.execute(sql):
            print(f"[{self.now}] status.analyze_stock_status 테이블 존재")
        else:
            sql = """
                CREATE TABLE IF NOT EXISTS status.analyze_stock_status (
                code CHAR(10), 
                stock VARCHAR(50), 
                fundamental_analyzed DATE, 
                valuation_analyzed DATE,
                momentum_analyzed DATE, 
                PRIMARY KEY (code, stock))
            """
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] status.analyze_stock_status 테이블 생성")


    def scrap_check(self):
        '''스크랩 실행'''
        sql = """SELECT * FROM status.scrap_all_status"""
        self.cur.execute(sql)
        checklist = self.cur.fetchall()
        checklist = list(checklist[0].values())

        if checklist[0] != self.today:
            print(f"[{self.now}] stock_info 스크랩 시작!")
            # stock_info 스크랩 실행
            self.ssi.scrap_stock_info()
            self.ssi.scrap_stock_konex()
            self.ssi.scrap_stock_insincerity()
            self.ssi.scrap_stock_managing()
            sql = f"UPDATE status.scrap_all_status SET stock_info_scraped='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] stock_info 스크랩 완료!")

        if checklist[1] != self.today:
            print(f"[{self.now}] market_index 스크랩 시작!")
            # market_index 스크랩 실행
            self.smi.scrap_kospi()
            self.smi.scrap_kosdaq()
            sql = f"UPDATE status.scrap_all_status SET market_index_scraped='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] market_index 스크랩 완료!")

        if checklist[2] != self.today:
            print(f"[{self.now}] macro_economics 스크랩 시작!")
            # macro_economics 스크랩 실행
            self.sme.scrap_macro_economics()
            sql = f"UPDATE status.scrap_all_status SET macro_economics_scraped='20200101'"    # '{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] macro_economics 스크랩 완료!")

        if checklist[3] != self.today:
            print(f"[{self.now}] daily_price 스크랩 시작!")
            # daily_price 스크랩 실행
            self.sdp.scrap_daily_price()
            sql = f"UPDATE status.scrap_all_status SET daily_price_scraped='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] daily_price 스크랩 완료!")

        if checklist[4] != self.today:
            print(f"[{self.now}] financial_statements 스크랩 시작!")
            # financial_statements 스크랩 실행
            self.sfs.scrap_financial_statements()
            self.sfs.scrap_bug_fix()
            sql = f"UPDATE status.scrap_all_status SET financial_statements_scraped='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] financial_statements 스크랩 완료!")



if __name__ == '__main__':
    scrap_all = scrap_all()
    scrap_all.scrap_check()