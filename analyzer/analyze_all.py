import pymysql
import datetime
from config import setting as cf
from config import logger as logger
from analyzer import analyze_fundamental as af
from analyzer import analyze_valuation as av
from analyzer import analyze_momentum as am
from analyzer import build_universe as bu

class analyze_all():
    def __init__(self):
        '''생성자 : 기본 변수 생성'''
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
        self.today = datetime.date.today()
        # DB초기화
        self.initialize_db()
        # 분석 모듈 불러오기
        self.af = af.analyze_fundamental()
        self.av = av.analyze_valuation()
        self.am = am.analyze_momentum()
        self.bu = bu.universe_builder()

    def initialize_db(self):
        '''DB초기화'''
        # status 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'status'"
        if self.cur.execute(sql):
            self.logger.info("status 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE status"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("status 스키마 생성")

        # status.scrap_all_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'scrap_all_status'"
        if self.cur.execute(sql):
            self.logger.info("status.scrap_all_status 테이블 존재")
            pass
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
            self.logger.info("status.scrap_all_status 테이블 생성")

        # status.scrap_stock_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'scrap_stock_status'"
        if self.cur.execute(sql):
            self.logger.info("status.scrap_stock_status 테이블 존재")
            pass
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
            self.logger.info("status.scrap_stock_status 테이블 생성")

        # status.analyze_all_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'analyze_all_status'"
        if self.cur.execute(sql):
            self.logger.info("status.analyze_all_status 테이블 존재")
            pass
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
                    ('2017-01-01', '2017-01-01', '2017-01-01', '2017-01-01')"""
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("status.analyze_all_status 테이블 생성")

        # status.analyze_stock_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'analyze_stock_status'"
        if self.cur.execute(sql):
            self.logger.info("status.analyze_stock_status 테이블 존재")
            pass
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
            self.logger.info("status.analyze_stock_status 테이블 생성")

    def analysis_check(self):
        '''스크랩 실행'''
        sql = """SELECT * FROM status.analyze_all_status"""
        self.cur.execute(sql)
        checklist = self.cur.fetchall()
        checklist = list(checklist[0].values())

        if checklist[0] != self.today:
            self.logger.info("fundamental 분석 시작!")
            # fundamental 분석 실행
            self.af.analyze_fundamental()
            sql = f"UPDATE status.analyze_all_status SET fundamental_analyzed='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("fundamental 분석 완료!")

        if checklist[1] != self.today:
            self.logger.info("valuation 분석 시작!")
            # valuation 분석 실행
            self.av.analyze_valuation()
            sql = f"UPDATE status.analyze_all_status SET valuation_analyzed='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("valuation 분석 완료!")

        if checklist[2] != self.today:
            self.logger.info("momentum 분석 시작!")
            # momentum 분석 실행
            self.am.analyze_momentum()
            sql = f"UPDATE status.analyze_all_status SET momentum_analyzed='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("momentum 분석 완료!")

        if checklist[3] != self.today:
            self.logger.info("universe 분석 시작!")
            # universe 분석 실행
            self.bu.universe_builder()
            sql = f"UPDATE status.analyze_all_status SET universe_analyzed='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("universe 분석 완료!")

if __name__ == '__main__':
    analyze_all = analyze_all()
    analyze_all.analysis_check()