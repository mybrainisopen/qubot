import pymysql
import datetime
from common import config as cf
from common import logger as logger

class InitDB():
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
        self.run()
        self.create_stock_status_table()

    def create_database(self, db_name):
        '''데이터베이스 생성 메소드'''
        sql = f"SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'"
        if self.cur.execute(sql):
            self.logger.info(f"{db_name} DB 존재")
            pass
        else:
            sql = f"CREATE DATABASE {db_name}"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"{db_name} DB 생성")

    def create_stock_status_table(self):
        '''stock_status 테이블 생성'''
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
                            ('2020-01-02', '2000-01-02', '2000-01-03', '2000-01-04')"""
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


    def run(self):
        self.create_database('status')
        self.create_database('scrap_stock_info')
        self.create_database('scrap_market_index')
        self.create_database('scrap_macro_economics')
        self.create_database('scrap_daily_price')
        self.create_database('scrap_financial_statements')
        self.create_database('scrap_IPO')
        self.create_database('analyze_fundamental')
        self.create_database('analyze_momentum')
        self.create_database('analyze_valuation')
        self.create_database('analyze_universe')
        self.create_database('backtest_book')
        self.create_database('backtest_portfolio')
        self.create_database('backtest_result')

if __name__=='__main__':
    initDB = InitDB()
    # initDB.run()
