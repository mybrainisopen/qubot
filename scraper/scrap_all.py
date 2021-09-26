import pymysql
import datetime
from common import config as cf
from common import logger as logger
from common import init_db as init_db
from scraper import scrap_stock_info as ssi
from scraper import scrap_market_index as smi
from scraper import scrap_macro_economics as sme
from scraper import scrap_daily_price as sdp
from scraper import scrap_financial_statements as sfs

class ScrapAll():
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
        self.initialize_db = init_db.InitDB()
        # 스크랩 모듈별 클래스 불러오기
        self.ssi = ssi.ScrapStockInfo()
        self.smi = smi.ScrapMarketIndex()
        self.sme = sme.ScrapMacroEconomics()
        self.sdp = sdp.ScrapDailyPrice()
        self.sfs = sfs.ScrapFinancialStatements()

    def scrap_check(self):
        '''스크랩 실행'''
        sql = """SELECT * FROM status.scrap_all_status"""
        self.cur.execute(sql)
        checklist = self.cur.fetchall()
        checklist = list(checklist[0].values())

        if checklist[0] != self.today:
            self.logger.info("stock_info 스크랩 시작!")
            # stock_info 스크랩 실행
            self.ssi.scrap_stock_info()
            self.ssi.scrap_stock_konex()
            self.ssi.scrap_stock_insincerity()
            self.ssi.scrap_stock_managing()
            sql = f"UPDATE status.scrap_all_status SET stock_info_scraped='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("stock_info 스크랩 완료!")

        if checklist[1] != self.today:
            self.logger.info("market_index 스크랩 시작!")
            # market_index 스크랩 실행
            self.smi.scrap_kospi()
            self.smi.scrap_kosdaq()
            sql = f"UPDATE status.scrap_all_status SET market_index_scraped='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("market_index 스크랩 완료!")

        if checklist[2] != self.today:
            # self.logger.info("macro_economics 스크랩 시작!")
            # # macro_economics 스크랩 실행
            # self.sme.scrap_macro_economics()
            # sql = f"UPDATE status.scrap_all_status SET macro_economics_scraped='20200101'"    # '{self.today}'"
            # self.cur.execute(sql)
            # self.conn.commit()
            # self.logger.info("macro_economics 스크랩 완료!")
            pass

        if checklist[3] != self.today:
            self.logger.info("daily_price 스크랩 시작!")
            # daily_price 스크랩 실행
            self.sdp.scrap_daily_price_naver_chart()
            sql = f"UPDATE status.scrap_all_status SET daily_price_scraped='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("daily_price 스크랩 완료!")

        if checklist[4] != self.today:
            self.logger.info("financial_statements 스크랩 시작!")
            # financial_statements 스크랩 실행
            self.sfs.scrap_financial_statements()
            self.sfs.scrap_bug_fix()
            sql = f"UPDATE status.scrap_all_status SET financial_statements_scraped='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("financial_statements 스크랩 완료!")



if __name__ == '__main__':
    scrap_all = ScrapAll()
    scrap_all.scrap_check()