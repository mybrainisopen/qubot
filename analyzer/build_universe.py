import pymysql
import pandas as pd
import common.config as cf
import datetime
from common import config as cf
from common import logger as logger
from common import init_db as init_db

class UniverseBuilder():
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
        self.now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        self.today = datetime.datetime.today().strftime('%Y%m%d')
        # DB초기화
        self.init_db = init_db.InitDB()

    def create_table(self, date):
        '''종목별 밸류에이션 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'universe' and table_name = '{date}'"
        if self.cur.execute(sql):
            self.logger.info(f"universe.{date} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS universe.`{date}` (" \
                  f"code CHAR(10), " \
                  f"stock VARCHAR(50), " \
                  f"ROE FLOAT, " \
                  f"ROA FLOAT, " \
                  f"GPA FLOAT, " \
                  f"F_SCORE INT(2), " \
                  f"PER FLOAT, " \
                  f"PBR FLOAT, " \
                  f"PSR FLOAT, " \
                  f"PCR FLOAT, " \
                  f"PEG FLOAT, " \
                  f"EVEBIT FLOAT, " \
                  f"1MRM FLOAT, " \
                  f"3MRM FLOAT, " \
                  f"6MRM FLOAT, " \
                  f"12MRM FLOAT, " \
                  f"PRIMARY KEY (code, stock))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"universe.{date} 테이블 생성 완료")

    def universe_builder_by_date(self, start_date, end_date):
        '''일자별로 유니버스 구축'''
        # 종목 리스트 가져오기
        sql = f"SELECT code, stock FROM stock_info.stock_info"
        self.cur.execute(sql)
        stock_list = self.cur.fetchall()
        stock_list = pd.DataFrame(stock_list)

        # 날짜 리스트 가져오기
        sql = f"SELECT date FROM market_index.kospi WHERE date BETWEEN '{start_date}' AND '{end_date}'"
        self.cur.execute(sql)
        date_list = self.cur.fetchall()
        date_list = pd.DataFrame(date_list)

        # 날짜별로 유니버스 구축
        for idx in range(len(date_list)):
            date = datetime.datetime.strftime(date_list['date'][idx], '%Y%m%d')
            self.create_table(date)  # 날짜별로 테이블 생성
            for idx in range(len(stock_list)):
                code = stock_list['code'][idx]
                stock = stock_list['stock'][idx]
                try:
                    sql = f"SELECT ROE, ROA, GPA, F_SCORE FROM fundamental.`{stock}` WHERE " \
                          f"date=(SELECT max(date) FROM fundamental.`{stock}` WHERE date<='{date}')"
                    self.cur.execute(sql)
                    fundamental = self.cur.fetchone()
                    sql = f"SELECT PER, PBR, PSR, PCR, PEG, EVEBIT FROM valuation.`{stock}` WHERE date='{date}'"
                    self.cur.execute(sql)
                    valuation = self.cur.fetchone()
                    sql = f"SELECT 1MRM, 3MRM, 6MRM, 12MRM FROM momentum.`{stock}` WHERE date='{date}'"
                    self.cur.execute(sql)
                    momentum = self.cur.fetchone()
                    ROE = fundamental['ROE']
                    ROA = fundamental['ROA']
                    GPA = fundamental['GPA']
                    F_SCORE = fundamental['F_SCORE']
                    PER = valuation['PER']
                    PBR = valuation['PBR']
                    PSR = valuation['PSR']
                    PCR = valuation['PCR']
                    PEG = valuation['PEG']
                    EVEBIT = valuation['EVEBIT']
                    MRM1 = momentum['1MRM']
                    MRM3 = momentum['3MRM']
                    MRM6 = momentum['6MRM']
                    MRM12 = momentum['12MRM']
                    sql = f"REPLACE INTO universe.`{date}` (code, stock, ROE, ROA, GPA, F_SCORE, PER, PBR, PSR, PCR, PEG, EVEBIT, 1MRM, 3MRM, 6MRM, 12MRM) " \
                          f"VALUES ('{code}', '{stock}', {ROE}, {ROA}, {GPA}, {F_SCORE}, {PER}, {PBR}, {PSR}, {PCR}, {PEG}, {EVEBIT}, {MRM1}, {MRM3}, {MRM6}, {MRM12}) "
                    self.cur.execute(sql)
                    self.conn.commit()
                    self.logger.info(f"({date}/{idx+1}/{stock}) 유니버스 구축 성공")
                except Exception as e:
                    self.logger.error(f"({date}/{idx+1}/{stock}) 유니버스 구축 실패:" + str(e))
                    continue
                
    def universe_builder(self):
        sql = f"SELECT universe_analyzed FROM status.analyze_all_status"
        self.cur.execute(sql)
        check = self.cur.fetchone()['universe_analyzed']
        check = datetime.datetime.strftime(check, '%Y%m%d')
        if check is None:
            self.universe_builder_by_date(start_date='20170101', end_date=self.today)
            sql = f"UPDATE status.analyze_all_status SET universe_analyzed='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("universe 분석 완료!")
        elif check < self.today:
            self.universe_builder_by_date(start_date=check, end_date=self.today)
            sql = f"UPDATE status.analyze_all_status SET universe_analyzed='{self.today}'"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("universe 분석 완료!")
        elif check == self.today:
            return


if __name__=="__main__":
    universe_builder = UniverseBuilder()
    universe_builder.universe_builder_by_date(start_date='20210901', end_date=datetime.datetime.today().strftime('%Y%m%d'))
