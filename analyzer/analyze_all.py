import pymysql
import datetime
from common import config as cf
from common import logger as logger
from common import init_db as init_db
from analyzer import analyze_fundamental as af
from analyzer import analyze_valuation as av
from analyzer import analyze_momentum as am
from analyzer import build_universe as bu

class AnalyzeAll():
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
        self.init_db = init_db.InitDB()
        # 분석 모듈 불러오기
        self.af = af.AnalyzeFundamental()
        self.av = av.AnalyzeValuation()
        self.am = am.AnalyzeMomentum()
        self.bu = bu.UniverseBuilder()

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
    analyze_all = AnalyzeAll()
    analyze_all.analysis_check()