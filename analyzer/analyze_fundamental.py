import pymysql
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
import config.setting as cf
from config import logger as logger

class analyze_fundamental():
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
        self.today = datetime.datetime.today().strftime('%Y-%m-%d')
        # DB초기화
        self.initialize_db()
        # 데이터프레임 조작시 SettingWithCopyWarning 에러 안 뜨게 처리
        pd.options.mode.chained_assignment = None

    def initialize_db(self):
        '''DB초기화'''
        # fundamental 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'fundamental'"
        if self.cur.execute(sql):
            self.logger.info("fundamental 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE fundamental"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("fundamental 스키마 생성")

    def drop_table(self, stock):
        '''해당 종목의 fundamental 테이블이 존재하면 삭제'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'fundamental' and table_name = '{stock}'"
        if self.cur.execute(sql):
            sql = f"DROP TABLE fundamental.`{stock}`"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"fundamental.{stock} 테이블 삭제 완료")
        else:
            self.logger.info(f"fundamental.{stock} 테이블 존재하지 않음")
            pass

    def copy_table(self, stock):
        '''해당 종목의 재무제표 테이블을 복사하고 PK지정'''
        # 테이블 복사
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'fundamental' and table_name = '{stock}'"
        if self.cur.execute(sql):
            self.logger.info(f"fundamental.{stock} 테이블 이미 존재함")
            pass
        else:
            sql = f"CREATE TABLE fundamental.`{stock}` SELECT * FROM financial_statements.`{stock}`"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"fundamental.{stock} 테이블 복사 완료")
        # PK지정
        sql = f"ALTER TABLE fundamental.`{stock}` MODIFY COLUMN date DATE PRIMARY KEY"
        self.cur.execute(sql)
        self.conn.commit()
        
    def add_columns(self, stock):
        '''해당 종목 테이블에 컬럼 추가'''
        col_list = {'매출액TTM': 'BIGINT(20)', '매출원가TTM': 'BIGINT(20)', '매출총이익TTM': 'BIGINT(20)', '판관비TTM': 'BIGINT(20)', '영업이익TTM': 'BIGINT(20)',
                    '세전순이익TTM': 'BIGINT(20)', '순이익TTM': 'BIGINT(20)', '영업현금TTM': 'BIGINT(20)', '투자현금TTM': 'BIGINT(20)', '재무현금TTM': 'BIGINT(20)',
                    '부채비율': 'FLOAT', '순부채비율': 'FLOAT', '자기자본비율': 'FLOAT', '유동비율': 'FLOAT',
                    'GPM': 'FLOAT', 'OPM': 'FLOAT', 'NPM': 'FLOAT', 'ROE': 'FLOAT', 'ROA': 'FLOAT', 'ROIC': 'FLOAT', 'GPA': 'FLOAT',
                    '총자산회전율': 'FLOAT', '유형자산회전율': 'FLOAT', '영업자산회전율': 'FLOAT', '재고자산회전율': 'FLOAT', '매출채권회전율': 'FLOAT', '매입채무회전율': 'FLOAT', '운전자본회전율': 'FLOAT',
                    '매출액증가율': 'FLOAT', '영업이익증가율': 'FLOAT', '순이익증가율': 'FLOAT', '유형자산증가율': 'FLOAT', '총자산증가율': 'FLOAT', '자기자본증가율': 'FLOAT',
                    'EPS': 'BIGINT(20)', 'BPS': 'BIGINT(20)', 'SPS': 'BIGINT(20)', 'CPS': 'BIGINT(20)', 'EPS증가율': 'FLOAT', 'F_SCORE': 'INT(10)'}
        for col_name in col_list:
            sql = f"ALTER TABLE fundamental.`{stock}` ADD {col_name} {col_list[col_name]}"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 테이블 컬럼 추가 완료")

    def calc_quarter(self, stock):
        '''사업보고서 재무제표를 분기화'''
        # 분기자료화할 항목 가져오기
        sql = f"SELECT date, 매출액, 매출원가, 매출총이익, 판관비, 영업이익, 세전순이익, 순이익, 영업현금, 투자현금, 재무현금 " \
              f"FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 결산월 가져오기
        sql = f"SELECT closing FROM stock_info.stock_info WHERE stock='{stock}'"
        self.cur.execute(sql)
        closing = int(self.cur.fetchone()['closing'])
        # 존재 체크를 위한 분기날짜 리스트 생성
        quarter_list = list(fs['date'])
        # 분기날짜 기준으로 순회하면서 계산하여 DB 입력
        for idx in range(len(fs)):
            if fs['date'][idx].month == int(closing):  # 결산월 분기에만 계산 실행
                year = fs['date'][idx].year
                date = fs['date'][idx].strftime('%Y%m%d')
                q4 = datetime.date(year, closing, 1) + relativedelta(months=1) - datetime.timedelta(days=1)
                q3 = datetime.date(year, closing, 1) - relativedelta(months=2) - datetime.timedelta(days=1)
                q2 = datetime.date(year, closing, 1) - relativedelta(months=5) - datetime.timedelta(days=1)
                q1 = datetime.date(year, closing, 1) - relativedelta(months=8) - datetime.timedelta(days=1)
                if (q1 in quarter_list) & (q2 in quarter_list) & (q3 in quarter_list):  # 4개 분기가 다 있는 경우
                    매출액 = int(fs[fs['date'] == q4]['매출액']) - int(fs[fs['date'] == q3]['매출액']) - int(fs[fs['date'] == q2]['매출액']) - int(fs[fs['date'] == q1]['매출액'])
                    매출원가 = int(fs[fs['date'] == q4]['매출원가']) - int(fs[fs['date'] == q3]['매출원가']) - int(fs[fs['date'] == q2]['매출원가']) - int(fs[fs['date'] == q1]['매출원가'])
                    매출총이익 = int(fs[fs['date'] == q4]['매출총이익']) - int(fs[fs['date'] == q3]['매출총이익']) - int(fs[fs['date'] == q2]['매출총이익']) - int(fs[fs['date'] == q1]['매출총이익'])
                    판관비 = int(fs[fs['date'] == q4]['판관비']) - int(fs[fs['date'] == q3]['판관비']) - int(fs[fs['date'] == q2]['판관비']) - int(fs[fs['date'] == q1]['판관비'])
                    영업이익 = int(fs[fs['date'] == q4]['영업이익']) - int(fs[fs['date'] == q3]['영업이익']) - int(fs[fs['date'] == q2]['영업이익']) - int(fs[fs['date'] == q1]['영업이익'])
                    세전순이익 = int(fs[fs['date'] == q4]['세전순이익']) - int(fs[fs['date'] == q3]['세전순이익']) - int(fs[fs['date'] == q2]['세전순이익']) - int(fs[fs['date'] == q1]['세전순이익'])
                    순이익 = int(fs[fs['date'] == q4]['순이익']) - int(fs[fs['date'] == q3]['순이익']) - int(fs[fs['date'] == q2]['순이익']) - int(fs[fs['date'] == q1]['순이익'])
                    영업현금 = int(fs[fs['date'] == q4]['영업현금']) - int(fs[fs['date'] == q3]['영업현금']) - int(fs[fs['date'] == q2]['영업현금']) - int(fs[fs['date'] == q1]['영업현금'])
                    투자현금 = int(fs[fs['date'] == q4]['투자현금']) - int(fs[fs['date'] == q3]['투자현금']) - int(fs[fs['date'] == q2]['투자현금']) - int(fs[fs['date'] == q1]['투자현금'])
                    재무현금 = int(fs[fs['date'] == q4]['재무현금']) - int(fs[fs['date'] == q3]['재무현금']) - int(fs[fs['date'] == q2]['재무현금']) - int(fs[fs['date'] == q1]['재무현금'])
                else:  # 4개 분기 중 한개라도 없는 경우
                    매출액 = int(int(fs[fs['date'] == q4]['매출액'])/4)
                    매출원가 = int(int(fs[fs['date'] == q4]['매출원가'])/4)
                    매출총이익 = int(int(fs[fs['date'] == q4]['매출총이익'])/4)
                    판관비 = int(int(fs[fs['date'] == q4]['판관비'])/4)
                    영업이익 = int(int(fs[fs['date'] == q4]['영업이익'])/4)
                    세전순이익 = int(int(fs[fs['date'] == q4]['세전순이익'])/4)
                    순이익 = int(int(fs[fs['date'] == q4]['순이익'])/4)
                    영업현금 = int(int(fs[fs['date'] == q4]['영업현금'])/4)
                    투자현금 = int(int(fs[fs['date'] == q4]['투자현금'])/4)
                    재무현금 = int(int(fs[fs['date'] == q4]['재무현금'])/4)
                sql = f"UPDATE fundamental.`{stock}` SET " \
                      f"매출액={매출액}, " \
                      f"매출원가={매출원가}, " \
                      f"매출총이익={매출총이익}, " \
                      f"판관비={판관비}, " \
                      f"영업이익={영업이익}, " \
                      f"세전순이익={세전순이익}, " \
                      f"순이익={순이익}, " \
                      f"영업현금={영업현금}, " \
                      f"투자현금={투자현금}, " \
                      f"재무현금={재무현금} " \
                      f"WHERE date='{date}'"
                self.cur.execute(sql)
                self.conn.commit()
        self.logger.info(f"({stock}) 재무제표 분기화 완료")

    def calc_TTM(self, stock):
        '''TTM 자료를 계산'''
        # TTM자료화할 항목 가져오기
        sql = f"SELECT date, 매출액, 매출원가, 매출총이익, 판관비, 영업이익, 세전순이익, 순이익, 영업현금, 투자현금, 재무현금 " \
              f"FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # TTM 자료 계산
        fs['매출액TTM'] = fs['매출액'].rolling(window=4).sum()
        fs['매출원가TTM'] = fs['매출원가'].rolling(window=4).sum()
        fs['매출총이익TTM'] = fs['매출총이익'].rolling(window=4).sum()
        fs['판관비TTM'] = fs['판관비'].rolling(window=4).sum()
        fs['영업이익TTM'] = fs['영업이익'].rolling(window=4).sum()
        fs['세전순이익TTM'] = fs['세전순이익'].rolling(window=4).sum()
        fs['순이익TTM'] = fs['순이익'].rolling(window=4).sum()
        fs['영업현금TTM'] = fs['영업현금'].rolling(window=4).sum()
        fs['투자현금TTM'] = fs['투자현금'].rolling(window=4).sum()
        fs['재무현금TTM'] = fs['재무현금'].rolling(window=4).sum()
        fs = fs.fillna(0)
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET " \
                  f"매출액TTM={row.매출액TTM}, " \
                  f"매출원가TTM={row.매출원가TTM}, " \
                  f"매출총이익TTM={row.매출총이익TTM}, " \
                  f"판관비TTM={row.판관비TTM}, " \
                  f"영업이익TTM={row.영업이익TTM}, " \
                  f"세전순이익TTM={row.세전순이익TTM}, " \
                  f"순이익TTM={row.순이익TTM}, " \
                  f"영업현금TTM={row.영업현금TTM}, " \
                  f"투자현금TTM={row.투자현금TTM}, " \
                  f"재무현금TTM={row.재무현금TTM} " \
                  f"WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 재무제표 분기화 완료")

    # 안정성: 부채비율, 순부채비율, 자기자본비율, 유동비율
    def calc_debt_ratio(self, stock):
        '''부채비율 = 부채총계/자본총계'''
        # 필요항목 가져오기
        sql = f"SELECT date, 부채총계, 자본총계, 부채비율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if fs['자본총계'][idx] == 0:
                fs['부채비율'][idx] = 0
            else:
                fs['부채비율'][idx] = fs['부채총계'][idx]/fs['자본총계'][idx]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 부채비율={row.부채비율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 부채비율 계산 완료")

    def calc_net_debt_ratio(self, stock):
        '''순부채비율 = (이자발생부채-현금)/자본총계'''
        # 필요항목 가져오기
        sql = f"SELECT date, 현금, 이자발생부채, 자본총계, 순부채비율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if fs['자본총계'][idx] == 0:
                fs['순부채비율'][idx] = 0
            else:
                fs['순부채비율'][idx] = (fs['이자발생부채'][idx] - fs['현금'][idx]) / fs['자본총계'][idx]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 순부채비율={row.순부채비율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 순부채비율 계산 완료")

    def calc_equity_capital_ratio(self, stock):
        '''자기자본비율 = 자본총계/자산총계'''
        # 필요항목 가져오기
        sql = f"SELECT date, 자본총계, 자산총계, 자기자본비율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if fs['자산총계'][idx] == 0:
                fs['자기자본비율'][idx] = 0
            else:
                fs['자기자본비율'][idx] = fs['자본총계'][idx] / fs['자산총계'][idx]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 자기자본비율={row.자기자본비율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 자기자본비율 계산 완료")

    def calc_liquidity_ratio(self, stock):
        '''유동비율 = 유동자산/유동부채'''
        # 필요항목 가져오기
        sql = f"SELECT date, 유동자산, 유동부채, 유동비율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if fs['유동부채'][idx] == 0:
                fs['유동비율'][idx] = 0
            else:
                fs['유동비율'][idx] = fs['유동자산'][idx] / fs['유동부채'][idx]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 유동비율={row.유동비율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 유동비율 계산 완료")

    # 수익성: GPM, OPM, NPM, ROE, ROA, ROIC, GPA
    def calc_GPM(self, stock):
        '''GPM = 매출총이익TTM/매출액TTM'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출총이익TTM, 매출액TTM, GPM FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if fs['매출액TTM'][idx] == 0:
                fs['GPM'][idx] = 0
            else:
                fs['GPM'][idx] = fs['매출총이익TTM'][idx] / fs['매출액TTM'][idx]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET GPM={row.GPM} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) GPM 계산 완료")

    def calc_OPM(self, stock):
        '''OPM = 영업이익TTM/매출액TTM'''
        # 필요항목 가져오기
        sql = f"SELECT date, 영업이익TTM, 매출액TTM, OPM FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if fs['매출액TTM'][idx] == 0:
                fs['OPM'][idx] = 0
            else:
                fs['OPM'][idx] = fs['영업이익TTM'][idx] / fs['매출액TTM'][idx]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET OPM={row.OPM} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) OPM 계산 완료")

    def calc_NPM(self, stock):
        '''NPM = 순이익TTM/매출액TTM'''
        # 필요항목 가져오기
        sql = f"SELECT date, 순이익TTM, 매출액TTM, NPM FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if fs['매출액TTM'][idx] == 0:
                fs['NPM'][idx] = 0
            else:
                fs['NPM'][idx] = fs['순이익TTM'][idx] / fs['매출액TTM'][idx]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET NPM={row.NPM} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) NPM 계산 완료")

    def calc_ROE(self, stock):
        '''ROE = 순이익TTM/(전기)자본총계'''
        # 필요항목 가져오기
        sql = f"SELECT date, 순이익TTM, 자본총계, ROE FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['ROE'][idx] = 0
            elif fs['자본총계'][idx-1] == 0:
                fs['ROE'][idx] = 0
            else:
                fs['ROE'][idx] = fs['순이익TTM'][idx] / fs['자본총계'][idx-1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET ROE={row.ROE} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) ROE 계산 완료")

    def calc_ROA(self, stock):
        '''ROA = 순이익TTM/(전기)자산총계'''
        # 필요항목 가져오기
        sql = f"SELECT date, 순이익TTM, 자산총계, ROA FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['ROA'][idx] = 0
            elif fs['자산총계'][idx-1] == 0:
                fs['ROA'][idx] = 0
            else:
                fs['ROA'][idx] = fs['순이익TTM'][idx] / fs['자산총계'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET ROA={row.ROA} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) ROA 계산 완료")

    def calc_ROIC(self, stock):
        '''ROIC = 세후영업이익TTM/영업투하자본, 세후영업이익TTM=영업이익TTM*(1-법인세율), 법인세율=(세전순이익TTM-순이익TTM)/세전순이익TTM, 영업투하자본=(유동자산-유동부채)+유형자산'''
        # 필요항목 가져오기
        sql = f"SELECT date, 영업이익TTM, 세전순이익TTM, 순이익TTM, 유동자산, 유동부채, 유형자산, ROIC FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['ROIC'][idx] = 0
            elif fs['세전순이익TTM'][idx] == 0:
                fs['ROIC'][idx] = 0
            elif fs['유동자산'][idx-1] - fs['유동부채'][idx-1] + fs['유형자산'][idx-1] == 0:
                fs['ROIC'][idx] = 0
            else:
                fs['ROIC'][idx] = (fs['영업이익TTM'][idx] * fs['순이익TTM'][idx]) / (fs['세전순이익TTM'][idx]*(fs['유동자산'][idx-1] - fs['유동부채'][idx-1] + fs['유형자산'][idx-1]))
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET ROIC={row.ROIC} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) ROIC 계산 완료")

    def calc_GPA(self, stock):
        '''GPA = 매출총이익TTM/(전기)자산총계'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출총이익TTM, 자산총계, GPA FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['GPA'][idx] = 0
            elif fs['자산총계'][idx-1] == 0:
                fs['GPA'][idx] = 0
            else:
                fs['GPA'][idx] = fs['매출총이익TTM'][idx] / fs['자산총계'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET GPA={row.GPA} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) GPA 계산 완료")

    # 활동성: 총자산회전율, 유형자산회전율, 영업자산회전율, 재고자산회전율, 매출채권회전율, 매입채무회전율, 순운전자본회전율
    def calc_assets_turnover(self, stock):
        '''총자산회전율 = 매출액TTM/(전기)자산총계'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출액TTM, 자산총계, 총자산회전율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['총자산회전율'][idx] = 0
            elif fs['자산총계'][idx-1] == 0:
                fs['총자산회전율'][idx] = 0
            else:
                fs['총자산회전율'][idx] = fs['매출액TTM'][idx] / fs['자산총계'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 총자산회전율={row.총자산회전율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 총자산회전율 계산 완료")

    def calc_property_turnover(self, stock):
        '''유형자산회전율 = 매출액TTM/(전기)유형자산'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출액TTM, 유형자산, 유형자산회전율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['유형자산회전율'][idx] = 0
            elif fs['유형자산'][idx-1] == 0:
                fs['유형자산회전율'][idx] = 0
            else:
                fs['유형자산회전율'][idx] = fs['매출액TTM'][idx] / fs['유형자산'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 유형자산회전율={row.유형자산회전율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 유형자산회전율 계산 완료")

    def calc_operating_assets_turnover(self, stock):
        '''영업자산회전율 = 매출액TTM/(전기)영업자산, 영업자산=유형자산+운전자본, 운전자본=매출채권+재고자산-매입채무'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출액TTM, 유형자산, 매출채권, 재고자산, 매입채무, 영업자산회전율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['영업자산회전율'][idx] = 0
            elif fs['유형자산'][idx - 1] + fs['매출채권'][idx - 1] + fs['재고자산'][idx - 1] - fs['매입채무'][idx - 1] == 0:
                fs['영업자산회전율'][idx] = 0
            else:
                fs['영업자산회전율'][idx] = fs['매출액TTM'][idx] / (fs['유형자산'][idx - 1] + fs['매출채권'][idx - 1] + fs['재고자산'][idx - 1] - fs['매입채무'][idx - 1])
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 영업자산회전율={row.영업자산회전율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 영업자산회전율 계산 완료")

    def calc_inventory_turnover(self, stock):
        '''재고자산회전율 = 매출액TTM/(전기)재고자산'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출액TTM, 재고자산, 재고자산회전율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['재고자산회전율'][idx] = 0
            elif fs['재고자산'][idx-1] == 0:
                fs['재고자산회전율'][idx] = 0
            else:
                fs['재고자산회전율'][idx] = fs['매출액TTM'][idx] / fs['재고자산'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 재고자산회전율={row.재고자산회전율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 재고자산회전율 계산 완료")

    def calc_receivables_turnover(self, stock):
        '''매출채권회전율 = 매출액TTM/(전기)매출채권'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출액TTM, 매출채권, 매출채권회전율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['매출채권회전율'][idx] = 0
            elif fs['매출채권'][idx - 1] == 0:
                fs['매출채권회전율'][idx] = 0
            else:
                fs['매출채권회전율'][idx] = fs['매출액TTM'][idx] / fs['매출채권'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 매출채권회전율={row.매출채권회전율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 매출채권회전율 계산 완료")

    def calc_payables_turnover(self, stock):
        '''매입채무회전율 = 매출액TTM/(전기)매입채무'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출액TTM, 매입채무, 매입채무회전율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['매입채무회전율'][idx] = 0
            elif fs['매입채무'][idx - 1] == 0:
                fs['매입채무회전율'][idx] = 0
            else:
                fs['매입채무회전율'][idx] = fs['매출액TTM'][idx] / fs['매입채무'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 매입채무회전율={row.매입채무회전율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 매입채무회전율 계산 완료")

    def calc_working_capital_turnover(self, stock):
        '''운전자본회전율 = 매출액(TTM)/(전기)운전자본, 운전자본 = 매출채권+재고자산-매입채무'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출액TTM, 매출채권, 재고자산, 매입채무, 운전자본회전율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['운전자본회전율'][idx] = 0
            elif (fs['매출채권'][idx - 1] + fs['재고자산'][idx - 1] - fs['매입채무'][idx - 1]) == 0:
                fs['운전자본회전율'][idx] = 0
            else:
                fs['운전자본회전율'][idx] = fs['매출액TTM'][idx] / (fs['매출채권'][idx - 1] + fs['재고자산'][idx - 1] - fs['매입채무'][idx - 1])
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 운전자본회전율={row.운전자본회전율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 운전자본회전율 계산 완료")

    # 성장성: 매출액증가율, 영업이익증가율, 순이익증가율, 유형자산증가율, 총자산증가율, 자기자본증가율
    def calc_sales_growth(self, stock):
        '''매출액증가율 = [매출액TTM-(전분기)매출액TTM]/(전분기)매출액TTM'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출액TTM, 매출액증가율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['매출액증가율'][idx] = 0
            elif fs['매출액TTM'][idx - 1] == 0:
                fs['매출액증가율'][idx] = 0
            else:
                fs['매출액증가율'][idx] = (fs['매출액TTM'][idx] - fs['매출액TTM'][idx - 1]) / fs['매출액TTM'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 매출액증가율={row.매출액증가율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 운전자본회전율 계산 완료")

    def calc_operating_profit_growth(self, stock):
        '''영업이익증가율 = [영업이익TTM-(전분기)영업이익TTM]/(전분기)영업이익TTM'''
        # 필요항목 가져오기
        sql = f"SELECT date, 영업이익TTM, 영업이익증가율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['영업이익증가율'][idx] = 0
            elif fs['영업이익TTM'][idx - 1] == 0:
                fs['영업이익증가율'][idx] = 0
            else:
                fs['영업이익증가율'][idx] = (fs['영업이익TTM'][idx] - fs['영업이익TTM'][idx - 1]) / fs['영업이익TTM'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 영업이익증가율={row.영업이익증가율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 영업이익증가율 계산 완료")

    def calc_net_income_growth(self, stock):
        '''순이익증가율 = [순이익TTM-(전분기)순이익TTM]/(전분기)순이익TTM'''
        # 필요항목 가져오기
        sql = f"SELECT date, 순이익TTM, 순이익증가율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['순이익증가율'][idx] = 0
            elif fs['순이익TTM'][idx - 1] == 0:
                fs['순이익증가율'][idx] = 0
            else:
                fs['순이익증가율'][idx] = (fs['순이익TTM'][idx] - fs['순이익TTM'][idx - 1]) / fs['순이익TTM'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 순이익증가율={row.순이익증가율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 순이익증가율 계산 완료")

    def calc_property_growth(self, stock):
        '''유형자산증가율 = [유형자산-(전분기)유형자산]/(전분기)유형자산'''
        # 필요항목 가져오기
        sql = f"SELECT date, 유형자산, 유형자산증가율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['유형자산증가율'][idx] = 0
            elif fs['유형자산'][idx - 1] == 0:
                fs['유형자산증가율'][idx] = 0
            else:
                fs['유형자산증가율'][idx] = (fs['유형자산'][idx] - fs['유형자산'][idx - 1]) / fs['유형자산'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 유형자산증가율={row.유형자산증가율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 유형자산증가율 계산 완료")

    def calc_assets_growth(self, stock):
        '''총자산증가율 = [자산총계-(전분기)자산총계]/(전분기)자산총계'''
        # 필요항목 가져오기
        sql = f"SELECT date, 자산총계, 총자산증가율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['총자산증가율'][idx] = 0
            elif fs['자산총계'][idx - 1] == 0:
                fs['총자산증가율'][idx] = 0
            else:
                fs['총자산증가율'][idx] = (fs['자산총계'][idx] - fs['자산총계'][idx - 1]) / fs['자산총계'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 총자산증가율={row.총자산증가율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 총자산증가율 계산 완료")

    def calc_capital_growth(self, stock):
        '''자기자본증가율 = [자본총계-(전분기)자본총계]/(전분기)자본총계'''
        # 필요항목 가져오기
        sql = f"SELECT date, 자본총계, 자기자본증가율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['자기자본증가율'][idx] = 0
            elif fs['자본총계'][idx - 1] == 0:
                fs['자기자본증가율'][idx] = 0
            else:
                fs['자기자본증가율'][idx] = (fs['자본총계'][idx] - fs['자본총계'][idx - 1]) / fs['자본총계'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET 자기자본증가율={row.자기자본증가율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 자기자본증가율 계산 완료")

    # 주당가치: EPS, BPS, SPS, CPS
    def calc_EPS(self, stock):
        '''EPS = 순이익TTM/발행주식수'''
        # 필요항목 가져오기
        sql = f"SELECT date, 순이익TTM, EPS FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        sql = f"SELECT shares FROM stock_info.stock_info WHERE stock='{stock}'"
        self.cur.execute(sql)
        shares = self.cur.fetchone()['shares']
        # 계산
        for idx in range(len(fs)):
            fs['EPS'][idx] = fs['순이익TTM'][idx] / shares
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET EPS={row.EPS} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) EPS 계산 완료")

    def calc_BPS(self, stock):
        '''BPS = 자본총계/발행주식수'''
        # 필요항목 가져오기
        sql = f"SELECT date, 자본총계, BPS FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        sql = f"SELECT shares FROM stock_info.stock_info WHERE stock='{stock}'"
        self.cur.execute(sql)
        shares = self.cur.fetchone()['shares']
        # 계산
        for idx in range(len(fs)):
            fs['BPS'][idx] = fs['자본총계'][idx] / shares
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET BPS={row.BPS} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) BPS 계산 완료")

    def calc_SPS(self, stock):
        '''SPS = 매출액TTM/발행주식수'''
        # 필요항목 가져오기
        sql = f"SELECT date, 매출액TTM, SPS FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        sql = f"SELECT shares FROM stock_info.stock_info WHERE stock='{stock}'"
        self.cur.execute(sql)
        shares = self.cur.fetchone()['shares']
        # 계산
        for idx in range(len(fs)):
            fs['SPS'][idx] = fs['매출액TTM'][idx] / shares
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET SPS={row.SPS} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) SPS 계산 완료")

    def calc_CPS(self, stock):
        '''CPS = 영업현금TTM/발행주식수'''
        # 필요항목 가져오기
        sql = f"SELECT date, 영업현금TTM, CPS FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        sql = f"SELECT shares FROM stock_info.stock_info WHERE stock='{stock}'"
        self.cur.execute(sql)
        shares = self.cur.fetchone()['shares']
        # 계산
        for idx in range(len(fs)):
            fs['CPS'][idx] = fs['영업현금TTM'][idx] / shares
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET CPS={row.CPS} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) CPS 계산 완료")

    def calc_EPS_growth(self, stock):
        '''EPS증가율 = (EPS-(전분기)EPS)/(전분기)EPS'''
        # 필요항목 가져오기
        sql = f"SELECT date, EPS, EPS증가율 FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['EPS증가율'][idx] = 0
            elif fs['EPS'][idx - 1] == 0:
                fs['EPS증가율'][idx] = 0
            else:
                fs['EPS증가율'][idx] = (fs['EPS'][idx] - fs['EPS'][idx - 1]) / fs['EPS'][idx - 1]
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET EPS증가율={row.EPS증가율} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) EPS증가율 계산 완료")

    # Piotroski f-score
    def calc_f_score(self, stock):
        ''' ROA : 순이익TTM > 0
            CFO : 영업현금TTM > 0
            dROA : ROA 증가
            ACCRUAL : 순이익TTM < 영업현금TTM
            dLEVER : 비유동부채/자산총계 비율 감소
            dLIQUID : 유동비율 증가
            EQ_OFFER : 발행주식수 x
            dMARGIN : GPM 증가
            dTURN : 총자산회전율 증가 '''
        # 필요항목 가져오기
        sql = f"SELECT date, 비유동부채, 자산총계, 발행주식수, 순이익TTM, 영업현금TTM, 유동비율, 총자산회전율, ROA, GPM, F_SCORE FROM fundamental.`{stock}`"
        self.cur.execute(sql)
        fs = self.cur.fetchall()
        fs = pd.DataFrame(fs)
        # 계산
        for idx in range(len(fs)):
            if idx == 0:
                fs['F_SCORE'][idx] = 0
            else:
                ROA, CFO, dROA, ACCRUAL, dLEVER, dLIQUID, EQ_OFFER, dMARGIN, dTURN = 0, 0, 0, 0, 0, 0, 0, 0, 0
                if fs['순이익TTM'][idx] > 0:
                    ROA = 1
                if fs['영업현금TTM'][idx] > 0:
                    CFO = 1
                if fs['ROA'][idx] > fs['ROA'][idx-1]:
                    dROA = 1
                if fs['순이익TTM'][idx] < fs['영업현금TTM'][idx]:
                    ACCRUAL = 1
                if (fs['자산총계'][idx-1] != 0) and (fs['자산총계'][idx] != 0) and (fs['비유동부채'][idx]/fs['자산총계'][idx] < fs['비유동부채'][idx-1]/fs['자산총계'][idx-1]):
                    dLEVER = 1
                if fs['유동비율'][idx] > fs['유동비율'][idx-1]:
                    dLIQUID = 1
                if fs['발행주식수'][idx] == 0:
                    EQ_OFFER = 1
                if fs['GPM'][idx] > fs['GPM'][idx-1]:
                    dMARGIN = 1
                if fs['총자산회전율'][idx] > fs['총자산회전율'][idx-1]:
                    dTURN = 1
                fs['F_SCORE'][idx] = ROA + CFO + dROA + ACCRUAL + dLEVER + dLIQUID + EQ_OFFER + dMARGIN + dTURN
        # DB입력
        for row in fs.itertuples():
            sql = f"UPDATE fundamental.`{stock}` SET F_SCORE={row.F_SCORE} WHERE date='{row.date}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) F_SCORE 계산 완료")

    def analyze_fundamental_by_stock(self, stock):
        '''종목별로 재무분석 실행'''
        self.drop_table(stock)  # 기존 테이블 삭제
        self.copy_table(stock)  # 재무제표 테이블 복사
        self.add_columns(stock)  # 테이블에 컬럼 추가
        self.calc_quarter(stock)  # 사업보고서 분기자료화
        self.calc_TTM(stock)  # TTM자료화
        self.calc_debt_ratio(stock)  # 부채비율
        self.calc_net_debt_ratio(stock)  # 순부채비율
        self.calc_equity_capital_ratio(stock)  # 자기자본비율
        self.calc_liquidity_ratio(stock)  # 유동비율
        self.calc_GPM(stock)  # GPM
        self.calc_OPM(stock)  # OPM
        self.calc_NPM(stock)  # NPM
        self.calc_ROE(stock)  # ROE
        self.calc_ROA(stock)  # ROA
        self.calc_ROIC(stock)  # ROIC
        self.calc_GPA(stock)  # GPA
        self.calc_assets_turnover(stock)  # 총자산회전율
        self.calc_property_turnover(stock)  # 유형자산회전율
        self.calc_operating_assets_turnover(stock)  # 영업자산회전율
        self.calc_inventory_turnover(stock)  # 재고자산회전율
        self.calc_receivables_turnover(stock)  # 매출채권회전율
        self.calc_payables_turnover(stock)  # 매입채무회전율
        self.calc_working_capital_turnover(stock)  # 운전자본회전율
        self.calc_sales_growth(stock)  # 매출액증가율
        self.calc_operating_profit_growth(stock)  # 영업이익증가율
        self.calc_net_income_growth(stock)  # 순이익증가율
        self.calc_property_growth(stock)  # 유형자산증가율
        self.calc_assets_growth(stock)  # 총자산증가율
        self.calc_capital_growth(stock)  # 자기자본증가율
        self.calc_EPS(stock)  # EPS
        self.calc_BPS(stock)  # BPS
        self.calc_SPS(stock)  # SPS
        self.calc_CPS(stock)  # CPS
        self.calc_EPS_growth(stock)  # EPS증가율
        self.calc_f_score(stock)  # F_SCORE

    def analyze_fundamental(self):
        '''전체 종목 재무분석 실행'''
        # 종목 리스트(stock_list) 가져오기
        sql = "SELECT a.stock, a.financial_statements_scraped, b.fundamental_analyzed " \
              "FROM status.scrap_stock_status a " \
              "INNER JOIN status.analyze_stock_status b ON a.stock=b.stock"
        self.cur.execute(sql)
        stock_list = self.cur.fetchall()
        stock_list = pd.DataFrame(stock_list)

        self.logger.info("(전종목) 펀더멘털 계산 시작")
        for idx in range(len(stock_list)):
            stock = stock_list['stock'][idx]
            check_scrap = stock_list['financial_statements_scraped'][idx]
            check_analysis = stock_list['fundamental_analyzed'][idx]

            # 종목별, 스크랩 상태별 스크랩 실행
            if check_scrap is None:  # 스크랩이 아직 안된 경우 다음 종목으로 그냥 넘어감
                self.logger.info(f"({idx+1}/{stock}) 재무제표 스크랩 아직 안됨")
                continue
            elif check_scrap == datetime.date(9000, 1, 1):  # 스크랩 비대상 종목(90000101)은 펀더멘털 계산 비대상 종목(90000101)으로 처리
                sql = f"UPDATE status.analyze_stock_status SET fundamental_analyzed='90000101' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                self.logger.info("({idx+1}/{stock}) 펀더멘털 계산 비대상 종목")
                continue
            elif check_scrap == datetime.date(1000, 1, 1):  # 스크랩 에러가 났던 종목(10000101)은 펀더멘털 계산하되 펀더멘털 계산 에러 종목(10000101)로 처리
                try:
                    self.analyze_fundamental_by_stock(stock=stock)
                except Exception as e:
                    self.logger.error(f"({idx+1}/{stock}) 펀더멘털 계산 미완료:" + str(e))
                sql = f"UPDATE status.analyze_stock_status SET fundamental_analyzed='10000101' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                self.logger.info(f"({idx+1}/{stock}) 펀더멘털 계산 미완료")
                continue
            elif check_analysis is None:  # 펀더멘털 계산 안된 종목 계산 실행
                self.analyze_fundamental_by_stock(stock=stock)
                sql = f"UPDATE status.analyze_stock_status SET fundamental_analyzed='{self.today}' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                self.logger.info(f"({idx+1}/{stock}) 펀더멘털 계산 완료")
                continue
            elif check_analysis.strftime('%Y%m') == datetime.date.today().strftime('%Y%m'):  # 이번달에 펀더멘털 계산을 이미 했다면 그냥 넘어감
                self.logger.info(f"({idx+1}/{stock}) 펀더멘털 계산 이미 완료됨")
                continue
            elif check_analysis.strftime('%Y%m') < datetime.date.today().strftime('%Y%m'):  # 펀더멘털 계산한지 한 달이 지난 종목은 다시 계산
                self.analyze_fundamental_by_stock(stock=stock)
                self.logger.info(f"({idx+1}/{stock}) 펀더멘털 계산 완료")
                continue
        self.logger.info("(전종목) 펀더멘털 계산 완료")


if __name__=="__main__":
    analyze_fundamental = analyze_fundamental()
    analyze_fundamental.analyze_fundamental()