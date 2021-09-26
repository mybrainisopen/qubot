import pymysql
import datetime
import re
import pandas as pd
import dart_fss as dart
from dateutil.relativedelta import relativedelta
from common import config as cf
from common import logger as logger
from common import init_db as init_db

class ScrapFinancialStatements():
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
        self.today = datetime.datetime.today().strftime('%Y-%m-%d')
        self.headers = {'User-Agent': cf.user_agent}

        # DB초기화
        self.init_db = init_db.InitDB()

    def create_tbl(self, stock):
        '''종목별 주가 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'financial_statements' and table_name = '{stock}'"
        if self.cur.execute(sql):
            self.logger.info(f"financial_statements.{stock} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS financial_statements.`{stock}` (" \
                  f"date DATE," \
                  f"현금 BIGINT(20), " \
                  f"매출채권 BIGINT(20), " \
                  f"재고자산 BIGINT(20), " \
                  f"유동자산 BIGINT(20), " \
                  f"유형자산 BIGINT(20), " \
                  f"무형자산 BIGINT(20), " \
                  f"비유동자산 BIGINT(20), " \
                  f"자산총계 BIGINT(20), " \
                  f"매입채무 BIGINT(20), " \
                  f"유동부채 BIGINT(20), " \
                  f"이자발생부채 BIGINT(20), " \
                  f"비유동부채 BIGINT(20), " \
                  f"부채총계 BIGINT(20), " \
                  f"자본총계 BIGINT(20), " \
                  f"매출액 BIGINT(20), " \
                  f"매출원가 BIGINT(20), " \
                  f"매출총이익 BIGINT(20), " \
                  f"판관비 BIGINT(20), " \
                  f"영업이익 BIGINT(20), " \
                  f"세전순이익 BIGINT(20), " \
                  f"순이익 BIGINT(20), " \
                  f"영업현금 BIGINT(20), " \
                  f"투자현금 BIGINT(20), " \
                  f"재무현금 BIGINT(20), " \
                  f"발행주식수 BIGINT(20), " \
                  f"주당배당금 BIGINT(20), " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"financial_statements.{stock} 테이블 생성 완료")

    def scrap_financial_statements_by_start_year_stock(self, start_year, stock):
        '''시작연도, 종목명 기준 재무제표 스크랩하여 DB저장'''
        # 시작연도~현재+1년까지 기준연도 리스트 생성
        this_year = datetime.datetime.today().year
        year_list = list(range(start_year, this_year+2, 1))

        # DB에 재무제표 테이블 생성
        self.create_tbl(stock)

        # 결산월, 주식코드 가져오기
        sql = f"SELECT code, closing FROM stock_info.stock_info WHERE stock='{stock}'"
        self.cur.execute(sql)
        closing = self.cur.fetchone()['closing']
        sql = f"SELECT code, closing FROM stock_info.stock_info WHERE stock='{stock}'"
        self.cur.execute(sql)
        stock_code = self.cur.fetchone()['code']

        # 분기말날짜 및 레포트코드 목록 만들기
        quarter_list = []
        for year in year_list:
            q4 = (datetime.date(year, int(closing), 1) + relativedelta(months=1) - datetime.timedelta(days=1)).strftime('%Y%m%d')
            q3 = (datetime.date(year, int(closing), 1) - relativedelta(months=2) - datetime.timedelta(days=1)).strftime('%Y%m%d')
            q2 = (datetime.date(year, int(closing), 1) - relativedelta(months=5) - datetime.timedelta(days=1)).strftime('%Y%m%d')
            q1 = (datetime.date(year, int(closing), 1) - relativedelta(months=8) - datetime.timedelta(days=1)).strftime('%Y%m%d')
            quarter_dict_temp = {q1: '11013', q2: '11012', q3: '11014', q4: '11011'}
            for quarter in quarter_dict_temp:
                quarter_list.append({quarter: quarter_dict_temp[quarter]})

        # Dart코드 가져오기
        for key in cf.dart_key:
            try:
                dart.set_api_key(api_key=key)
                corp_code = dart.get_corp_list().find_by_stock_code(stock_code).to_dict()['corp_code']
                break
            except Exception as e:
                self.logger.error("Dart 접속 에러:" + str(e))
                continue

        # 스크랩 실행
        # 1분기보고서: 11013,  반기보고서: 11012,  3분기보고서: 11014,  사업보고서: 11011
        # CFS: 연결재무제표,  OFS: 개별재무제표
        for quarter in quarter_list:
            date = list(quarter.keys())[0]
            year = date[:4]
            report_code = list(quarter.values())[0]

            check_date = datetime.datetime.strptime(date, '%Y%m%d').date()
            min_date = datetime.date(2016, 1, 1)
            today = datetime.date.today()

            fs = list()
            eq = list()
            dd = list()
            if check_date < min_date:
                continue
            elif check_date > today:
                break
            else:
                try:
                    fs = dart.api.finance.get_single_fs(corp_code=corp_code, bsns_year=year, reprt_code=report_code, fs_div='CFS')['list']
                    self.logger.info(f"({stock}/{year}/{report_code}) 연결재무제표 스크랩 성공")
                except Exception as e:
                    self.logger.error(f"({stock}/{year}/{report_code}) 연결재무제표 스크랩 실패:" + str(e))
                    try:
                        fs = dart.api.finance.get_single_fs(corp_code=corp_code, bsns_year=year, reprt_code=report_code, fs_div='OFS')['list']
                        self.logger.info(f"({stock}/{year}/{report_code}) 개별재무제표 스크랩 성공")
                    except Exception as e:
                        self.logger.error(f"({stock}/{year}/{report_code}) 개별재무제표 스크랩 실패:" + str(e))
                try:
                    eq = dart.api.info.get_capital_increase(corp_code=corp_code, bsns_year=year, reprt_code=report_code)['list']
                    self.logger.info(f"({stock}/{year}/{report_code}) 주식발행내역 스크랩 성공")
                except Exception as e:
                    self.logger.error(f"({stock}/{year}/{report_code}) 주식발행내역 스크랩 실패:" + str(e))

                try:
                    dd = dart.api.info.get_dividend(corp_code=corp_code, bsns_year=year, reprt_code=report_code)['list']
                    self.logger.info(f"({stock}/{year}/{report_code}) 배당내역 스크랩 성공")
                except Exception as e:
                    self.logger.error(f"({stock}/{year}/{report_code}) 배당내역 스크랩 실패:" + str(e))

                if len(fs) == 0:  # 재무제표 스크랩이 제대로 안된 경우 에러코드 삽입
                    sql = f"UPDATE status.scrap_stock_status SET financial_statements_scraped='10000101' WHERE stock='{stock}'"
                    self.cur.execute(sql)
                    self.conn.commit()
                else:
                    현금, 매출채권, 재고자산, 유동자산, 유형자산, 무형자산, 비유동자산, 자산총계 = 0, 0, 0, 0, 0, 0, 0, 0  # 재무상태표(BS)
                    매입채무, 유동부채, 이자발생부채, 비유동부채, 부채총계, 자본총계 = 0, 0, 0, 0, 0, 0  # 재무상태표
                    매출액, 매출원가, 매출총이익, 판관비, 영업이익, 세전순이익, 순이익 = 0, 0, 0, 0, 0, 0, 0  # 손익계산서(IS) (또는 포괄손익계산서(CIS))
                    영업현금, 투자현금, 재무현금 = 0, 0, 0  # 현금흐름표(CF)
                    발행주식수 = 0  # 주식발행내역
                    주당배당금 = 0  # 배당내역

                    # 손익계산서(IS), 포괄손익계산서(CIS) 존재 체크를 위한 set 생성
                    sheet = set()
                    for idx in range(len(fs)):
                        sheet.add(fs[idx]['sj_div'])

                    # 각 재무 항목 숫자 정의
                    for idx in range(len(fs)):
                        if fs[idx]['sj_div'] == 'BS':
                            if fs[idx]['account_nm'].replace(' ', '') in ['현금및현금성자산']:
                                if fs[idx]['thstrm_amount'] == '':
                                    현금 += 0
                                else:
                                    현금 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['매출채권', '매출채권및기타채권', '매출채권및기타유동채권', '장기매출채권및기타비유동채권']:
                                if fs[idx]['thstrm_amount'] == '':
                                    매출채권 += 0
                                else:
                                    매출채권 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['재고자산']:
                                if fs[idx]['thstrm_amount'] == '':
                                    재고자산 += 0
                                else:
                                    재고자산 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['유동자산']:
                                if fs[idx]['thstrm_amount'] == '':
                                    유동자산 += 0
                                else:
                                    유동자산 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['유형자산']:
                                if fs[idx]['thstrm_amount'] == '':
                                    유형자산 += 0
                                else:
                                    유형자산 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['무형자산', '기타무형자산', '영업권', '영업권이외의무형자산']:
                                if fs[idx]['thstrm_amount'] == '':
                                    무형자산 += 0
                                else:
                                    무형자산 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['비유동자산']:
                                if fs[idx]['thstrm_amount'] == '':
                                    비유동자산 += 0
                                else:
                                    비유동자산 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['자산총계']:
                                if fs[idx]['thstrm_amount'] == '':
                                    자산총계 += 0
                                else:
                                    자산총계 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['매입채무', '매입채무및기타채무', '매입채무및기타유동채무', '단기매입채무']:
                                if fs[idx]['thstrm_amount'] == '':
                                    매입채무 += 0
                                else:
                                    매입채무 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['유동부채']:
                                if fs[idx]['thstrm_amount'] == '':
                                    유동부채 += 0
                                else:
                                    유동부채 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['이자발생부채', '사채', '차입금', '차입금및사채', '사채및차입금', '단기차입금및사채', '사채및단기차입금', '장기차입금및사채',
                                                                          '사채및장기차입금', '리스채권', '리스부채', '리스부채(유동)', '리스부채(비유동)', '단기사채', '단기차입금', '차입금(단기)',
                                                                          '유동성장기차입금', '단기매매금융부채', '당기손익인식(지정)금융부채', '단기상환우선주부채', '장기사채', '장기차입금',
                                                                          '금융리스부채' '장기상환우선주부채', '장기리스부채', '유동차입금', '비유동차입금', '회사채', '단기차입부채', '장기차입부채',
                                                                          '유동리스부채', '비유동리스부채']:
                                if fs[idx]['thstrm_amount'] == '':
                                    이자발생부채 += 0
                                else:
                                    이자발생부채 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['비유동부채']:
                                if fs[idx]['thstrm_amount'] == '':
                                    비유동부채 += 0
                                else:
                                    비유동부채 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['부채총계']:
                                if fs[idx]['thstrm_amount'] == '':
                                    부채총계 += 0
                                else:
                                    부채총계 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['자본총계', '분기말자본', '반기말자본']:
                                if fs[idx]['thstrm_amount'] == '':
                                    자본총계 += 0
                                else:
                                    자본총계 += int(fs[idx]['thstrm_amount'])

                        # IS가 있으면 IS로 / IS가 없으면 CIS로 스크랩 실행
                        elif (('IS' in sheet) & (fs[idx]['sj_div'] == 'IS')) | (('IS' not in sheet) & (fs[idx]['sj_div'] == 'CIS')):
                            if fs[idx]['account_nm'].replace(' ', '') in ['매출', '매출액', '매출액(수익)', '수익(매출액)', '수익(매출과지분법손익)']:
                                if fs[idx]['thstrm_amount'] == '':
                                    매출액 += 0
                                else:
                                    매출액 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['매출원가', '매출원가(영업비용)']:
                                if fs[idx]['thstrm_amount'] == '':
                                    매출원가 += 0
                                else:
                                    매출원가 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['매출총이익', '매출총이익(손실)', '영업수익']:
                                if fs[idx]['thstrm_amount'] == '':
                                    매출총이익 += 0
                                else:
                                    매출총이익 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['판관비', '판매비와관리비', '판매비', '관리비', '영업비용']:
                                if fs[idx]['thstrm_amount'] == '':
                                    판관비 += 0
                                else:
                                    판관비 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['영업이익', '영업이익(손실)']:
                                if fs[idx]['thstrm_amount'] == '':
                                    영업이익 += 0
                                else:
                                    영업이익 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['세전순이익', '법인세차감전순이익', '법인세비용차감전순이익', '법인세비용차감전순이익(손실)']:
                                if fs[idx]['thstrm_amount'] == '':
                                    세전순이익 += 0
                                else:
                                    세전순이익 += int(fs[idx]['thstrm_amount'])

                            if fs[idx]['account_nm'].replace(' ', '') in ['순이익', '당기순이익', '분기순이익', '반기순이익', '당기순이익(손실)', '분기순이익(손실)', '반기순이익(손실)', '당(분)기순이익']:
                                if fs[idx]['thstrm_amount'] == '':
                                    순이익 += 0
                                else:
                                    순이익 += int(fs[idx]['thstrm_amount'])

                        elif fs[idx]['sj_div'] == 'CF':
                            if re.compile('[가-힣]+').findall(fs[idx]['account_nm'].replace(' ', ''))[0] in ['영업현금', '영업활동현금흐름', '영업활동으로인한현금흐름']:
                                if fs[idx]['thstrm_amount'] == '':
                                    영업현금 += 0
                                else:
                                    영업현금 += int(fs[idx]['thstrm_amount'])

                            if re.compile('[가-힣]+').findall(fs[idx]['account_nm'].replace(' ', ''))[0] in ['투자현금', '투자활동현금흐름', '투자활동으로인한현금흐름']:
                                if fs[idx]['thstrm_amount'] == '':
                                    투자현금 += 0
                                else:
                                    투자현금 += int(fs[idx]['thstrm_amount'])

                            if re.compile('[가-힣]+').findall(fs[idx]['account_nm'].replace(' ', ''))[0] in ['재무현금', '재무활동현금흐름', '재무활동으로인한현금흐름']:
                                if fs[idx]['thstrm_amount'] == '':
                                    재무현금 += 0
                                else:
                                    재무현금 += int(fs[idx]['thstrm_amount'])

                    if len(eq) == 0:  # 재무제표 스크랩이 제대로 안된 경우 에러코드 삽입
                        sql = f"UPDATE status.scrap_stock_status SET financial_statements_scraped='10000101' WHERE stock='{stock}'"
                        self.cur.execute(sql)
                        self.conn.commit()
                    else:
                        for idx in range(len(eq)):
                            if eq[idx]['isu_dcrs_de'] == '-':  # 발행일 항목이 없는 경우
                                발행주식수 += 0
                            elif check_date - relativedelta(months=3) < datetime.datetime.strptime(eq[idx]['isu_dcrs_de'].replace('.', ''), '%Y%m%d').date() < check_date:
                                if eq[idx]['isu_dcrs_stle'].replace(' ', '') in ['유상증자', '유상증자(제3자배정)', '유상증자(주주배정)', '전환권행사', '신주인수권행사']:
                                    if eq[idx]['isu_dcrs_stock_knd'].replace(' ', '') in ['보통주']:
                                        if eq[idx]['isu_dcrs_qy'] == '-':
                                            발행주식수 += 0
                                        else:
                                            발행주식수 += int(eq[idx]['isu_dcrs_qy'].replace(',', ''))

                    if len(dd) == 0:  # 재무제표 스크랩이 제대로 안된 경우 에러코드 삽입
                        sql = f"UPDATE status.scrap_stock_status SET financial_statements_scraped='10000101' WHERE stock='{stock}'"
                        self.cur.execute(sql)
                        self.conn.commit()
                    else:
                        for idx in range(len(dd)):
                            if dd[idx]['se'].replace(' ', '') in ['주당현금배당금(원)']:
                                if 'stock_knd' in list(dd[idx].keys()):  # stock_knd 구분자가 있는 경우는 보통주인 경우만 배당금 입력
                                    if dd[idx]['stock_knd'].replace(' ', '') in ['보통주']:
                                        if dd[idx]['thstrm'] == '-':
                                            주당배당금 += 0
                                        else:
                                            주당배당금 += int(dd[idx]['thstrm'].replace(',', ''))
                                else:  # stock_knd 구분자가 없는 경우는 보통주만 있으므로 바로 배당금 입력
                                    if dd[idx]['thstrm'] == '-':
                                        주당배당금 += 0
                                    else:
                                        주당배당금 += int(dd[idx]['thstrm'].replace(',', ''))

                    sql = f"REPLACE INTO financial_statements.`{stock}` (date, " \
                          f"현금, 매출채권, 재고자산, 유동자산, 유형자산, 무형자산, 비유동자산, 자산총계, " \
                          f"매입채무, 유동부채, 이자발생부채, 비유동부채, 부채총계, 자본총계, " \
                          f"매출액, 매출원가, 매출총이익, 판관비, 영업이익, 세전순이익, 순이익, " \
                          f"영업현금, 투자현금, 재무현금, 발행주식수, 주당배당금) VALUES ('{date}', " \
                          f"'{현금}', '{매출채권}', '{재고자산}', '{유동자산}', '{유형자산}', '{무형자산}', '{비유동자산}', '{자산총계}', " \
                          f"'{매입채무}', '{유동부채}', '{이자발생부채}', '{비유동부채}', '{부채총계}', '{자본총계}', " \
                          f"'{매출액}', '{매출원가}', '{매출총이익}', '{판관비}', '{영업이익}', '{세전순이익}', '{순이익}', " \
                          f"'{영업현금}', '{투자현금}', '{재무현금}', '{발행주식수}', '{주당배당금}')"
                    self.cur.execute(sql)
                    self.conn.commit()
        
        # 에러 없이 스크랩 완료시 scrap_stock_status 테이블에 스크랩 체크
        sql = f"SELECT financial_statements_scraped FROM status.scrap_stock_status WHERE stock='{stock}'"
        self.cur.execute(sql)
        check_code = self.cur.fetchone()['financial_statements_scraped']
        if check_code == datetime.date(1000, 1, 1):
            pass
        else:
            sql = f"UPDATE status.scrap_stock_status SET financial_statements_scraped='{self.today}' WHERE stock='{stock}'"
            self.cur.execute(sql)
            self.conn.commit()
        self.logger.info(f"({stock}) 재무제표 스크랩 완료")

    def scrap_financial_statements(self):
        '''전종목 재무제표 스크랩하여 DB저장'''

        # 종목 리스트(stock_list) 가져오기
        sql = "SELECT a.stock, a.wics, b.financial_statements_scraped FROM stock_info.stock_info a " \
              "INNER JOIN status.scrap_stock_status b ON a.stock=b.stock"
        self.cur.execute(sql)
        stock_list = self.cur.fetchall()
        stock_list = pd.DataFrame(stock_list)

        self.logger.info("(전종목) 재무제표 스크랩 시작")
        for idx in range(len(stock_list)):
            stock = stock_list['stock'][idx]
            wics = stock_list['wics'][idx]
            check = stock_list['financial_statements_scraped'][idx]

            # 종목별, 스크랩 상태별 스크랩 실행
            if wics in ['은행', '창업투자', '부동산', '손해보험', '생명보험', '증권', '카드', '기타금융']:  # 스크랩 비대상 종목(90000101) 처리
                sql = f"UPDATE status.scrap_stock_status SET financial_statements_scraped='90000101' WHERE stock='{stock}'"
                self.cur.execute(sql)
                self.conn.commit()
                self.logger.info(f"({idx+1}/{stock}/{wics}) 재무제표 스크랩 비대상 종목")
                continue
            elif check is None:  # 스크랩이 아직 안된 경우 전체 스크랩 실행
                self.scrap_financial_statements_by_start_year_stock(start_year=2016, stock=stock)
                self.logger.info(f"({idx+1}/{stock}) 재무제표 스크랩 완료")
                continue
            elif check == datetime.date(1000, 1, 1):  # 스크랩 에러가 났던 종목(10000101)은 다시 처음부터 스크랩
                self.scrap_financial_statements_by_start_year_stock(start_year=2016, stock=stock)
                self.logger.info(f"({idx+1}/{stock}) 재무제표 스크랩 에러 종목")
                continue
            else:
                this_year = datetime.date.today().year
                self.scrap_financial_statements_by_start_year_stock(start_year=this_year-1, stock=stock)
                self.logger.info(f"({idx+1}/{stock}) 재무제표 스크랩 완료")
                continue
        self.logger.info("(전종목) 재무제표 스크랩 완료")

    def scrap_bug_fix(self):
        sql_list = [
            "UPDATE financial_statements.`국보` SET 순이익=-770578920 WHERE date='20160331'"
        ]
        for sql in sql_list:
            self.cur.execute(sql)
            self.conn.commit()
        pass
        self.logger.info("(전종목) 재무제표 스크랩 버그 픽스 완료")



if __name__ == '__main__':
    scrap_financial_statements = ScrapFinancialStatements()
    scrap_financial_statements.scrap_financial_statements()
    scrap_financial_statements.scrap_bug_fix()