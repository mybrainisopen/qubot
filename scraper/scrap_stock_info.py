import pymysql
import requests
import re
import time
import pandas as pd
import config.config as cf
from datetime import datetime
from bs4 import BeautifulSoup

class scrap_stock_info():
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
        # status 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'status'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] status 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE status"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] status 스키마 생성")

        # status.scrap_all_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'scrap_all_status'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] status.scrap_all_status 테이블 존재")
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
            # print(f"[{self.now}] status.scrap_all_status 테이블 생성")

        # status.scrap_stock_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'scrap_stock_status'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] status.scrap_stock_status 테이블 존재")
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
            # print(f"[{self.now}] status.scrap_stock_status 테이블 생성")

        # status.analyze_all_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'analyze_all_status'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] status.analyze_all_status 테이블 존재")
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
            # print(f"[{self.now}] status.analyze_all_status 테이블 생성")

        # status.analyze_stock_status 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'status' and table_name = 'analyze_stock_status'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] status.analyze_stock_status 테이블 존재")
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
            # print(f"[{self.now}] status.analyze_stock_status 테이블 생성")

        # stock_info 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'stock_info'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] stock_info 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE stock_info"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] stock_info 스키마 생성")

        # stock_info.stock_info 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'stock_info' and table_name = 'stock_info'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] stock_info.stock_info 테이블 존재")
            pass
        else:
            sql = """CREATE TABLE stock_info.stock_info (
                     code CHAR(10),
                     stock VARCHAR(50),
                     market CHAR(10),
                     industry VARCHAR(100),
                     wics VARCHAR(100), 
                     ipo DATE, 
                     closing CHAR(5), 
                     face INT(10), 
                     shares BIGINT(20),
                     PRIMARY KEY (code, stock))"""
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] stock_info.stock_info 테이블 생성")

        # stock_info.stock_newlisted 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'stock_info' and table_name = 'stock_newlisted'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] stock_info.stock_newlisted 테이블 존재")
            pass
        else:
            sql = """CREATE TABLE IF NOT EXISTS stock_info.stock_newlisted (
                     code CHAR(10),
                     stock VARCHAR(50), 
                     market CHAR(10),
                     newlisted_date DATE, 
                     PRIMARY KEY (code, stock))"""
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] stock_info.stock_newlisted 테이블 생성")

        # stock_info.stock_delisted 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'stock_info' and table_name = 'stock_delisted'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] stock_info.stock_delisted 테이블 존재")
            pass
        else:
            sql = """CREATE TABLE IF NOT EXISTS stock_info.stock_delisted (
                     code CHAR(10),
                     stock VARCHAR(50), 
                     market CHAR(10),
                     delisted_date DATE, 
                     PRIMARY KEY (code, stock))"""
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] stock_info.stock_delisted 테이블 생성")

        # stock_info.stock_konex 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'stock_info' and table_name = 'stock_konex'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] stock_info.stock_konex 테이블 존재")
            pass
        else:
            sql = """CREATE TABLE IF NOT EXISTS stock_info.stock_konex (
                     code CHAR(10),
                     stock VARCHAR(50), 
                     market CHAR(10),
                     industry VARCHAR(100), 
                     ipo DATE, 
                     closing CHAR(5), 
                     PRIMARY KEY (code, stock))"""
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] stock_info.stock_konex 테이블 생성")

        # stock_info.stock_insincerity 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'stock_info' and table_name = 'stock_insincerity'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] stock_info.stock_insincerity 테이블 존재")
            pass
        else:
            sql = """CREATE TABLE IF NOT EXISTS stock_info.stock_insincerity (
                     code CHAR(10),
                     stock VARCHAR(50), 
                     PRIMARY KEY (code, stock))"""
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] stock_info.stock_insincerity 테이블 생성")

        # stock_info.stock_managing 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'stock_info' and table_name = 'stock_managing'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] stock_info.stock_managing 테이블 존재")
            pass
        else:
            sql = """CREATE TABLE IF NOT EXISTS stock_info.stock_managing (
                     code CHAR(10),
                     stock VARCHAR(50), 
                     PRIMARY KEY (code, stock))"""
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] stock_info.stock_managing 테이블 생성")

    def scrap_stock_info(self):
        '''상장 종목 상세 정보 스크랩'''

        # 종목 리스트(stock_list) 가져오기
        sql = "SELECT code, stock, market from stock_info.stock_info"
        self.cur.execute(sql)
        stock_list = self.cur.fetchall()
        stock_list = pd.DataFrame(stock_list)

        # 코스피(kospi) 종목리스트 가져오기
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt'
        kospi = pd.read_html(url, header=0)[0]
        kospi.종목코드 = kospi.종목코드.map('{:06d}'.format)
        kospi = kospi[['종목코드', '회사명', '업종', '상장일', '결산월']]
        kospi = kospi.rename(columns={'종목코드': 'code', '회사명': 'stock', '업종': 'industry', '상장일': 'ipo', '결산월': 'closing'})
        kospi['market'] = 'KOSPI'
        kospi = kospi[['code', 'stock', 'market', 'ipo', 'closing']]

        # 코스닥(kosdaq) 종목리스트 가져오기
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt'
        kosdaq = pd.read_html(url, header=0)[0]
        kosdaq.종목코드 = kosdaq.종목코드.map('{:06d}'.format)
        kosdaq = kosdaq[['종목코드', '회사명', '업종', '상장일', '결산월']]
        kosdaq = kosdaq.rename(columns={'종목코드': 'code', '회사명': 'stock', '업종': 'industry', '상장일': 'ipo', '결산월': 'closing'})
        kosdaq['market'] = 'KOSDAQ'
        kosdaq = kosdaq[['code', 'stock', 'market', 'ipo', 'closing']]

        # 코스피, 코스닥 종목리스트를 상장 종목 리스트(krx) 데이터프레임으로 합치기
        krx = pd.concat([kospi, kosdaq], ignore_index=True)
        krx = krx.sort_values(by='code')
        krx = krx.reset_index(drop=True)

        # 신규상장종목/상장폐지종목 체크
        if stock_list.empty:  # 첫 실행시 패스 (DB 테이블이 비어있는 경우)
            print(f"[{self.now}] (KRX) 데이터베이스 신규 구축 시작")
            pass
        else:  # 재실행시 krx(KIND)와 stock_list(DB)를 비교하여 신규상장종목/상장폐지종목 체크하여 업데이트
            # krx(KIND)에는 있지만 stock_list(DB)에는 없는 신규상장종목 stock_info(추가) / stock_newlisted(추가) 업데이트
            print(f"[{self.now}] (KRX신규상장종목) 업데이트 체크 시작")
            for idx in range(len(krx)):
                code = krx['code'][idx]
                stock = krx['stock'][idx]
                market = krx['market'][idx]
                sql = f"SELECT code FROM stock_info.stock_info WHERE code='{code}'"
                self.cur.execute(sql)
                res = self.cur.fetchone()
                if res is not None:  # 양쪽 다 종목이 있는 경우 Pass
                    pass
                else:  # krx(KIND)에는 있지만 stock_list(DB)에는 없는 신규상장종목은 스크랩 실행
                    # stock_info DB 입력
                    sql = f"INSERT IGNORE INTO stock_info.stock_info (code, stock) " \
                          f"VALUES ('{code}', '{stock}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    # scrap_stock_status DB 입력
                    sql = f"INSERT IGNORE INTO status.scrap_stock_status (code, stock) " \
                          f"VALUES ('{code}', '{stock}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    # analyze_stock_status DB 입력
                    sql = f"INSERT IGNORE INTO status.analyze_stock_status (code, stock) " \
                          f"VALUES ('{code}', '{stock}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    # stock_newlisted DB 입력
                    sql = f"INSERT IGNORE INTO stock_info.stock_newlisted (code, stock, market, newlisted_date) " \
                          f"VALUES ('{code}', '{stock}', '{market}', '{self.today}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    time.sleep(0.1)
                    print(f"[{self.now}] (KRX신규상장종목) {stock} 스크랩 완료")
            print(f"[{self.now}] (KRX신규상장종목) 업데이트 체크 종료")

            # stock_list(DB)에는 있지만 krx(KIND)에는 없는 상장폐지종목 stock_info(삭제) / stock_delisted(추가) 업데이트
            print(f"[{self.now}] (KRX상장폐지종목) 업데이트 체크 시작")
            for idx in range(len(stock_list)):
                code = stock_list['code'][idx]
                stock = stock_list['stock'][idx]
                market = stock_list['market'][idx]
                res = krx[krx['code'] == code]
                if res is not None:  # 양쪽 다 종목이 있는 경우
                    pass
                else:  # stock_list(DB)에 있지만 krx(KIND)에 없는 상장폐지 종목
                    # stock_info(DB)에서 삭제
                    sql = f"DELETE FROM stock_info.stock_info WHERE code = {code}"
                    self.cur.execute(sql)
                    self.conn.commit()
                    # scrap_stock_status(DB)에서 삭제
                    sql = f"DELETE FROM status.scrap_stock_status WHERE code = {code}"
                    self.cur.execute(sql)
                    self.conn.commit()
                    # analyze_stock_status(DB)에서 삭제
                    sql = f"DELETE FROM status.analyze_stock_status WHERE code = {code}"
                    self.cur.execute(sql)
                    self.conn.commit()
                    # stock_delisted(DB)에 입력
                    sql = f"INSERT IGNORE INTO stock_info.stock_newlisted (code, stock, market, delisted_date) " \
                          f"VALUES ('{code}', '{stock}', '{market}', '{self.today}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    time.sleep(0.1)
                    print(f"[{self.now}] (KRX상장폐지종목) {stock} 스크랩 완료")
            print(f"[{self.now}] (KRX상장폐지종목) 업데이트 체크 종료")
        # 참고 : 변경상장(회사명 변경)은 테이블만 새로 만들며 별도 처리 안함

        # krx 종목 정보 스크랩
        for idx in range(len(krx)):
            code = krx['code'][idx]
            stock = krx['stock'][idx]
            market = krx['market'][idx]
            ipo = krx['ipo'][idx]
            closing = krx['closing'][idx][:2]
            sql = f"SELECT stock_info_scraped FROM status.scrap_stock_status WHERE code={code}"
            self.cur.execute(sql)
            check_date = self.cur.fetchone()
            check_date = list(check_date.values())[0]
            if str(check_date) == self.today:
                print(f"[{self.now}] ({idx:04d}/{len(krx)}) {stock} 이미 스크랩 완료됨")
            else:
                try:
                    # 스크랩 세팅
                    url = f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={code}'
                    webpage = requests.get(url, headers=self.headers)
                    soup = BeautifulSoup(webpage.content, 'html.parser')
                    # industry, wics, shares, face 스크래핑
                    industry = soup.find_all("dt", class_="line-left")[8].get_text().split(':')[1].strip()
                    wics = soup.find_all("dt", class_="line-left")[9].get_text().split(':')[1].strip()
                    shares = soup.find_all("td", class_="num")[6].get_text().strip().split("/")[0].replace(",", "")
                    shares = int(re.findall("\d+", shares)[0])
                    face = soup.find_all("td", class_="num")[2].get_text().strip().replace(",", "")
                    face = int(re.findall("\d+", face)[0])
                    # DB 입력 (stock_info, scrap_stock_status, analyze_stock_status)
                    sql = f"REPLACE INTO stock_info.stock_info (code, stock, market, industry, wics, ipo, closing, face, shares) " \
                          f"VALUES ('{code}', '{stock}', '{market}', '{industry}', '{wics}', '{ipo}', '{closing}', '{face}', '{shares}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    sql = f"INSERT IGNORE INTO status.scrap_stock_status (code, stock) " \
                          f"VALUES ('{code}', '{stock}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    sql = f"INSERT IGNORE INTO status.analyze_stock_status (code, stock) " \
                          f"VALUES ('{code}', '{stock}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    sql = f"UPDATE status.scrap_stock_status SET stock_info_scraped='{self.today}' WHERE code='{code}'"
                    self.cur.execute(sql)
                    self.conn.commit()
                    time.sleep(0.1)
                    print(f"[{self.now}] ({idx:04d}/{len(krx)}) {stock} 스크랩 완료")
                except Exception as e:
                    print(f"[{self.now}] ({idx:04d}/{len(krx)}) {stock} 스크랩 에러: {str(e)}")

    def scrap_stock_konex(self):
        '''코스피 종목 정보 스크랩'''

        # 종목 리스트(stock_list) 가져오기
        sql = "SELECT code, stock from stock_info.stock_konex"
        self.cur.execute(sql)
        stock_list = self.cur.fetchall()
        stock_list = pd.DataFrame(stock_list)

        # 코넥스(konex) 종목리스트 가져오기
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=konexMkt'
        konex = pd.read_html(url, header=0)[0]
        konex.종목코드 = konex.종목코드.map('{:06d}'.format)
        konex = konex[['종목코드', '회사명', '업종', '상장일', '결산월']]
        konex = konex.rename(columns={'종목코드': 'code', '회사명': 'stock', '업종': 'industry', '상장일': 'ipo', '결산월': 'closing'})

        # 신규상장종목/상장폐지종목 체크
        if stock_list.empty:  # 첫 실행시 DB 테이블이 비어있는 경우
            print(f"[{self.now}] (KONEX) 데이터베이스 신규 구축 시작")
        else:  # 재실행시 konex(KIND)와 stock_list(DB)를 비교하여 신규상장종목/상장폐지종목 체크하여 업데이트
            # konex(KIND)에는 있지만 stock_list(DB)에는 없는 신규상장종목 stock_konex(추가) / stock_newlisted(추가) 업데이트
            print(f"[{self.now}] (KONEX신규상장종목) 업데이트 체크 시작")
            for idx in range(len(konex)):
                code = konex['code'][idx]
                sql = f"SELECT code FROM stock_info.stock_konex WHERE code='{code}'"
                self.cur.execute(sql)
                res = self.cur.fetchone()
                if res is not None:  # 양쪽 다 종목이 있는 경우 Pass
                    pass
                else:  # konex(KIND)에는 있지만 stock_list(DB)에는 없는 신규상장종목은 스크랩 실행
                    stock = konex['stock'][idx]
                    market = 'KONEX'
                    ipo = konex['ipo'][idx]
                    industry = konex['industry'][idx].strip().replace(' ', '')
                    closing = konex['closing'][idx][:2]
                    # stock_konex DB 입력
                    sql = f"REPLACE INTO stock_info.stock_konex (code, stock, market, industry, ipo, closing) " \
                          f"VALUES ('{code}', '{stock}', '{market}', '{industry}', '{ipo}', '{closing}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    # stock_newlisted DB 입력
                    sql = f"REPLACE INTO stock_info.stock_newlisted (code, stock, market, newlisted_date) " \
                          f"VALUES ('{code}', '{stock}', '{market}', '{self.today}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    time.sleep(0.1)
                    print(f"[{self.now}] (KONEX신규상장종목) {stock} 스크랩 완료")
            print(f"[{self.now}] (KONEX신규상장종목) 업데이트 체크 종료")
            # stock_list(DB)에는 있지만 krx(KIND)에는 없는 상장폐지종목 stock_konex(삭제) / stock_delisted(추가) 업데이트
            print(f"[{self.now}] (KONEX상장폐지종목) 업데이트 체크 시작")
            for idx in range(len(stock_list)):
                code = stock_list['code'][idx]
                res = konex[konex['code'] == code]
                if res is not None:  # 양쪽 다 종목이 있는 경우
                    pass
                else:  # status에는 있지만 stock_list에는 없는 경우 = 상장폐지 종목
                    stock = konex['stock'][idx]
                    market = konex['market'][idx]
                    # stock_konex DB 삭제
                    sql = f"DELETE FROM stock_info.stock_konex WHERE code = {code}"
                    self.cur.execute(sql)
                    self.conn.commit()
                    # stock_delisted DB 입력
                    sql = f"REPLACE INTO stock_info.stock_delisted (code, stock, market, delisted_date) " \
                          f"VALUES ('{code}', '{stock}', '{market}', '{self.today}')"
                    self.cur.execute(sql)
                    self.conn.commit()
                    time.sleep(0.1)
                    print(f"[{self.now}] (KONEX상장폐지 종목) {stock} 스크랩 완료")
            print(f"[{self.now}] (KONEX상장폐지종목) 업데이트 체크 종료")

        # konex 종목 정보 스크렙
        for idx in range(len(konex)):
            code = konex['code'][idx]
            stock = konex['stock'][idx]
            market = 'KONEX'
            industry = konex['industry'][idx].strip().replace(' ', '')
            ipo = konex['ipo'][idx]
            closing = konex['closing'][idx][:2]
            sql = f"REPLACE INTO stock_info.stock_konex (code, stock, market, industry, ipo, closing) " \
                  f"VALUES ('{code}', '{stock}', '{market}', '{industry}', '{ipo}', '{closing}')"
            self.cur.execute(sql)
            self.conn.commit()
            time.sleep(0.1)
            print(f"[{self.now}] ({idx:03d}/{len(konex)}) {stock} 스크랩 완료")

    def scrap_stock_insincerity(self):
        '''불성실공시법인 스크랩'''
        # 불성실공시법인 리스트 가져오기
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=05'
        insincerity = pd.read_html(url, header=0)[0]
        insincerity.종목코드 = insincerity.종목코드.map('{:06d}'.format)
        insincerity = insincerity[['종목코드', '회사명']]
        insincerity = insincerity.rename(columns={'종목코드': 'code', '회사명': 'stock'})

        for idx in range(len(insincerity)):
            code = insincerity['code'][idx]
            stock = insincerity['stock'][idx]
            sql = f"REPLACE INTO stock_info.stock_insincerity (code, stock) " \
                  f"VALUES ('{code}', '{stock}')"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] ({idx+1:02d}/{len(insincerity)}) 불성실공시법인 스크랩 완료")


    def scrap_stock_managing(self):
        '''관리종목 스크랩'''
        # 관리종목 리스트 가져오기
        url = 'http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=01'
        managing = pd.read_html(url, header=0)[0]
        managing.종목코드 = managing.종목코드.map('{:06d}'.format)
        managing = managing[['종목코드', '회사명']]
        managing = managing.rename(columns={'종목코드': 'code', '회사명': 'stock'})

        for idx in range(len(managing)):
            code = managing['code'][idx]
            stock = managing['stock'][idx]
            sql = f"REPLACE INTO stock_info.stock_managing (code, stock) " \
                  f"VALUES ('{code}', '{stock}')"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] ({idx+1:03d}/{len(managing)}) 관리종목 스크랩 완료")


if __name__ == '__main__':
    scrap_stock_info = scrap_stock_info()
    scrap_stock_info.scrap_stock_info()
    scrap_stock_info.scrap_stock_konex()
    scrap_stock_info.scrap_stock_insincerity()
    scrap_stock_info.scrap_stock_managing()


'''
모든상장종목 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13
코스닥 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt
코스피 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt
코넥스 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=konexMkt
관리종목 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=01
불성실공시법인 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=05
'''