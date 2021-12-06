import time
import pymysql
import datetime
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup, NavigableString
from common import logger as logger
from common import config as cf
from common import init_db as init_db
logger = logger.logger

class NaverMacroEconomics(object):
    def __init__(self, url, df):
        self.url = url
        self.df = df

    def get_page_html(self, url, n):
        url = url + "&page={}".format(n)
        logger.info(url)
        try:
            html = urlopen(url)
        except Exception as error:
            logger.info(error)
            return None, None
        soup = BeautifulSoup(html.read(), 'html.parser')
        thead = soup.find('thead')
        tbody = soup.find('tbody')
        return thead, tbody

    def data_to_df(self, data_list, df):
        index = len(df)
        for i in range(len(data_list)):
            date = data_list[i][0]
            num = data_list[i][1]
            if date in df[self.df.columns[0]].values:
                return df, False
            df.at[index, self.df.columns[0]] = date
            df.at[index, self.df.columns[1]] = num
            index += 1
        return df, True

    def tbody_data(self, tbody):
        out_list = []
        for tr in tbody.children:
            if isinstance(tr, NavigableString):
                continue
            date = tr.find('td' , {'class' : 'date'})
            num = tr.find('td', {'class' : 'num'})
            date = date.get_text().strip()
            date = date.replace('.', '')
            num = num.get_text().strip()
            num = num.replace(',', '')
            num = float(num)
            out_list.append((date, num))
        return out_list

    def run(self, df, n):  # 페이지수 바꾸기
        logger.info("run start")
        # df = pd.DataFrame(columns=['date', 'rate'])
        #425 가 마지막
        while(True):
            try:
                _, tbody = self.get_page_html(self.url, n)
            except Exception as e:
                logger.error(e)
                break
            out_list = self.tbody_data(tbody)
            # 더 이상 데이터가 없으면 중지함
            if len(out_list) < 1:
                break
            df, conti = self.data_to_df(out_list, df)
            if not conti:
                break
            time.sleep(0.5)
            n += 1
            # 만들때 테스트 할 때만, 그 외는 주석 처리
            if n > 20:
                break

        df = df.sort_values(by=self.df.columns[0], ascending=False)
        df = df.reindex(range(len(df)))
        return df

class ScrapMacroEconomics(object):
    def __init__(self):
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

    def create_rate_table(self, tb_name):
        '''환율 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = '{tb_name}'"
        if self.cur.execute(sql):
            logger.info(f"macro_economics.{tb_name} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`{tb_name}` (" \
                  f"date DATE," \
                  f"rate FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            logger.info(f"macro_economics.{tb_name} 테이블 생성 완료")

    def create_price_table(self, tb_name):
        '''가격 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = '{tb_name}'"
        if self.cur.execute(sql):
            logger.info(f"macro_economics.{tb_name} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`{tb_name}` (" \
                  f"date DATE," \
                  f"price FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            logger.info(f"macro_economics.{tb_name} 테이블 생성 완료")

    def scrap_USDKRW(self, n):
        logger.warning('scrap_USDKRW 시작')
        self.create_rate_table('USDKRW')
        url = "https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_USDKRW"
        df = pd.DataFrame(columns=['date', 'rate'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.USDKRW VALUES ('{r.date}', '{r.rate}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_USDKRW 종료')

    def scrap_EURKRW(self, n):
        logger.warning('scrap_EURKRW 시작')
        self.create_rate_table('EURKRW')
        url = "https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_EURKRW"
        df = pd.DataFrame(columns=['date', 'rate'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.EURKRW VALUES ('{r.date}', '{r.rate}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_EURKRW 종료')

    def scrap_JPYKRW(self, n):
        logger.warning('scrap_JPYKRW 시작')
        self.create_rate_table('JPYKRW')
        url = "https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_JPYKRW"
        df = pd.DataFrame(columns=['date', 'rate'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.JPYKRW VALUES ('{r.date}', '{r.rate}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_JPYKRW 종료')

    def scrap_CD91(self, n):
        logger.warning('scrap_CD91 시작')
        self.create_rate_table('CD91')
        url = "https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CD91"
        df = pd.DataFrame(columns=['date', 'rate'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.CD91 VALUES ('{r.date}', '{r.rate}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_CD91 종료')

    def scrap_CALL(self, n):
        logger.warning('scrap_CALL 시작')
        self.create_rate_table('CALL')
        url = "https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CALL"
        df = pd.DataFrame(columns=['date', 'rate'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.CALL VALUES ('{r.date}', '{r.rate}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_CALL 종료')

    def scrap_GOVT03Y(self, n):
        logger.warning('scrap_GOVT03Y 시작')
        self.create_rate_table('GOVT03Y')
        url = "https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_GOVT03Y"
        df = pd.DataFrame(columns=['date', 'rate'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.GOVT03Y VALUES ('{r.date}', '{r.rate}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_GOVT03Y 종료')

    def scrap_CORP03Y(self, n):
        logger.warning('scrap_CORP03Y 시작')
        self.create_rate_table('CORP03Y')
        url = "https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CORP03Y"
        df = pd.DataFrame(columns=['date', 'rate'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.CORP03Y VALUES ('{r.date}', '{r.rate}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_CORP03Y 종료')

    def scrap_KOREA_GOLD(self, n):
        logger.warning('scrap_KOREA_GOLD 시작')
        self.create_price_table('KOREA_GOLD')
        url = "https://finance.naver.com/marketindex/goldDailyQuote.nhn?"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.KOREA_GOLD VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_KOREA_GOLD 종료')

    def scrap_WORLD_GOLD(self, n):
        logger.warning('scrap_WORLD_GOLD 시작')
        self.create_price_table('WORLD_GOLD')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=CMDT_GC&fdtc=2"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.WORLD_GOLD VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_WORLD_GOLD 종료')


    def scrap_DUBAI_OIL(self, n):
        logger.warning('scrap_DUBAI_OIL 시작')
        self.create_price_table('DUBAI_OIL')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_DU&fdtc=2"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.DUBAI_OIL VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_DUBAI_OIL 종료')

    def scrap_WTI(self, n):
        logger.warning('scrap_WTI 시작')
        self.create_price_table('WTI')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_CL&fdtc=2"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.WTI VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_WTI 종료')

    def scrap_CU(self, n):
        logger.warning('scrap_CU 시작')
        self.create_price_table('CU')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_CDY"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.CU VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_CU 종료')

    def scrap_PB(self, n):
        logger.warning('scrap_PB 시작')
        self.create_price_table('PB')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_PDY"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.PB VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_PB 종료')

    def scrap_ZN(self, n):
        logger.warning('scrap_ZN 시작')
        self.create_price_table('ZN')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_ZDY"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.ZN VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_ZN 종료')

    def scrap_NI(self, n):
        logger.warning('scrap_NI 시작')
        self.create_price_table('NI')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_NDY"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.NI VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_NI 종료')

    def scrap_AL(self, n):
        logger.warning('scrap_AL 시작')
        self.create_price_table('AL')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_AAY"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.AL VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_AL 종료')

    def scrap_SN(self, n):
        logger.warning('scrap_SN 시작')
        self.create_price_table('SN')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SDY"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.SN VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_SN 종료')

    def scrap_corn(self, n):
        logger.warning('scrap_corn 시작')
        self.create_price_table('corn')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_C"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.corn VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_corn 종료')

    def scrap_soybean(self, n):
        logger.warning('scrap_soybean 시작')
        self.create_price_table('soybean')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_S"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.soybean VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_soybean 종료')

    def scrap_soybean_cake(self, n):
        logger.warning('scrap_soybean_cake 시작')
        self.create_price_table('soybean_cake')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SM"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.soybean_cake VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_soybean_cake 종료')

    def scrap_soybean_oil(self, n):
        logger.warning('scrap_soybean_oil 시작')
        self.create_price_table('soybean_oil')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_BO"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.soybean_oil VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_soybean_oil 종료')

    def scrap_wheat(self, n):
        logger.warning('scrap_wheat 시작')
        self.create_price_table('wheat')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_W"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.wheat VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_wheat 종료')

    def scrap_rice(self, n):
        logger.warning('scrap_rice 시작')
        self.create_price_table('rice')
        url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_RR"
        df = pd.DataFrame(columns=['date', 'price'])
        nfc = NaverMacroEconomics(url, df)
        df = nfc.run(df, n)
        try:
            for r in df.itertuples():
                sql = f"REPLACE INTO macro_economics.rice VALUES ('{r.date}', '{r.price}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_rice 종료')



if __name__ == '__main__':
    scrap_macro_economics = ScrapMacroEconomics()
    
    # 달러, 유로, 엔 환율
    # scrap_macro_economics.scrap_USDKRW(10)
    # scrap_macro_economics.scrap_EURKRW(10)
    # scrap_macro_economics.scrap_JPYKRW(10)

    # 금리, 채권
    # scrap_macro_economics.scrap_CD91(10)
    # scrap_macro_economics.scrap_CALL(10)
    # scrap_macro_economics.scrap_GOVT03Y(10)
    # scrap_macro_economics.scrap_CORP03Y(10)
    
    # 금, 유가
    # scrap_macro_economics.scrap_KOREA_GOLD(10)
    # scrap_macro_economics.scrap_WORLD_GOLD(10)
    # scrap_macro_economics.scrap_DUBAI_OIL(10)
    # scrap_macro_economics.scrap_WTI(10)

    # 금속
    # scrap_macro_economics.scrap_CU(10)
    # scrap_macro_economics.scrap_PB(10)
    # scrap_macro_economics.scrap_ZN(10)
    # scrap_macro_economics.scrap_NI(10)
    # scrap_macro_economics.scrap_AL(10)
    # scrap_macro_economics.scrap_SN(10)

    # 상품
    scrap_macro_economics.scrap_corn(10)
    scrap_macro_economics.scrap_soybean(10)
    scrap_macro_economics.scrap_soybean_cake(10)
    scrap_macro_economics.scrap_soybean_oil(10)
    scrap_macro_economics.scrap_wheat(10)
    scrap_macro_economics.scrap_rice(10)


    """ 
    모든상장종목 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13
    코스닥 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt
    코스피 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt
    코넥스 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=konexMkt
    관리종목 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=01
    불성실공시법인 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=05
    
    다우산업 DOW :  https://finance.naver.com/world/sise.nhn?symbol=DJI@DJI // 일자, 종가, 시가, 고가, 저가
    나스닥종합 NASDAQ : https://finance.naver.com/world/sise.nhn?symbol=NAS@IXIC // 일자, 종가, 시가, 고가, 저가
    S&P500 SNP500 : https://finance.naver.com/world/sise.nhn?symbol=SPI@SPX // 일자, 종가, 시가, 고가, 저가
    상해종합 SHANGHAI : https://finance.naver.com/world/sise.nhn?symbol=SHS@000001 // 일자, 종가, 시가, 고가, 저가
    니케이225 NIKKEI225 : https://finance.naver.com/world/sise.nhn?symbol=NII@NI225 // 일자, 종가, 시가, 고가, 저가
    
    원달러 USDKRW : https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_USDKRW   // 날짜, 매매기준율
    원유로 EURKRW : https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_EURKRW   // 날짜, 매매기준율
    원엔 JPYKRW : https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_JPYKRW     // 날짜, 매매기준율
    
    CD금리 CD : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CD91   // 날짜, 종가
    콜금리 CALL : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CALL   // 날짜, 종가
    국고채3년 GOVT03Y : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_GOVT03Y   // 날짜, 종가
    회사채3년 CORP03Y : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CORP03Y   // 날짜, 종가
    
    국내금 KOREA_GOLD : https://finance.naver.com/marketindex/goldDailyQuote.nhn?  // 날짜, 종가(매매기준율)
    국제금 WORLD_GOLD: https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=CMDT_GC&fdtc=2   // 날짜, 종가
    두바이유 DUBAI_OIL : https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_DU&fdtc=2  // 날짜, 종가
    유가WTI WTI : https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_CL&fdtc=2  // 날짜, 종가
    
    구리 Cu : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_CDY   // 날짜, 종가
    납 Pb : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_PDY   // 날짜, 종가
    아연 Zn : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_ZDY   // 날짜, 종가
    니켈 Ni : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_NDY   // 날짜, 종가
    알루미늄합금 Al : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_AAY   // 날짜, 종가
    주석 Sn : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SDY   // 날짜, 종가
    
    옥수수 corn : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_C   // 날짜, 종가
    대두 soybean : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_S   // 날짜, 종가
    대두박 soybean_cake : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SM   // 날짜, 종가
    대두유 soybean_oil: https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_BO    // 날짜, 종가
    소맥 wheat : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_W   // 날짜, 종가
    쌀 rice : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_RR   // 날짜, 종가
    """
