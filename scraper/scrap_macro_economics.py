import os
import time
import pymysql
import datetime
import pandas as pd
from pandas import DataFrame
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
        #print(soup)
        thead = soup.find('thead')
        tbody = soup.find('tbody')
        #print(tbody)
        return thead, tbody

    def data_to_df(self, data_list, df):
        #print(tbody)
        index = len(df)
        for i in range(len(data_list)):
            day = data_list[i][0]
            num = data_list[i][1]
            # print(day)
            # print(day in df[self.df.columns[0]].values)
            if day in df[self.df.columns[0]].values:
                return df, False
            df.at[index, self.df.columns[0]] = day
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
            date = date.replace('.', '-')
            #date = datetime.datetime.strptime(date, "%Y-%m-%d")
            num = num.get_text().strip()
            num = num.replace(',', '')
            num = float(num)
            out_list.append((date, num))
        return out_list

    def read_csv(self, load_path):
        try:
            df = pd.read_csv(load_path, encoding = 'utf-8')
            logger.info("have file : {} ".format(load_path))
            # load 하면 column 이 하나 더 생겨서 삭제함
            df = df.iloc[:, 1:]
            return df
        except:
            logger.info("have not file : {} ".format(load_path))
            return self.df

    def run(self, save_csv):
        logger.info("run start")
        load_path = os.path.join('data', 'csv_file', save_csv)
        df = self.read_csv(load_path)
        # print(df)
        # print(df.columns)
        # print(df.index)
        #425 가 마지막
        # 기본값 1
        n = 10
        # print(df[self.df.columns[0]])
        while(True):
            #print(n)
            try:
                _, tbody = self.get_page_html(self.url, n)
            except Exception as e:
                logger.error(e)
                break
            #print(tbody)
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
        print(df)
        # print(df.columns)
        # print(df.index)
        df.to_csv(os.path.join("data", "csv_file", save_csv))
        logger.info('save csv')
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

    def create_exchange_table(self, tb_name):
        '''종목별 주가 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = '{tb_name}'"
        if self.cur.execute(sql):
            logger.info(f"daily_price.{tb_name} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.`{tb_name}` (" \
                  f"date DATE," \
                  f"rate FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            logger.info(f"macro_economics.{tb_name} 테이블 생성 완료")

    def scrap_USDKRW(self):
        self.create_exchange_table('USDKRW')
        url = "https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_USDKRW"
        df = DataFrame(columns=['day', '매매기준율'])
        pass


if __name__ == '__main__':
    # 달러 환율
    url = "https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_USDKRW"
    df = DataFrame(columns=['day', '매매기준율'])
    nfc = NaverMacroEconomics(url, df)
    save_csv = 'doller.csv'
    nfc.run(save_csv)
    # load_path = os.path.join('data', 'csv_file', 'doller.csv')
    # abs_path = os.path.abspath(load_path)
    # print(abs_path)


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
    
    CD금리 CD : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CD91&page=1   // 날짜, 종가
    콜금리 CALL : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CALL   // 날짜, 종가
    국고채3년 GOVT03Y : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_GOVT03Y   // 날짜, 종가
    회사채3년 CORP03Y : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CORP03Y   // 날짜, 종가
    
    국내금 GOLD : https://finance.naver.com/marketindex/goldDailyQuote.nhn?  // 날짜, 종가(매매기준율)
    국제금 World_Gold: https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=CMDT_GC&fdtc=2   // 날짜, 종가
    두바이유 Dubai_oil: https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_DU&fdtc=2  // 날짜, 종가
    유가WTI WTI : https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_CL&fdtc=2  // 날짜, 종가
    
    구리 Cu : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_CDY&page=1   // 날짜, 종가
    납 Pb : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_PDY&page=1   // 날짜, 종가
    아연 Zn : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_ZDY&page=1   // 날짜, 종가
    니켈 Ni : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_NDY&page=1   // 날짜, 종가
    알루미늄합금 Al : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_AAY&page=1   // 날짜, 종가
    주석 Sn : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SDY&page=1   // 날짜, 종가
    
    옥수수 corn : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_C&page=1    // 날짜, 종가
    대두 soybean : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_S&page=1   // 날짜, 종가
    대두박 soybean_cake : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SM&page=1   // 날짜, 종가
    대두유 soybean_oil: https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_BO&page=1    // 날짜, 종가
    소맥 wheat : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_W&page=1   // 날짜, 종가
    쌀 rice : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_RR&page=1   // 날짜, 종가
    """
