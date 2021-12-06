import os
import time
import pymysql
import datetime
import pandas as pd
from urllib.request import urlopen
from bs4 import BeautifulSoup, NavigableString
from common import logger as logger
from common import config as cf
from common import init_db as init_db
from common import selenium_process as sp

logger = logger.logger

class CrawlerBS4(object):
    """Selenium driver를 이용한 stock index crawler 부분"""
    def __init__(self, driver):
        """driver를 받아 BeautifulSoup 객체를 생성한다."""
        self.html = driver.page_source
        self.soup = BeautifulSoup(self.html, "html.parser")
        logger.info("soup 생성")

    def get_page_html(self):
        try:
            soup = self.soup
            thead = soup.find('thead')
            tbody = soup.findAll('tbody')[1]
        except Exception as error:
            logger.info(error)
            return None, None
        return thead, tbody

    def get_data(self, tbody):
        out_list = []
        for tr in tbody.children:
            if isinstance(tr, NavigableString):
                continue
            day = tr.find('td', {'class' : 'tb_td'})
            day = day.get_text().strip()
            day = day.replace('.', '-')
            open_point = tr.find('td', {'class' : 'tb_td4'})
            open_point = open_point.get_text().strip()
            open_point = open_point.replace(',', '')
            open_point = float(open_point)
            high_point = tr.find('td', {'class' : 'tb_td5'})
            high_point = high_point.get_text().strip()
            high_point = high_point.replace(',', '')
            high_point = float(high_point)
            low_point = tr.find('td', {'class' : 'tb_td6'})
            low_point = low_point.get_text().strip()
            low_point = low_point.replace(',', '')
            low_point = float(low_point)
            close_point = tr.find('td', {'class' : 'tb_td2'})
            close_point = close_point.get_text().strip()
            close_point = close_point.replace(',', '')
            close_point = float(close_point)
            out_list.append((day, open_point, high_point, low_point, close_point))
        return out_list

    def data_to_df(self, df, data_list):
        index = len(df)
        for i in range(len(data_list)):
            date = data_list[i][0]
            open_point = data_list[i][1]
            high_point = data_list[i][2]
            low_point = data_list[i][3]
            close_point = data_list[i][4]
            if date in df[df.columns[0]].values:
                return df, False
            df.at[index, df.columns[0]] = date
            df.at[index, df.columns[1]] = open_point
            df.at[index, df.columns[2]] = high_point
            df.at[index, df.columns[3]] = low_point
            df.at[index, df.columns[4]] = close_point
            index += 1
        return df, True

    def process(self, df):
        _, tbody = self.get_page_html()
        #print(tbody)
        out_list = self.get_data(tbody)
        #print(out_list)
        df, conti = self.data_to_df(df, out_list)
        return df, conti


class WorldStockMarket(object):
    def __init__(self, url, df):
        self.df = df
        self.sp = sp.SeleniumProcess(url)

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

    def run(self, df, num):  # 페이지 수 입력하게 바꾸기
        logger.info("run start")
        #driver 로딩
        driver = self.sp.run_chromedriver()
        self.main_page = driver.current_window_handle
        time.sleep(3)
        # load_path = os.path.join('data', 'csv_file', save_csv)
        # df = self.read_csv(load_path)
        #//*[@id="dayLink1"]
        #//*[@id="dayLink2"]
        #//*[@id="dayPaging"]/a[11]
        #//*[@id="dayPaging"]/a[12]
        #//*[@id="dayPaging"]/a[12]
        max_count_page = 1
        count_page = num
        stop = False
        while(stop == False):
            cbs4 = None
            for n in range(1, 11):
                n = count_page * 10 + n
                page_xpath = '//*[@id="dayLink{}"]'.format(n)
                page = driver.find_element_by_xpath(page_xpath)
                try:
                    page.click()
                except Exception as error:
                    logger.info(error)
                    stop = True
                    break
                time.sleep(3)
                self.main_page = driver.current_window_handle
                time.sleep(3)
                try:
                    cbs4 = CrawlerBS4(driver)
                    time.sleep(3)
                except Exception as error:
                    logger.info(error)
                    stop = True
                    break
                try:
                    df, conti = cbs4.process(df)
                    logger.info('id="dayLink{}" 의 데이터를 DataFrame 으로 저장함'.format(n))
                except Exception as error:
                    logger.info(error)
                    stop = True
                    break
                if not conti:
                    stop = True
                    break
                logger.info('id="dayLink{}" 처리 완료'.format(n))
                time.sleep(3)
                # 여기까지가 for 문 내용
            count_page += 1
            logger.info("{} page 종료".format(count_page))
            if count_page >= max_count_page:
                break
            class_name = 'next'
            next_page = driver.find_element_by_class_name(class_name)
            try:
                next_page.click()
            except Exception as error:
                logger.info(error)
                break
            time.sleep(3)
            self.main_page = driver.current_window_handle
            time.sleep(3)
        df = df.sort_values(by=self.df.columns[0], ascending=False)
        df = df.reindex(range(len(df)))
        self.sp.down_chromedriver(driver)
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

    def create_price_table(self, tb_name):
        '''가격 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'macro_economics' and table_name = '{tb_name}'"
        if self.cur.execute(sql):
            logger.info(f"macro_economics.{tb_name} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS macro_economics.{tb_name} (" \
                  f"date DATE," \
                  f"open BIGINT(20), " \
                  f"high BIGINT(20), " \
                  f"low BIGINT(20), " \
                  f"close BIGINT(20), " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            logger.info(f"macro_economics.{tb_name} 테이블 생성 완료")

    def scrap_DOW(self):
        logger.warning('scrap_DOW 시작')
        self.create_price_table('DOW')
        url = "https://finance.naver.com/world/sise.nhn?symbol=DJI@DJI"
        df = pd.DataFrame(columns = ['date', 'open', 'high', 'low', 'close'])
        wsm = WorldStockMarket(url, df)
        result = wsm.run(df, 0)
        print('result', result)
        try:
            for r in result.itertuples():
                sql = f"REPLACE INTO macro_economics.DOW VALUES ('{r.date}','{r.open}','{r.high}','{r.low}','{r.close}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_DOW 종료')

    def scrap_NASDAQ(self):
        logger.warning('scrap_DNASDAQ 시작')
        self.create_price_table('NASDAQ')
        url = "https://finance.naver.com/world/sise.nhn?symbol=NAS@IXIC"
        df = pd.DataFrame(columns = ['date', 'open', 'high', 'low', 'close'])
        wsm = WorldStockMarket(url, df)
        result = wsm.run(df, 0)
        print('result', result)
        try:
            for r in result.itertuples():
                sql = f"REPLACE INTO macro_economics.NASDAQ VALUES ('{r.date}','{r.open}','{r.high}','{r.low}','{r.close}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_NASDAQ 종료')

    def scrap_SNP500(self):
        logger.warning('scrap_SNP500 시작')
        self.create_price_table('SNP500')
        url = "https://finance.naver.com/world/sise.nhn?symbol=SPI@SPX"
        df = pd.DataFrame(columns = ['date', 'open', 'high', 'low', 'close'])
        wsm = WorldStockMarket(url, df)
        result = wsm.run(df, 0)
        print('result', result)
        try:
            for r in result.itertuples():
                sql = f"REPLACE INTO macro_economics.SNP500 VALUES ('{r.date}','{r.open}','{r.high}','{r.low}','{r.close}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_SNP500 종료')

    def scrap_SHANGHAI(self):
        logger.warning('scrap_SHANGHAI 시작')
        self.create_price_table('SHANGHAI')
        url = "https://finance.naver.com/world/sise.nhn?symbol=SHS@000001"
        df = pd.DataFrame(columns = ['date', 'open', 'high', 'low', 'close'])
        wsm = WorldStockMarket(url, df)
        result = wsm.run(df, 0)
        print('result', result)
        try:
            for r in result.itertuples():
                sql = f"REPLACE INTO macro_economics.SHANGHAI VALUES ('{r.date}','{r.open}','{r.high}','{r.low}','{r.close}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_SHANGHAI 종료')

    def scrap_NIKKEI225(self):
        logger.warning('scrap_NIKKEI225 시작')
        self.create_price_table('NIKKEI225')
        url = "https://finance.naver.com/world/sise.nhn?symbol=NII@NI225"
        df = pd.DataFrame(columns = ['date', 'open', 'high', 'low', 'close'])
        wsm = WorldStockMarket(url, df)
        result = wsm.run(df, 0)
        print('result', result)
        try:
            for r in result.itertuples():
                sql = f"REPLACE INTO macro_economics.NIKKEI225 VALUES ('{r.date}','{r.open}','{r.high}','{r.low}','{r.close}')"
                self.cur.execute(sql)
                self.conn.commit()
        except Exception as e:
            logger.error(e)
        logger.warning('scrap_NIKKEI225 종료')


if __name__ == '__main__':
    scrap_macro_economics = ScrapMacroEconomics()
    scrap_macro_economics.scrap_DOW()
    scrap_macro_economics.scrap_NASDAQ()
    scrap_macro_economics.scrap_SNP500()
    scrap_macro_economics.scrap_SHANGHAI()
    scrap_macro_economics.scrap_NIKKEI225()




    """ 
    다우산업 DOW :  https://finance.naver.com/world/sise.nhn?symbol=DJI@DJI // 일자, 종가, 시가, 고가, 저가
    나스닥종합 NASDAQ : https://finance.naver.com/world/sise.nhn?symbol=NAS@IXIC // 일자, 종가, 시가, 고가, 저가
    S&P500 SNP500 : https://finance.naver.com/world/sise.nhn?symbol=SPI@SPX // 일자, 종가, 시가, 고가, 저가
    상해종합 SHANGHAI : https://finance.naver.com/world/sise.nhn?symbol=SHS@000001 // 일자, 종가, 시가, 고가, 저가
    니케이225 NIKKEI225 : https://finance.naver.com/world/sise.nhn?symbol=NII@NI225 // 일자, 종가, 시가, 고가, 저가
    """
