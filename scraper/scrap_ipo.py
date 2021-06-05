import selenium
import pymysql
import requests
import time
import pandas as pd
from config import setting as cf
import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from config import logger as logger
logger = logger.logger

# selenium을 이용해서 IPO 스크랩하게..

class scrap_ipo():
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
        self.now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        self.today = datetime.date.today()
        self.headers = {'User-Agent': cf.user_agent}
        self.driver = webdriver.Chrome('C:\\Project\\qubot\\chromedriver.exe')
        # DB초기화
        self.initialize_db()


    def initialize_db(self):
        '''DB 초기화'''
        # ipo 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'ipo'"
        if self.cur.execute(sql):
            print(f"[{self.now}] ipo 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE ipo"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] ipo 스키마 생성")

    def scrap_prep_ipo(self):
        self.driver.get('https://kind.krx.co.kr/listinvstg/listinvstgcom.do?method=searchListInvstgCorpMain')
        self.driver.find_element_by_xpath('/html/body/section[2]/section/article/section[1]/table/tbody/tr[1]/td[1]').click()


if __name__ == '__main__':
    scrap_ipo = scrap_ipo()
    scrap_ipo.scrap_prep_ipo()


# 'https://kind.krx.co.kr/listinvstg/listinvstgcom.do?method=searchListInvstgCorpMain')  # 예비심사기업
# https://kind.krx.co.kr/listinvstg/pubofrprogcom.do?method=searchPubofrProgComMain  # 공모기업
# https://kind.krx.co.kr/listinvstg/listingcompany.do?method=searchListingTypeMain  # 신규상장기업