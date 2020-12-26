import pymysql
import requests
import time
import pandas as pd
import config.config as cf
from datetime import datetime
from bs4 import BeautifulSoup

class scrap_macro_economics():
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
        # macro_economics 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'macro_economics'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] macro_economics 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE macro_economics"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] macro_economics 스키마 생성")

    def scrap_macro_economics(self):
        pass

if __name__ == "__main__":
    scrap_macro_economics = scrap_macro_economics()
    scrap_macro_economics.scrap_macro_economics()