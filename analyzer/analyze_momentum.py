import pymysql
import requests
import time
import pandas as pd
import config.config as cf
from datetime import datetime
from bs4 import BeautifulSoup

class analyze_momentum():
    def __init__(self):
        """생성자"""
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

        # DB초기화
        self.initialize_db()

    def initialize_db(self):
        """DB초기화"""
        # momentum 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'momentum'"
        if self.cur.execute(sql):
            print(f"[{self.now}] momentum 스키마 존재")
        else:
            sql = "CREATE DATABASE momentum"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] momentum 스키마 생성")

    def analyze_momentum(self):
        pass

if __name__=="__main__":
    analyze_momentum = analyze_momentum()
    analyze_momentum.analyze_momentum()