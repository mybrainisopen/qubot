import pymysql
import time
import pandas as pd
import config.config as cf
from datetime import datetime

class universe_builder():
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
        # universe 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'universe'"
        if self.cur.execute(sql):
            print(f"[{self.now}] universe 스키마 존재")
        else:
            sql = "CREATE DATABASE universe"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] universe 스키마 생성")

    def create_table(self, date):
        """종목별 밸류에이션 테이블 생성 함수"""
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'universe' and table_name = '{date}'"
        if self.cur.execute(sql):
            print(f"[{self.now}] universe.{date} 테이블 존재함")
        else:
            sql = f"CREATE TABLE IF NOT EXISTS universe.`{date}` (" \
                  f"date DATE," \
                  f"PER FLOAT, " \
                  f"PBR FLOAT, " \
                  f"PSR FLOAT, " \
                  f"PCR FLOAT, " \
                  f"PEG FLOAT, " \
                  f"EVEBIT FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"[{self.now}] universe.{date} 테이블 생성 완료")

    def universe_builder_by_date(self, start_date, end_date):
        pass

    def universe_builder(self):
        pass

if __name__=="__main__":
    universe_builder = universe_builder()
    universe_builder.universe_builder()