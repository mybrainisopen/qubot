import pymysql
import pandas as pd
import numpy as np
import math
import datetime
import config.setting as cf
import config.db_sql as dbl
from matplotlib import pyplot as plt
from config import logger as logger

class real_investment():
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
        # 데이터프레임 조작시 SettingWithCopyWarning 에러 안 뜨게 처리
        pd.options.mode.chained_assignment = None
        # DB초기화
        self.initialize_db()
        # 변수 설정
        self.tax_rate = cf.tax_rate
        self.fee_rate = cf.fee_rate
        self.slippage = cf.slippage

    def initialize_db(self):
        '''DB초기화'''
        # real_book 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'real_book'"
        if self.cur.execute(sql):
            self.logger.info("real_book 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE real_book"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("real_book 스키마 생성")


"""
21.04.06 매수
퍼스텍 : 2,487 / 90
금호석유 : 245,496 / 2
계룡건설 : 31,632 / 15
대림제지 : 13,614 / 35
삼보판지 : 14,907 / 32
삼천리자전거 : 13,213 / 35
KPX케미칼 : 65,365 / 7
우진플라임 : 3,974 / 120
STX엔진 : 8,327 / 55
인터지스 : 4,555 / 100

"""