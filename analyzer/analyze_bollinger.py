import matplotlib.pyplot as plt
import pymysql
import pandas as pd
import datetime
from dateutil.relativedelta import relativedelta
from common import config as cf
from common import logger as logger
from common import init_db as init_db

class AnalyzeBollinger():
    def __init__(self):
        '''생성자 : 기본 변수 생성'''
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
        # DB초기화
        self.init_db = init_db.InitDB()
        # 데이터프레임 조작시 SettingWithCopyWarning 에러 안 뜨게 처리
        pd.options.mode.chained_assignment = None

    def create_table(self, stock):
        '''종목별 볼린져밴드 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'bollinger' and table_name = '{stock}'"
        if self.cur.execute(sql):
            self.logger.info(f"bollinger.{stock} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS bollinger.`{stock}` (" \
                  f"date DATE," \
                  f"open BIGINT(20), " \
                  f"high BIGINT(20), " \
                  f"low BIGINT(20), " \
                  f"close BIGINT(20), " \
                  f"volume BIGINT(20), " \
                  f"MA20 FLOAT, " \
                  f"stddev FLOAT, " \
                  f"upper FLOAT, " \
                  f"lower FLOAT, " \
                  f"PB FLOAT, " \
                  f"TP FLOAT, " \
                  f"PMF FLOAT, " \
                  f"NMF FLOAT, " \
                  f"MFR FLOAT, " \
                  f"MFI10 FLOAT, " \
                  f"II FLOAT, " \
                  f"IIP21 FLOAT, " \
                  f"trend CHAR(10), " \
                  f"reversal CHAR(10), " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"bollinger.{stock} 테이블 생성 완료")

    def analyze_bollinger_by_date_stock(self, start_date, end_date, stock):
        ''' 종목별 볼린져 밴드 계산 및 DB입력'''
        # 볼린져밴드 테이블 생성
        self.create_table(stock=stock)

        # 날짜 가져오기
        sql = f"SELECT date FROM market_index.kospi"
        self.cur.execute(sql)
        date_df = self.cur.fetchall()
        date_df = pd.DataFrame(date_df)
        print(date_df)

        # # 주가 리스트 가져오기
        # sql = f"SELECT * FROM daily_price.`{stock}` WHERE date BETWEEN {start_date} AND {end_date}"
        # self.cur.execute(sql)
        # df = self.cur.fetchall()
        # df = pd.DataFrame(df)
        #
        # df['MA20'] = df['close'].rolling(window=20).mean()
        # df['stddev'] = df['close'].rolling(window=20).std()
        # df['upper'] = df['MA20'] + (df['stddev'] * 2)
        # df['lower'] = df['MA20'] - (df['stddev'] * 2)
        # df['PB'] = (df['close'] - df['lower']) / (df['upper'] - df['lower'])
        # df['TP'] = (df['high'] + df['low'] + df['close']) / 3
        # df['PMF'] = 0
        # df['NMF'] = 0
        # for i in range(len(df.close)-1):
        #     if df.TP.values[i] < df.TP.values[i+1]:
        #         df.PMF.values[i+1] = df.TP.values[i+1] * df.volume.values[i+1]
        #         df.NMF.values[i+1] = 0
        #     else:
        #         df.NMF.values[i+1] = df.TP.values[i+1] * df.volume.values[i+1]
        #         df.PMF.values[i+1] = 0
        # df['MFR'] = (df.PMF.rolling(window=10).sum() /
        #     df.NMF.rolling(window=10).sum())
        # df['MFI10'] = 100 - 100 / (1 + df['MFR'])
        # # df = df[19:]
        # df['II'] = (2 * df['close'] - df['high'] - df['low']) / (df['high'] - df['low']) * df['volume']
        # df['IIP21'] = df['II'].rolling(window=21).sum() / df['volume'].rolling(window=21).sum() * 100
        # df = df.dropna()
        #
        # # Trend
        # plt.figure(figsize=(9, 8))
        # plt.subplot(2, 1, 1)
        # plt.title(f'{stock} Bollinger Band(20 day, 2 std) - Trend Following')
        # plt.plot(df.index, df['close'], color='#0000ff', label='Close')
        # plt.plot(df.index, df['upper'], 'r--', label='Upper band')
        # plt.plot(df.index, df['MA20'], 'k--', label='Moving average 20')
        # plt.plot(df.index, df['lower'], 'c--', label='Lower band')
        # plt.fill_between(df.index, df['upper'], df['lower'], color='0.9')
        # for i in range(len(df.close)):
        #     if df.PB.values[i] > 0.8 and df.MFI10.values[i] > 80:
        #         plt.plot(df.index.values[i], df.close.values[i], 'r^')
        #     elif df.PB.values[i] < 0.2 and df.MFI10.values[i] < 20:
        #         plt.plot(df.index.values[i], df.close.values[i], 'bv')
        # plt.legend(loc='best')
        #
        # plt.subplot(2, 1, 2)
        # plt.plot(df.index, df['PB'] * 100, 'b', label='%B x 100')
        # plt.plot(df.index, df['MFI10'], 'g--', label='MFI(10 day)')
        # plt.yticks([-20, 0, 20, 40, 60, 80, 100, 120])
        # for i in range(len(df.close)):
        #     if df.PB.values[i] > 0.8 and df.MFI10.values[i] > 80:
        #         plt.plot(df.index.values[i], 0, 'r^')
        #     elif df.PB.values[i] < 0.2 and df.MFI10.values[i] < 20:
        #         plt.plot(df.index.values[i], 0, 'bv')
        # plt.grid(True)
        # plt.legend(loc='best')
        # plt.show()
        #
        # # Reversals
        # plt.figure(figsize=(9, 9))
        # plt.subplot(3, 1, 1)
        # plt.title(f'{stock} Bollinger Band(20 day, 2 std) - Reversals')
        # plt.plot(df.index, df['close'], 'm', label='Close')
        # plt.plot(df.index, df['upper'], 'r--', label='Upper band')
        # plt.plot(df.index, df['MA20'], 'k--', label='Moving average 20')
        # plt.plot(df.index, df['lower'], 'c--', label='Lower band')
        # plt.fill_between(df.index, df['upper'], df['lower'], color='0.9')
        # for i in range(0, len(df.close)):
        #     if df.PB.values[i] < 0.05 and df.IIP21.values[i] > 0:
        #         plt.plot(df.index.values[i], df.close.values[i], 'r^')
        #     elif df.PB.values[i] > 0.95 and df.IIP21.values[i] < 0:
        #         plt.plot(df.index.values[i], df.close.values[i], 'bv')
        #
        # plt.legend(loc='best')
        # plt.subplot(3, 1, 2)
        # plt.plot(df.index, df['PB'], 'b', label='%b')
        # plt.grid(True)
        # plt.legend(loc='best')
        #
        # plt.subplot(3, 1, 3)
        # plt.bar(df.index, df['IIP21'], color='g', label='II% 21day')
        # for i in range(0, len(df.close)):
        #     if df.PB.values[i] < 0.05 and df.IIP21.values[i] > 0:
        #         plt.plot(df.index.values[i], 0, 'r^')
        #     elif df.PB.values[i] > 0.95 and df.IIP21.values[i] < 0:
        #         plt.plot(df.index.values[i], 0, 'bv')
        # plt.grid(True)
        # plt.legend(loc='best')
        # plt.show()
        #
        # print(df)


        # DB입력하기
        # for idx in range(len(df)):
        #     date = price_list['date'][idx]
        #     MRM_1 = price_list['1MRM'][idx]
        #     MRM_3 = price_list['3MRM'][idx]
        #     MRM_6 = price_list['6MRM'][idx]
        #     MRM_12 = price_list['12MRM'][idx]
        #     sql = f"REPLACE INTO momentum.`{stock}` (date, 1MRM, 3MRM, 6MRM, 12MRM) " \
        #           f"VALUES ('{date}','{MRM_1}','{MRM_3}','{MRM_6}','{MRM_12}')"
        #     self.cur.execute(sql)
        #     self.conn.commit()



if __name__ == "__main__":
    bollinger = AnalyzeBollinger()
    bollinger.analyze_bollinger_by_date_stock(stock='3s', start_date='20170102', end_date='20170331')




