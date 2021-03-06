import pymysql
import pandas as pd
import numpy as np
import math
import datetime
import config.setting as cf
import config.db_sql as dbl
from matplotlib import pyplot as plt
from config import logger as logger

class backtester():
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
        # backtest_book 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'backtest_book'"
        if self.cur.execute(sql):
            self.logger.info("backtest_book 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE backtest_book"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("backtest_book 스키마 생성")

        # backtest_portfolio 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'backtest_portfolio'"
        if self.cur.execute(sql):
            self.logger.info("backtest_portfolio 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE backtest_portfolio"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("backtest_portfolio 스키마 생성")

        # backtest_result 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'backtest_result'"
        if self.cur.execute(sql):
            self.logger.info("backtest_result 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE backtest_result"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("backtest_result 스키마 생성")

        # backtest_result.evaluation 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'backtest_result' and table_name = 'evaluation'"
        if self.cur.execute(sql):
            self.logger.info("backtest_result.evaluation 테이블 존재")
            pass
        else:
            sql = """
                  CREATE TABLE IF NOT EXISTS backtest_result.evaluation (
                  strategy CHAR(100), 
                  start DATE, 
                  end DATE, 
                  initial BIGINT(20), 
                  final BIGINT(20),
                  ROI FLOAT,  
                  CAGR FLOAT, 
                  MEAN FLOAT, 
                  VOL FLOAT, 
                  MDD FLOAT, 
                  Sharpe FLOAT)
            """
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info("backtest_result.evaluation 테이블 생성")

    def create_book_table(self, strategy):
        '''종목별 모멘텀 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtest_book' and table_name = '{strategy}'"
        if self.cur.execute(sql):
            self.logger.info(f"backtest_book.{strategy} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS backtest_book.`{strategy}` (" \
                  f"date DATE," \
                  f"trading CHAR(10), " \
                  f"stock VARCHAR(50), " \
                  f"price BIGINT(20), " \
                  f"quantity BIGINT(20), " \
                  f"investment BIGINT(20), " \
                  f"fee BIGINT(20), " \
                  f"tax BIGINT(20), " \
                  f"slippage BIGINT(20))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"backtest_book.{strategy} 테이블 생성 완료")

    def create_portfolio_table(self, strategy):
        '''종목별 모멘텀 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtest_portfolio' and table_name = '{strategy}'"
        if self.cur.execute(sql):
            self.logger.info(f"backtest_portfolio.{strategy} 테이블 존재함")
            pass
        else:
            sql = f"CREATE TABLE IF NOT EXISTS backtest_portfolio.`{strategy}` (" \
                  f"date DATE," \
                  f"cash BIGINT(20), " \
                  f"investment BIGINT(20), " \
                  f"total BIGINT(20), " \
                  f"daily_return FLOAT, " \
                  f"total_return FLOAT, " \
                  f"PRIMARY KEY (date))"
            self.cur.execute(sql)
            self.conn.commit()
            self.logger.info(f"backtest_portfolio.{strategy} 테이블 생성 완료")

    def get_universe(self, strategy, date):
        '''전략에 해당하는 종목 리스트를 반환
            전략변수 = 전략1+전략2+..+전략n+F_SCORE>m+종목수'''
        # 전략 리스트 생성
        strategy_list = strategy.split('+')
        # 유니버스 데이터프레임 가져오기
        sql = f"SELECT * FROM universe.`{date}`"
        self.cur.execute(sql)
        universe = self.cur.fetchall()
        universe = pd.DataFrame(universe)
        # 전략별 순위매기기
        for stg in strategy_list:
            if stg in ['PER', 'PBR', 'PSR', 'PCR', 'PEG', 'EVEBIT']:  # 오름차순 순위 대상
                universe[f'{stg}'+'_RANK'] = universe[f'{stg}'].rank(ascending=True)
            elif stg in ['ROE', 'ROA', 'GPA', '1MRM', '3MRM', '6MRM', '12MRM']:  # 내림차순 순위 대상
                universe[f'{stg}' + '_RANK'] = universe[f'{stg}'].rank(ascending=False)
            elif stg[0:7] == 'F_SCORE':
                universe = universe[universe['F_SCORE'] >= int(stg[-1])]
        # 종합 순위 매기기
        universe['TOTAL_SCORE'] = 0
        for stg in strategy_list:
            if stg in ['PER', 'PBR', 'PSR', 'PCR', 'PEG', 'EVEBIT', 'ROE', 'ROA', 'GPA', '1MRM', '3MRM', '6MRM', '12MRM']:
                universe['TOTAL_SCORE'] += universe[f'{stg}' + '_RANK']
        # 주어진 주식수에 맞는 종목 리스트만 추출
        universe = universe.sort_values(by=['TOTAL_SCORE'], axis=0, ascending=True)
        universe = list(universe['stock'][:int(strategy_list[-1])])
        return universe

    def backtest_book(self, strategy, initial, universe, start, end):
        '''전략에 맞는 거래장부 테이블 생성'''
        # # 거래장부 테이블이 있었다면 삭제
        # sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtest_book' and table_name = '{strategy}'"
        # if self.cur.execute(sql):
        #     sql = f"DROP TABLE backtest_book.`{strategy}`"
        #     self.cur.execute(sql)
        #     self.conn.commit()

        # 데이터프레임 생성
        book = pd.DataFrame(columns=['date', 'trading', 'stock', 'price', 'quantity', 'investment', 'fee', 'tax', 'slippage'])
        # 매수 장부
        for stock in universe:
            sql = f"SELECT close FROM daily_price.`{stock}` WHERE date={start}"
            self.cur.execute(sql)
            price = self.cur.fetchone()['close']
            slot = math.floor(initial/len(universe))
            trading = 'buy'
            quantity = math.floor(slot/price)
            investment = price * quantity
            fee = math.floor(investment * self.fee_rate)
            if investment + fee > slot:
                quantity -= 1
            investment = price * quantity
            fee = math.floor(investment * self.fee_rate)
            tax = 0
            slippage = 0
            book = book.append({'date': start, 'trading': trading, 'stock': stock, 'price': price, 'quantity': quantity,
                                'investment': investment, 'fee': fee, 'tax': tax, 'slippage': slippage}, ignore_index=True)
        # 매도 장부
        for stock in universe:
            sql = f"SELECT close FROM daily_price.`{stock}` WHERE date={end}"
            self.cur.execute(sql)
            price = self.cur.fetchone()['close']
            # slot = math.floor(initial/len(universe))
            trading = 'sell'
            quantity = int(book[(book['stock'] == stock) & (book['trading'] == 'buy')]['quantity'])
            investment = price * quantity
            fee = math.floor(investment * self.fee_rate)
            tax = math.floor(investment * self.tax_rate)
            slippage = 0
            # print(end, trading, stock, price, quantity, investment, fee, tax, slippage)
            book = book.append({'date': end, 'trading': trading, 'stock': stock, 'price': price, 'quantity': quantity,
                                'investment': investment, 'fee': fee, 'tax': tax, 'slippage': slippage}, ignore_index=True)
        # DB 저장
        # book.to_csv(f'C:\\Project\\qubot\\csv\\{strategy}_book.csv', encoding='euc-kr')
        self.create_book_table(strategy)
        for row in book.itertuples():
            sql = f"INSERT INTO backtest_book.`{strategy}` (date, trading, stock, price, quantity, investment, fee, tax, slippage) " \
                  f"VALUES ({row.date}, '{row.trading}', '{row.stock}', {row.price}, {row.quantity}, {row.investment}, {row.fee}, {row.tax}, {row.slippage})"
            self.cur.execute(sql)
            self.conn.commit()
        return book


    def backtest_portfolio(self, strategy, initial, book):
        '''전략에 맞는 포트폴리오 테이블 생성'''
        # # 거래장부 가져오기
        # sql = f"SELECT * FROM backtest_book.`{strategy}`"
        # self.cur.execute(sql)
        # book = self.cur.fetchall()
        # book = pd.DataFrame(book)

        # # 포트폴리오 테이블이 있었다면 삭제
        # sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtest_portfolio' and table_name = '{strategy}'"
        # if self.cur.execute(sql):
        #     sql = f"DROP TABLE backtest_portfolio.`{strategy}`"
        #     self.cur.execute(sql)
        #     self.conn.commit()

        # 투자종목 리스트 추출
        stock_list = list(book[book['trading'] == 'buy']['stock'])

        # 시작일~종료일, 날짜리스트 추출
        start_dt = book['date'].min()
        end_dt = book['date'].max()
        # start_str = datetime.datetime.strftime(book['date'].min(), '%Y%m%d')
        # end_str = datetime.datetime.strftime(book['date'].max(), '%Y%m%d')
        sql = f"SELECT date FROM market_index.kospi WHERE date BETWEEN {start_dt} AND {end_dt}"
        self.cur.execute(sql)
        date_list = self.cur.fetchall()

        # 포트폴리오 데이터프레임 생성
        portfolio = pd.DataFrame(columns=['date', 'cash', 'investment', 'total', 'daily_return', 'total_return'])
        for date in date_list:
            date_str = datetime.datetime.strftime(date['date'], '%Y%m%d')
            if date_str == start_dt:
                investment = 0
                fee = 0
                tax = 0
                slippage = 0
                for stock in stock_list:
                    investment += int(book[(book['stock'] == stock) & (book['trading'] == 'buy') & (book['date'] == start_dt)]['investment'])
                    fee += int(book[(book['stock'] == stock) & (book['trading'] == 'buy') & (book['date'] == start_dt)]['fee'])
                    tax += int(book[(book['stock'] == stock) & (book['trading'] == 'buy') & (book['date'] == start_dt)]['tax'])
                    slippage += int(book[(book['stock'] == stock) & (book['trading'] == 'buy') & (book['date'] == start_dt)]['slippage'])
                    cash = initial - investment - fee - tax - slippage
            elif date_str == end_dt:
                investment = 0
                fee = 0
                tax = 0
                slippage = 0
                for stock in stock_list:
                    investment += int(book[(book['stock'] == stock) & (book['trading'] == 'sell') & (book['date'] == end_dt)]['investment'])
                    fee += int(book[(book['stock'] == stock) & (book['trading'] == 'sell') & (book['date'] == end_dt)]['fee'])
                    tax += int(book[(book['stock'] == stock) & (book['trading'] == 'sell') & (book['date'] == end_dt)]['tax'])
                    slippage += int(book[(book['stock'] == stock) & (book['trading'] == 'sell') & (book['date'] == end_dt)]['slippage'])
            else:
                investment = 0
                for stock in stock_list:
                    sql = f"SELECT close FROM daily_price.`{stock}` WHERE date={date_str}"
                    self.cur.execute(sql)
                    price = self.cur.fetchone()['close']
                    quantity = int(book[(book['stock'] == stock) & (book['trading'] == 'buy') & (book['date'] == start_dt)]['quantity'])
                    investment += price*quantity
            portfolio = portfolio.append({'date': date_str, 'cash': cash, 'investment': investment}, ignore_index=True)

        # 수익률 계산
        portfolio['total'] = portfolio['cash'] + portfolio['investment']
        portfolio['daily_return'] = portfolio['total'].pct_change()
        portfolio['total_return'] = (portfolio['total']-initial)/initial
        portfolio['daily_return'][0] = (portfolio['total'][0]-initial)/initial

        # DB 저장
        self.create_portfolio_table(strategy=strategy)
        for row in portfolio.itertuples():
            sql = f"INSERT INTO backtest_portfolio.`{strategy}` (date, cash, investment, total, daily_return, total_return) " \
                  f"VALUES ({row.date}, {row.cash}, {row.investment}, {row.total}, {row.daily_return}, {row.total_return})"
            self.cur.execute(sql)
            self.conn.commit()

        return portfolio

    def backtest_evaluation(self, strategy, initial):
        '''포트폴리오 평가'''
        # 포트폴리오 가져오기
        sql = f"SELECT * FROM backtest_portfolio.`{strategy}`"
        self.cur.execute(sql)
        portfolio = self.cur.fetchall()
        portfolio = pd.DataFrame(portfolio)

        # 평가지표 계산
        start = datetime.datetime.strftime(portfolio['date'].min(), '%Y%m%d')
        end = datetime.datetime.strftime(portfolio['date'].max(), '%Y%m%d')
        final = portfolio['total'][len(portfolio)-1]
        ROI = (final-initial)/initial
        if ROI < 0:
            CAGR = 0
        else:
            CAGR = (ROI ** (252./len(portfolio))) - 1
        MEAN = portfolio['daily_return'].mean()
        VOL = np.std(portfolio['daily_return'] * np.sqrt(252.))
        MDD = (portfolio['total'].max() - portfolio['total'].min())/portfolio['total'].max()
        Sharpe = np.mean(portfolio['daily_return'])/np.std(portfolio['daily_return']) * np.sqrt(252.)

        # DB입력
        sql = f"INSERT INTO backtest_result.evaluation (strategy, start, end, initial, final, ROI, CAGR, MEAN, VOL, MDD, Sharpe) " \
              f"VALUES ('{strategy}', {start}, {end}, {initial}, {final}, {ROI}, {CAGR}, {MEAN}, {VOL}, {MDD}, {Sharpe})"
        self.cur.execute(sql)
        self.conn.commit()

    def backtest(self, strategy, initial, set_date_list, buy_date_list, sell_date_list):
        '''포트폴리오 리밸런싱을 포함하여 백테스트 실행'''
        for i in range(len(set_date_list)):
            if i == 0:
                initial_cash = initial
            else:
                initial_cash = portfolio['total'][len(portfolio)-1]
            universe = self.get_universe(strategy=strategy, date=set_date_list[i])
            book = self.backtest_book(strategy=strategy, initial=initial_cash, universe=universe, start=buy_date_list[i], end=sell_date_list[i])
            portfolio = self.backtest_portfolio(strategy=strategy, initial=initial_cash, book=book)
            self.logger.info(f"({i+1}/{len(set_date_list)}) {strategy} 포트폴리오 생성중")
        self.backtest_evaluation(strategy=strategy, initial=initial)

    def backtest_check(self, strategy):
        '''csv 체크용'''
        sql = f"SELECT * FROM backtest_book.`{strategy}`"
        self.cur.execute(sql)
        book = self.cur.fetchall()
        book = pd.DataFrame(book)

        sql = f"SELECT * FROM backtest_portfolio.`{strategy}`"
        self.cur.execute(sql)
        portfolio = self.cur.fetchall()
        portfolio = pd.DataFrame(portfolio)

        book.to_csv(f'C:\\Project\\qubot\\csv\\book.csv', encoding='euc-kr')
        portfolio.to_csv(f'C:\\Project\\qubot\\csv\\portfolio.csv', encoding='euc-kr')


    def backtest_graph(self, strategy):
        '''백테스트 결과 그래프 그리기'''
        sql = f"SELECT * FROM backtest_portfolio.`{strategy}`"
        self.cur.execute(sql)
        portfolio = self.cur.fetchall()
        portfolio = pd.DataFrame(portfolio)

        start_date = portfolio['date'].min()
        end_date = portfolio['date'].max()

        sql = f"SELECT * FROM market_index.kospi WHERE date BETWEEN {start_date} AND {end_date}"
        self.cur.execute(sql)
        kospi = self.cur.fetchall()
        kospi = pd.DataFrame(kospi)

        sql = f"SELECT * FROM market_index.kosdaq WHERE date BETWEEN {start_date} AND {end_date}"
        self.cur.execute(sql)
        kosdaq = self.cur.fetchall()
        kosdaq = pd.DataFrame(kosdaq)

        plt.figure(figsize=(9, 6))
        plt.title(f'{strategy}')
        plt.plot(portfolio['date'], portfolio['total'], 'c', label='portfolio')
        plt.legend(loc='best')
        plt.show()



    def meta_analysis(self):
        '''메타 분석'''
        sql = "SELECT * FROM backtest_result.evaluation"
        self.cur.execute(sql)
        meta = self.cur.fetchall()
        meta = pd.DataFrame(meta)

        max_return = meta.loc[meta['ROI'] == meta['ROI'].max()]
        max_sharpe = meta.loc[meta['Sharpe'] == meta['Sharpe'].max()]
        min_risk = meta.loc[meta['VOL'] == meta['VOL'].min()]  # VOL 또는 MDD
        meta.plot.scatter(x='VOL', y='ROI', c='Sharpe', cmap='viridis', edgecolors='k', figsize=(11, 6), grid=True)  # 컬러맵 vidiris, 테두리는 검정(k)로 표시
        plt.scatter(x=max_sharpe['VOL'], y=max_sharpe['ROI'], c='r', marker='*', s=300)  # 샤프지수가 가장 큰 포트폴리오를 300크기 붉은 별표로 표시
        plt.scatter(x=min_risk['VOL'], y=min_risk['ROI'], c='r', marker='X', s=200)  # 리스크가 가장 작은 포트폴리오를 200크기 붉은 X표로 표시
        plt.title('Portfolio Optimization')
        plt.xlabel('Risk')
        plt.ylabel('Expected Returns')
        print('max_return:', max_return['strategy'].iloc[0], '| ROI:', max_return['ROI'].iloc[0], '| VOL:', max_return['VOL'].iloc[0], '| Sharpe', max_return['Sharpe'].iloc[0])
        print('max_sharpe:', max_sharpe['strategy'].iloc[0], '| ROI:', max_sharpe['ROI'].iloc[0], '| VOL:', max_sharpe['VOL'].iloc[0], '| Sharpe', max_sharpe['Sharpe'].iloc[0])
        print('min_risk:', min_risk['strategy'].iloc[0], '| ROI:', min_risk['ROI'].iloc[0], '| VOL:', min_risk['VOL'].iloc[0], '| Sharpe', min_risk['Sharpe'].iloc[0])
        plt.show()
        self.backtest_graph(max_return['strategy'].iloc[0])
        self.backtest_graph(max_sharpe['strategy'].iloc[0])
        self.backtest_graph(min_risk['strategy'].iloc[0])

    def executor(self):
        today = datetime.datetime.today().strftime('%Y%m%d')
        # 4, 6, 9, 12월 거래
        set_date_list = ['20170403', '20170601', '20170901', '20171201',
                         '20180402', '20180601', '20180903', '20181203',
                         '20190401', '20190603', '20190902', '20191202',
                         '20200401', '20200601', '20200901', '20201201',
                         '20210331', '20210531']
        buy_date_list = ['20170404', '20170602', '20170904', '20171204',
                         '20180403', '20180604', '20180904', '20181204',
                         '20190402', '20190604', '20190903', '20191203',
                         '20200402', '20200602', '20200902', '20201202',
                         '20210401', '20210601']
        sell_date_list = ['20170601', '20170901', '20171201', '20180402',
                          '20180601', '20180903', '20181203', '20190401',
                          '20190603', '20190902', '20191202', '20200401',
                          '20200601', '20200901', '20201201', '20210331',
                          '20210528', '20210604']

        # strategy_list 생성
        # strategy = 'PER+PBR+PSR+ROE+ROA+GPA+1MRM+3MRM+6MRM+F_SCORE>=9+10'
        val_list = ['', 'PER', 'PBR', 'PSR']
        profit_list = ['', 'ROE', 'ROA', 'GPA']
        mrm_list = ['', '1MRM', '3MRM', '6MRM']
        amt_list = ['10']
        strategy_list = ['PER+PBR+PSR+ROE+ROA+GPA+1MRM+3MRM+6MRM+F_SCORE>=9+10']
        # for i in val_list:
        #     for j in profit_list:
        #         for k in mrm_list:
        #             for l in amt_list:
        #                 if i == '':
        #                     if j == '':
        #                         if k == '':
        #                             item = 'F_SCORE>=9' + '+' + l
        #                         else:
        #                             item = k + '+' + 'F_SCORE>=9' + '+' + l
        #                     else:
        #                         if k == '':
        #                             item = j + '+' + 'F_SCORE>=9' + '+' + l
        #                         else:
        #                             item = j + '+' + k + '+' + 'F_SCORE>=9' + '+' + l
        #                 else:
        #                     if j == '':
        #                         if k == '':
        #                             item = i + '+' + 'F_SCORE>=9' + '+' + l
        #                         else:
        #                             item = i + '+' + k + '+' + 'F_SCORE>=9' + '+' + l
        #                     else:
        #                         if k == '':
        #                             item = i + '+' + j + '+' + 'F_SCORE>=9' + '+' + l
        #                         else:
        #                             item = i + '+' + j + '+' + k + '+' + 'F_SCORE>=9' + '+' + l
        #                 strategy_list.append(item)

        for strategy in strategy_list:
            self.backtest(strategy=strategy, initial=10000000, set_date_list=set_date_list, buy_date_list=buy_date_list, sell_date_list=sell_date_list)


if __name__=="__main__":
    # 초기화
    dbl = dbl.db_sql()
    dbl.drop_db(db_name='backtest_book')
    dbl.drop_db(db_name='backtest_portfolio')
    dbl.drop_db(db_name='backtest_result')

    # 메타분석
    backtester = backtester()
    backtester.executor()
    backtester.meta_analysis()


