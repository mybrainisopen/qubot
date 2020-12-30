import pymysql
import pandas as pd
import numpy as np
import math
import datetime
import config.config as cf
import config.db_sql as dbl
from matplotlib import pyplot as plt

class backtester():
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
            # print(f"[{self.now}] backtest_book 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE backtest_book"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] backtest_book 스키마 생성")

        # backtest_portfolio 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'backtest_portfolio'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] backtest_portfolio 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE backtest_portfolio"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] backtest_portfolio 스키마 생성")

        # backtest_result 스키마 생성
        sql = "SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = 'backtest_result'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] backtest_result 스키마 존재")
            pass
        else:
            sql = "CREATE DATABASE backtest_result"
            self.cur.execute(sql)
            self.conn.commit()
            # print(f"[{self.now}] backtest_result 스키마 생성")

        # backtest_result.evaluation 테이블 생성
        sql = "SELECT 1 FROM Information_schema.tables where " \
              "table_schema = 'backtest_result' and table_name = 'evaluation'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] backtest_result.evaluation 테이블 존재")
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
            # print(f"[{self.now}] backtest_result.evaluation 테이블 생성")

    def create_book_table(self, strategy):
        '''종목별 모멘텀 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtest_book' and table_name = '{strategy}'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] backtest_book.{strategy} 테이블 존재함")
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
            # print(f"[{self.now}] backtest_book.{strategy} 테이블 생성 완료")

    def create_portfolio_table(self, strategy):
        '''종목별 모멘텀 테이블 생성 함수'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = 'backtest_portfolio' and table_name = '{strategy}'"
        if self.cur.execute(sql):
            # print(f"[{self.now}] backtest_portfolio.{strategy} 테이블 존재함")
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
            # print(f"[{self.now}] backtest_portfolio.{strategy} 테이블 생성 완료")

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
            CAGR = ROI ** ((252./len(portfolio))-1)
        MEAN = portfolio['daily_return'].mean()
        VOL = np.std(portfolio['daily_return'] * np.sqrt(252.))
        MDD = (portfolio['total'].max() - portfolio['total'].min())/portfolio['total'].max()
        Sharpe = np.mean(portfolio['daily_return'])/np.std(portfolio['daily_return']) * np.sqrt(252.)

        # DB입력
        # print(strategy, start, end, initial, final, ROI, CAGR, VOL, MDD, Sharpe)
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
            # print(buy_date_list[i], universe)
            book = self.backtest_book(strategy=strategy, initial=initial_cash, universe=universe, start=buy_date_list[i], end=sell_date_list[i])
            portfolio = self.backtest_portfolio(strategy=strategy, initial=initial_cash, book=book)
            print(f"[{self.now}] ({i+1}/{len(set_date_list)}) {strategy} 포트폴리오 생성중")
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
        meta.plot.scatter(x='VOL', y='ROI', c='Sharpe', cmap='viridis', edgecolors='k', figsize=(11, 6), grid=True)  # 컬러맵 vidiris, 테두리는 rjawjd(k)로 표시
        plt.scatter(x=max_sharpe['VOL'], y=max_sharpe['ROI'], c='r', marker='*', s=300)  # 샤프지수가 가장 큰 포트폴리오를 300크기 붉은 별표로 표시
        plt.scatter(x=min_risk['VOL'], y=min_risk['ROI'], c='r', marker='X', s=200)  # 리스크가 가장 작은 포트폴리오를 200크기 붉은 X표로 표시
        plt.title('Portfolio Optimization')
        plt.xlabel('Risk')
        plt.ylabel('Expected Returns')
        print('max_return:', max_return['strategy'].iloc[0], '| ROI:', max_return['ROI'].iloc[0], '| VOL:', max_return['VOL'].iloc[0], '| Sharpe', max_return['Sharpe'].iloc[0])
        print('max_sharpe:', max_sharpe['strategy'].iloc[0], '| ROI:', max_sharpe['ROI'].iloc[0], '| VOL:', max_sharpe['VOL'].iloc[0], '| Sharpe', max_sharpe['Sharpe'].iloc[0])
        print('min_risk:', min_risk['strategy'].iloc[0], '| ROI:', min_risk['ROI'].iloc[0], '| VOL:', min_risk['VOL'].iloc[0], '| Sharpe', min_risk['Sharpe'].iloc[0])
        plt.show()





if __name__=="__main__":
    backtester = backtester()
    # backtester.get_universe('PER+ROE+1MRM+F_SCORE>=9+5', '20180102')
    # backtester.backtest_book(strategy=strategy, initial=10000000, universe=stock_list, start=buy_date, end=sell_date)
    # backtester.backtest_portfolio(strategy=strategy, initial=10000000)
    # backtester.backtest_evaluation(strategy=strategy, initial=10000000)
    # strategy = 'PER+ROE+1MRM+F_SCORE>=9+10'
    set_date_list = ['20170403', '20170601', '20170901', '20171201',
                     '20180402', '20180601', '20180903', '20181203',
                     '20190401', '20190603', '20190902', '20191202',
                     '20200401', '20200601', '20200901', '20201201']
    buy_date_list = ['20170404', '20170602', '20170904', '20171204',
                     '20180403', '20180604', '20180904', '20181204',
                     '20190402', '20190604', '20190903', '20191203',
                     '20200402', '20200602', '20200902', '20201202']
    sell_date_list = ['20170601', '20170901', '20171201', '20180402',
                      '20180601', '20180903', '20181203', '20190401',
                      '20190603', '20190902', '20191202', '20200401',
                      '20200601', '20200901', '20201201', '20201224']
    # set_date_list = ['20200401', '20200601', '20200901']
    # buy_date_list = ['20200402', '20200602', '20200902']
    # sell_date_list = ['20200601', '20200901', '20201218']
    strategy_list = [
                     'PER+ROE+1MRM+F_SCORE>=9+10', 'PER+ROA+1MRM+F_SCORE>=9+10', 'PER+GPA+1MRM+F_SCORE>=9+10', 'PBR+ROE+1MRM+F_SCORE>=9+10', 'PBR+ROA+1MRM+F_SCORE>=9+10', 'PBR+GPA+1MRM+F_SCORE>=9+10', 'PSR+ROE+1MRM+F_SCORE>=9+10', 'PSR+ROA+1MRM+F_SCORE>=9+10', 'PSR+GPA+1MRM+F_SCORE>=9+10', 'EVEBIT+ROE+1MRM+F_SCORE>=9+10', 'EVEBIT+ROA+1MRM+F_SCORE>=9+10', 'EVEBIT+GPA+1MRM+F_SCORE>=9+10',
                     'PER+ROE+3MRM+F_SCORE>=9+10', 'PER+ROA+3MRM+F_SCORE>=9+10', 'PER+GPA+3MRM+F_SCORE>=9+10', 'PBR+ROE+3MRM+F_SCORE>=9+10', 'PBR+ROA+3MRM+F_SCORE>=9+10', 'PBR+GPA+3MRM+F_SCORE>=9+10', 'PSR+ROE+3MRM+F_SCORE>=9+10', 'PSR+ROA+3MRM+F_SCORE>=9+10', 'PSR+GPA+3MRM+F_SCORE>=9+10', 'EVEBIT+ROE+3MRM+F_SCORE>=9+10', 'EVEBIT+ROA+3MRM+F_SCORE>=9+10', 'EVEBIT+GPA+3MRM+F_SCORE>=9+10',
                     'PER+ROE+6MRM+F_SCORE>=9+10', 'PER+ROA+6MRM+F_SCORE>=9+10', 'PER+GPA+6MRM+F_SCORE>=9+10', 'PBR+ROE+6MRM+F_SCORE>=9+10', 'PBR+ROA+6MRM+F_SCORE>=9+10', 'PBR+GPA+6MRM+F_SCORE>=9+10', 'PSR+ROE+6MRM+F_SCORE>=9+10', 'PSR+ROA+6MRM+F_SCORE>=9+10', 'PSR+GPA+6MRM+F_SCORE>=9+10', 'EVEBIT+ROE+6MRM+F_SCORE>=9+10', 'EVEBIT+ROA+6MRM+F_SCORE>=9+10', 'EVEBIT+GPA+6MRM+F_SCORE>=9+10',
                     'PER+ROE+12MRM+F_SCORE>=9+10', 'PER+ROA+12MRM+F_SCORE>=9+10', 'PER+GPA+12MRM+F_SCORE>=9+10', 'PBR+ROE+12MRM+F_SCORE>=9+10', 'PBR+ROA+12MRM+F_SCORE>=9+10', 'PBR+GPA+12MRM+F_SCORE>=9+10', 'PSR+ROE+12MRM+F_SCORE>=9+10', 'PSR+ROA+12MRM+F_SCORE>=9+10', 'PSR+GPA+12MRM+F_SCORE>=9+10', 'EVEBIT+ROE+12MRM+F_SCORE>=9+10', 'EVEBIT+ROA+12MRM+F_SCORE>=9+10', 'EVEBIT+GPA+12MRM+F_SCORE>=9+10',
                     'PER+ROE+1MRM+F_SCORE>=9+20', 'PER+ROA+1MRM+F_SCORE>=9+20', 'PER+GPA+1MRM+F_SCORE>=9+20', 'PBR+ROE+1MRM+F_SCORE>=9+20', 'PBR+ROA+1MRM+F_SCORE>=9+20', 'PBR+GPA+1MRM+F_SCORE>=9+20', 'PSR+ROE+1MRM+F_SCORE>=9+20', 'PSR+ROA+1MRM+F_SCORE>=9+20', 'PSR+GPA+1MRM+F_SCORE>=9+20', 'EVEBIT+ROE+1MRM+F_SCORE>=9+20', 'EVEBIT+ROA+1MRM+F_SCORE>=9+20', 'EVEBIT+GPA+1MRM+F_SCORE>=9+20',
                     'PER+ROE+3MRM+F_SCORE>=9+20', 'PER+ROA+3MRM+F_SCORE>=9+20', 'PER+GPA+3MRM+F_SCORE>=9+20', 'PBR+ROE+3MRM+F_SCORE>=9+20', 'PBR+ROA+3MRM+F_SCORE>=9+20', 'PBR+GPA+3MRM+F_SCORE>=9+20', 'PSR+ROE+3MRM+F_SCORE>=9+20', 'PSR+ROA+3MRM+F_SCORE>=9+20', 'PSR+GPA+3MRM+F_SCORE>=9+20', 'EVEBIT+ROE+3MRM+F_SCORE>=9+20', 'EVEBIT+ROA+3MRM+F_SCORE>=9+20', 'EVEBIT+GPA+3MRM+F_SCORE>=9+20',
                     'PER+ROE+6MRM+F_SCORE>=9+20', 'PER+ROA+6MRM+F_SCORE>=9+20', 'PER+GPA+6MRM+F_SCORE>=9+20', 'PBR+ROE+6MRM+F_SCORE>=9+20', 'PBR+ROA+6MRM+F_SCORE>=9+20', 'PBR+GPA+6MRM+F_SCORE>=9+20', 'PSR+ROE+6MRM+F_SCORE>=9+20', 'PSR+ROA+6MRM+F_SCORE>=9+20', 'PSR+GPA+6MRM+F_SCORE>=9+20', 'EVEBIT+ROE+6MRM+F_SCORE>=9+20', 'EVEBIT+ROA+6MRM+F_SCORE>=9+20', 'EVEBIT+GPA+6MRM+F_SCORE>=9+20',
                     'PER+ROE+12MRM+F_SCORE>=9+20', 'PER+ROA+12MRM+F_SCORE>=9+20', 'PER+GPA+12MRM+F_SCORE>=9+20', 'PBR+ROE+12MRM+F_SCORE>=9+20', 'PBR+ROA+12MRM+F_SCORE>=9+20', 'PBR+GPA+12MRM+F_SCORE>=9+20', 'PSR+ROE+12MRM+F_SCORE>=9+20', 'PSR+ROA+12MRM+F_SCORE>=9+20', 'PSR+GPA+12MRM+F_SCORE>=9+20', 'EVEBIT+ROE+12MRM+F_SCORE>=9+20', 'EVEBIT+ROA+12MRM+F_SCORE>=9+20', 'EVEBIT+GPA+12MRM+F_SCORE>=9+20'
                     ]
    # for strategy in strategy_list:
    #     backtester.backtest(strategy=strategy, initial=10000000, set_date_list=set_date_list, buy_date_list=buy_date_list, sell_date_list=sell_date_list)

    backtester.meta_analysis()
    backtester.backtest_graph('PSR+GPA+1MRM+F_SCORE>=9+10')
    backtester.backtest_graph('PSR+GPA+3MRM+F_SCORE>=9+10')
    backtester.backtest_graph('PBR+ROE+12MRM+F_SCORE>=9+20')

    # 초기화
    # dbl.drop_db(db_name='backtest_book')
    # dbl.drop_db(db_name='backtest_portfolio')
    # dbl.drop_db(db_name='backtest_result')


    # backtester.backtest_check(strategy)

    # ['PER', 'PBR', 'PSR', 'PCR', 'PEG', 'EVEBIT', 'ROE', 'ROA', 'GPA', '1MRM', '3MRM', '6MRM', '12MRM']:

    # strategy_list = ['PER+10', 'PBR+10', 'PSR+10', 'PCR+10', 'PEG+10', 'EVEBIT+10',
    #                  'PER+20', 'PBR+20', 'PSR+20', 'PCR+20', 'PEG+20', 'EVEBIT+20',
    #                  'ROE+10', 'ROA+10', 'GPA+10',
    #                  'ROE+20', 'ROA+20', 'GPA+20',
    #                  '1MRM+10', '3MRM+10', '6MRM+10', '10MRM+10',
    #                  '1MRM+20', '3MRM+20', '6MRM+20', '10MRM+20',
    #                  'PER+ROE+10', 'PER+ROA+10', 'PER+GPA+10', 'PBR+ROE+10', 'PBR+ROA+10', 'PBR+GPA+10', 'PSR+ROE+10', 'PSR+ROA+10', 'PSR+GPA+10',
    #                  'PCR+ROE+10', 'PCR+ROA+10', 'PCR+GPA+10', 'PEG+ROE+10', 'PEG+ROA+10', 'PEG+GPA+10', 'EVEBIT+ROE+10', 'EVEBIT+ROA+10', 'EVEBIT+GPA+10',
    #                  'PER+ROE+20', 'PER+ROA+20', 'PER+GPA+20', 'PBR+ROE+20', 'PBR+ROA+20', 'PBR+GPA+20', 'PSR+ROE+20', 'PSR+ROA+20', 'PSR+GPA+20',
    #                  'PCR+ROE+20', 'PCR+ROA+20', 'PCR+GPA+20', 'PEG+ROE+20', 'PEG+ROA+20', 'PEG+GPA+20', 'EVEBIT+ROE+20', 'EVEBIT+ROA+20', 'EVEBIT+GPA+20',
    #                  'PER+1MRM+10', 'PER+3MRM+10', 'PER+6MRM+10', 'PER+12MRM+10', 'PBR+1MRM+10', 'PBR+3MRM+10', 'PBR+6MRM+10', 'PBR+12MRM+10', 'PSR+1MRM+10', 'PSR+3MRM+10', 'PSR+6MRM+10', 'PSR+12MRM+10',
    #                  'PCR+1MRM+10', 'PCR+3MRM+10', 'PCR+6MRM+10', 'PCR+12MRM+10', 'PEG+1MRM+10', 'PEG+3MRM+10', 'PEG+6MRM+10', 'PEG+12MRM+10', 'EVEBIT+1MRM+10', 'EVEBIT+3MRM+10', 'EVEBIT+6MRM+10', 'EVEBIT+12MRM+10',
    #                  'PER+1MRM+20', 'PER+3MRM+20', 'PER+6MRM+20', 'PER+12MRM+10', 'PBR+1MRM+20', 'PBR+3MRM+20', 'PBR+6MRM+20', 'PBR+12MRM+20', 'PSR+1MRM+20', 'PSR+3MRM+20', 'PSR+6MRM+20', 'PSR+12MRM+10',
    #                  'PCR+1MRM+20', 'PCR+3MRM+20', 'PCR+6MRM+20', 'PCR+12MRM+10', 'PEG+1MRM+20', 'PEG+3MRM+20', 'PEG+6MRM+20', 'PEG+12MRM+20', 'EVEBIT+1MRM+20', 'EVEBIT+3MRM+20', 'EVEBIT+6MRM+20', 'EVEBIT+12MRM+10',
    #                  'ROE+1MRM+10', 'ROE+3MRM+10', 'ROE+6MRM+10', 'ROE+12MRM+10', 'ROA+1MRM+10', 'ROA+3MRM+10', 'ROA+6MRM+10', 'ROA+12MRM+10', 'GPA+1MRM+10', 'GPA+3MRM+10', 'GPA+6MRM+10', 'GPA+12MRM+10',
    #                  'ROE+1MRM+20', 'ROE+3MRM+20', 'ROE+6MRM+20', 'ROE+12MRM+20', 'ROA+1MRM+20', 'ROA+3MRM+20', 'ROA+6MRM+20', 'ROA+12MRM+20', 'GPA+1MRM+20', 'GPA+3MRM+20', 'GPA+6MRM+20', 'GPA+12MRM+20',
    #                  'PER+ROE+1MRM+10', 'PER+ROA+1MRM+10', 'PER+GPA+1MRM+10', 'PBR+ROE+1MRM+10', 'PBR+ROA+1MRM+10', 'PBR+GPA+1MRM+10', 'PSR+ROE+1MRM+10', 'PSR+ROA+1MRM+10', 'PSR+GPA+1MRM+10',
    #                  'PER+ROE+3MRM+10', 'PER+ROA+3MRM+10', 'PER+GPA+3MRM+10', 'PBR+ROE+3MRM+10', 'PBR+ROA+3MRM+10', 'PBR+GPA+3MRM+10', 'PSR+ROE+3MRM+10', 'PSR+ROA+3MRM+10', 'PSR+GPA+3MRM+10',
    #                  'PER+ROE+6MRM+10', 'PER+ROA+6MRM+10', 'PER+GPA+6MRM+10', 'PBR+ROE+6MRM+10', 'PBR+ROA+6MRM+10', 'PBR+GPA+6MRM+10', 'PSR+ROE+6MRM+10', 'PSR+ROA+6MRM+10', 'PSR+GPA+6MRM+10',
    #                  'PER+ROE+12MRM+10', 'PER+ROA+12MRM+10', 'PER+GPA+12MRM+10', 'PBR+ROE+12MRM+10', 'PBR+ROA+12MRM+10', 'PBR+GPA+12MRM+10', 'PSR+ROE+12MRM+10', 'PSR+ROA+12MRM+10', 'PSR+GPA+12MRM+10',
    #                  'PER+ROE+1MRM+20', 'PER+ROA+1MRM+20', 'PER+GPA+1MRM+20', 'PBR+ROE+1MRM+20', 'PBR+ROA+1MRM+20', 'PBR+GPA+1MRM+20', 'PSR+ROE+1MRM+20', 'PSR+ROA+1MRM+20', 'PSR+GPA+1MRM+20',
    #                  'PER+ROE+3MRM+20', 'PER+ROA+3MRM+20', 'PER+GPA+3MRM+20', 'PBR+ROE+3MRM+20', 'PBR+ROA+3MRM+20', 'PBR+GPA+3MRM+20', 'PSR+ROE+3MRM+20', 'PSR+ROA+3MRM+20', 'PSR+GPA+3MRM+20',
    #                  'PER+ROE+6MRM+20', 'PER+ROA+6MRM+20', 'PER+GPA+6MRM+20', 'PBR+ROE+6MRM+20', 'PBR+ROA+6MRM+20', 'PBR+GPA+6MRM+20', 'PSR+ROE+6MRM+20', 'PSR+ROA+6MRM+20', 'PSR+GPA+6MRM+20',
    #                  'PER+ROE+12MRM+20', 'PER+ROA+12MRM+20', 'PER+GPA+12MRM+20', 'PBR+ROE+12MRM+20', 'PBR+ROA+12MRM+20', 'PBR+GPA+12MRM+20', 'PSR+ROE+12MRM+20', 'PSR+ROA+12MRM+20', 'PSR+GPA+12MRM+20',
    #                  'PER+F_SCORE>=9+10', 'PBR+F_SCORE>=9+10', 'PSR+F_SCORE>=9+10', 'PCR+F_SCORE>=9+10', 'PEG+F_SCORE>=9+10', 'EVEBIT+F_SCORE>=9+10',
    #                  'PER+F_SCORE>=9+20', 'PBR+F_SCORE>=9+20', 'PSR+F_SCORE>=9+20', 'PCR+F_SCORE>=9+20', 'PEG+F_SCORE>=9+20', 'EVEBIT+F_SCORE>=9+20',
    #                  'ROE+F_SCORE>=9+10', 'ROA+F_SCORE>=9+10', 'GPA+F_SCORE>=9+10',
    #                  'ROE+F_SCORE>=9+20', 'ROA+F_SCORE>=9+20', 'GPA+F_SCORE>=9+20',
    #                  '1MRM+F_SCORE>=9+10', '3MRM+F_SCORE>=9+10', '6MRM+F_SCORE>=9+10', '10MRM+F_SCORE>=9+10',
    #                  '1MRM+F_SCORE>=9+20', '3MRM+F_SCORE>=9+20', '6MRM+F_SCORE>=9+20', '10MRM+F_SCORE>=9+20',
    #                  'PER+ROE+F_SCORE>=9+10', 'PER+ROA+F_SCORE>=9+10', 'PER+GPA+F_SCORE>=9+10', 'PBR+ROE+F_SCORE>=9+10', 'PBR+ROA+F_SCORE>=9+10', 'PBR+GPA+F_SCORE>=9+10', 'PSR+ROE+F_SCORE>=9+10', 'PSR+ROA+F_SCORE>=9+10', 'PSR+GPA+F_SCORE>=9+10',
    #                  'PCR+ROE+F_SCORE>=9+10', 'PCR+ROA+F_SCORE>=9+10', 'PCR+GPA+F_SCORE>=9+10', 'PEG+ROE+F_SCORE>=9+10', 'PEG+ROA+F_SCORE>=9+10', 'PEG+GPA+F_SCORE>=9+10', 'EVEBIT+ROE+F_SCORE>=9+10', 'EVEBIT+ROA+F_SCORE>=9+10', 'EVEBIT+GPA+F_SCORE>=9+10',
    #                  'PER+ROE+F_SCORE>=9+20', 'PER+ROA+F_SCORE>=9+20', 'PER+GPA+F_SCORE>=9+20', 'PBR+ROE+F_SCORE>=9+20', 'PBR+ROA+F_SCORE>=9+20', 'PBR+GPA+F_SCORE>=9+20', 'PSR+ROE+F_SCORE>=9+20', 'PSR+ROA+F_SCORE>=9+20', 'PSR+GPA+F_SCORE>=9+20',
    #                  'PCR+ROE+F_SCORE>=9+20', 'PCR+ROA+F_SCORE>=9+20', 'PCR+GPA+F_SCORE>=9+20', 'PEG+ROE+F_SCORE>=9+20', 'PEG+ROA+F_SCORE>=9+20', 'PEG+GPA+F_SCORE>=9+20', 'EVEBIT+ROE+F_SCORE>=9+20', 'EVEBIT+ROA+F_SCORE>=9+20', 'EVEBIT+GPA+F_SCORE>=9+20',
    #                  'PER+1MRM+F_SCORE>=9+10', 'PER+3MRM+F_SCORE>=9+10', 'PER+6MRM+F_SCORE>=9+10', 'PER+12MRM+F_SCORE>=9+10', 'PBR+1MRM+F_SCORE>=9+10', 'PBR+3MRM+F_SCORE>=9+10', 'PBR+6MRM+F_SCORE>=9+10', 'PBR+12MRM+F_SCORE>=9+10', 'PSR+1MRM+F_SCORE>=9+10', 'PSR+3MRM+F_SCORE>=9+10', 'PSR+6MRM+F_SCORE>=9+10', 'PSR+12MRM+F_SCORE>=9+10',
    #                  'PCR+1MRM+F_SCORE>=9+10', 'PCR+3MRM+F_SCORE>=9+10', 'PCR+6MRM+F_SCORE>=9+10', 'PCR+12MRM+F_SCORE>=9+10', 'PEG+1MRM+F_SCORE>=9+10', 'PEG+3MRM+F_SCORE>=9+10', 'PEG+6MRM+F_SCORE>=9+10', 'PEG+12MRM+F_SCORE>=9+10', 'EVEBIT+1MRM+F_SCORE>=9+10', 'EVEBIT+3MRM+F_SCORE>=9+10', 'EVEBIT+6MRM+F_SCORE>=9+10', 'EVEBIT+12MRM+F_SCORE>=9+10',
    #                  'PER+1MRM+F_SCORE>=9+20', 'PER+3MRM+F_SCORE>=9+20', 'PER+6MRM+F_SCORE>=9+20', 'PER+12MRM+F_SCORE>=9+10', 'PBR+1MRM+F_SCORE>=9+20', 'PBR+3MRM+F_SCORE>=9+20', 'PBR+6MRM+F_SCORE>=9+20', 'PBR+12MRM+F_SCORE>=9+20', 'PSR+1MRM+F_SCORE>=9+20', 'PSR+3MRM+F_SCORE>=9+20', 'PSR+6MRM+F_SCORE>=9+20', 'PSR+12MRM+F_SCORE>=9+10',
    #                  'PCR+1MRM+F_SCORE>=9+20', 'PCR+3MRM+F_SCORE>=9+20', 'PCR+6MRM+F_SCORE>=9+20', 'PCR+12MRM+F_SCORE>=9+10', 'PEG+1MRM+F_SCORE>=9+20', 'PEG+3MRM+F_SCORE>=9+20', 'PEG+6MRM+F_SCORE>=9+20', 'PEG+12MRM+F_SCORE>=9+20', 'EVEBIT+1MRM+F_SCORE>=9+20', 'EVEBIT+3MRM+F_SCORE>=9+20', 'EVEBIT+6MRM+F_SCORE>=9+20', 'EVEBIT+12MRM+F_SCORE>=9+10',
    #                  'ROE+1MRM+F_SCORE>=9+10', 'ROE+3MRM+F_SCORE>=9+10', 'ROE+6MRM+F_SCORE>=9+10', 'ROE+12MRM+F_SCORE>=9+10', 'ROA+1MRM+F_SCORE>=9+10', 'ROA+3MRM+F_SCORE>=9+10', 'ROA+6MRM+F_SCORE>=9+10', 'ROA+12MRM+F_SCORE>=9+10', 'GPA+1MRM+F_SCORE>=9+10', 'GPA+3MRM+F_SCORE>=9+10', 'GPA+6MRM+F_SCORE>=9+10', 'GPA+12MRM+F_SCORE>=9+10',
    #                  'ROE+1MRM+F_SCORE>=9+20', 'ROE+3MRM+F_SCORE>=9+20', 'ROE+6MRM+F_SCORE>=9+20', 'ROE+12MRM+F_SCORE>=9+20', 'ROA+1MRM+F_SCORE>=9+20', 'ROA+3MRM+F_SCORE>=9+20', 'ROA+6MRM+F_SCORE>=9+20', 'ROA+12MRM+F_SCORE>=9+20', 'GPA+1MRM+F_SCORE>=9+20', 'GPA+3MRM+F_SCORE>=9+20', 'GPA+6MRM+F_SCORE>=9+20', 'GPA+12MRM+F_SCORE>=9+20',
    #                  'PER+ROE+1MRM+F_SCORE>=9+10', 'PER+ROA+1MRM+F_SCORE>=9+10', 'PER+GPA+1MRM+F_SCORE>=9+10', 'PBR+ROE+1MRM+F_SCORE>=9+10', 'PBR+ROA+1MRM+F_SCORE>=9+10', 'PBR+GPA+1MRM+F_SCORE>=9+10', 'PSR+ROE+1MRM+F_SCORE>=9+10', 'PSR+ROA+1MRM+F_SCORE>=9+10', 'PSR+GPA+1MRM+F_SCORE>=9+10',
    #                  'PER+ROE+3MRM+F_SCORE>=9+10', 'PER+ROA+3MRM+F_SCORE>=9+10', 'PER+GPA+3MRM+F_SCORE>=9+10', 'PBR+ROE+3MRM+F_SCORE>=9+10', 'PBR+ROA+3MRM+F_SCORE>=9+10', 'PBR+GPA+3MRM+F_SCORE>=9+10', 'PSR+ROE+3MRM+F_SCORE>=9+10', 'PSR+ROA+3MRM+F_SCORE>=9+10', 'PSR+GPA+3MRM+F_SCORE>=9+10',
    #                  'PER+ROE+6MRM+F_SCORE>=9+10', 'PER+ROA+6MRM+F_SCORE>=9+10', 'PER+GPA+6MRM+F_SCORE>=9+10', 'PBR+ROE+6MRM+F_SCORE>=9+10', 'PBR+ROA+6MRM+F_SCORE>=9+10', 'PBR+GPA+6MRM+F_SCORE>=9+10', 'PSR+ROE+6MRM+F_SCORE>=9+10', 'PSR+ROA+6MRM+F_SCORE>=9+10', 'PSR+GPA+6MRM+F_SCORE>=9+10',
    #                  'PER+ROE+12MRM+F_SCORE>=9+10', 'PER+ROA+12MRM+F_SCORE>=9+10', 'PER+GPA+12MRM+F_SCORE>=9+10', 'PBR+ROE+12MRM+F_SCORE>=9+10', 'PBR+ROA+12MRM+F_SCORE>=9+10', 'PBR+GPA+12MRM+F_SCORE>=9+10', 'PSR+ROE+12MRM+F_SCORE>=9+10', 'PSR+ROA+12MRM+F_SCORE>=9+10', 'PSR+GPA+12MRM+F_SCORE>=9+10',
    #                  'PER+ROE+1MRM+F_SCORE>=9+20', 'PER+ROA+1MRM+F_SCORE>=9+20', 'PER+GPA+1MRM+F_SCORE>=9+20', 'PBR+ROE+1MRM+F_SCORE>=9+20', 'PBR+ROA+1MRM+F_SCORE>=9+20', 'PBR+GPA+1MRM+F_SCORE>=9+20', 'PSR+ROE+1MRM+F_SCORE>=9+20', 'PSR+ROA+1MRM+F_SCORE>=9+20', 'PSR+GPA+1MRM+F_SCORE>=9+20',
    #                  'PER+ROE+3MRM+F_SCORE>=9+20', 'PER+ROA+3MRM+F_SCORE>=9+20', 'PER+GPA+3MRM+F_SCORE>=9+20', 'PBR+ROE+3MRM+F_SCORE>=9+20', 'PBR+ROA+3MRM+F_SCORE>=9+20', 'PBR+GPA+3MRM+F_SCORE>=9+20', 'PSR+ROE+3MRM+F_SCORE>=9+20', 'PSR+ROA+3MRM+F_SCORE>=9+20', 'PSR+GPA+3MRM+F_SCORE>=9+20',
    #                  'PER+ROE+6MRM+F_SCORE>=9+20', 'PER+ROA+6MRM+F_SCORE>=9+20', 'PER+GPA+6MRM+F_SCORE>=9+20', 'PBR+ROE+6MRM+F_SCORE>=9+20', 'PBR+ROA+6MRM+F_SCORE>=9+20', 'PBR+GPA+6MRM+F_SCORE>=9+20', 'PSR+ROE+6MRM+F_SCORE>=9+20', 'PSR+ROA+6MRM+F_SCORE>=9+20', 'PSR+GPA+6MRM+F_SCORE>=9+20',
    #                  'PER+ROE+12MRM+F_SCORE>=9+20', 'PER+ROA+12MRM+F_SCORE>=9+20', 'PER+GPA+12MRM+F_SCORE>=9+20', 'PBR+ROE+12MRM+F_SCORE>=9+20', 'PBR+ROA+12MRM+F_SCORE>=9+20', 'PBR+GPA+12MRM+F_SCORE>=9+20', 'PSR+ROE+12MRM+F_SCORE>=9+20', 'PSR+ROA+12MRM+F_SCORE>=9+20', 'PSR+GPA+12MRM+F_SCORE>=9+20'
    #                  ]