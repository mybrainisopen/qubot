import pymysql
import config as cf
import pandas as pd
pymysql.install_as_MySQLdb()


class db_sql():
    def __init__(self):
        '''생성자 : DB 접속'''
        self.conn = pymysql.connect(host=cf.db_ip,
                                    port=int(cf.db_port),
                                    user=cf.db_id,
                                    password=cf.db_pw,
                                    charset='utf8mb4',
                                    cursorclass=pymysql.cursors.DictCursor)
        self.cur = self.conn.cursor()

    def __del__(self):
        '''소멸자 : DB 접속 해제'''
        self.conn.close()

    def exist_db(self, db_name):
        '''DB SCHEMA 존재 확인 '''
        sql = f"SELECT 1 FROM Information_schema.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'"
        if self.cur.execute(sql):
            print(f"{db_name} DB 존재")
            return True
        else:
            print(f"{db_name} DB 존재하지 않음")
            return False

    def create_db(self, db_name):
        '''DB SCHEMA 생성'''
        if not self.exist_db(db_name):
            sql = f"CREATE DATABASE {db_name}"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"{db_name} DB 생성 완료")

    def drop_db(self, db_name):
        '''DB SCHEMA 삭제'''
        if self.exist_db(db_name):
            sql = f"DROP DATABASE {db_name}"
            self.cur.execute(sql)
            self.conn.commit()
            print(f"{db_name} DB 삭제 완료")

    def exist_db_tbl(self, db_name, tbl_name):
        '''DB.TABLE 존재 확인'''
        sql = f"SELECT 1 FROM information_schema.tables WHERE table_schema = '{db_name}' and table_name = '{tbl_name}'"
        if self.cur.execute(sql):
            print(f"{db_name}.{tbl_name} 존재")
            return True
        else:
            print(f"{db_name}.{tbl_name} 존재하지 않음")
            return False

    def drop_db_tbl(self, db_name, tbl_name):
        '''DB.TABLE 삭제'''
        sql = f"DROP TABLE {db_name}.{tbl_name}"
        self.cur.execute(sql)
        self.conn.commit()
        print(f"{db_name}.{tbl_name} 삭제 완료")

    def drop_db_tbl_col(self, db_name, tbl_name, col_name):
        '''DB.TABLE.COLUMN 삭제'''
        sql = f"SELECT 1 FROM information_schema.columns " \
              f"WHERE table_schema = '{db_name}' and table_name = '{tbl_name}' and column_name='{col_name}'"
        if self.cur.execute(sql):
            print(f"{db_name}.{tbl_name}.{col_name} 존재")
            sql = f"ALTER TABLE {db_name}.`{tbl_name}` DROP {col_name}"
            self.cur.execute(sql)
            self.conn.commit()
        else:
            print(f"{db_name}.{tbl_name}.{col_name} 존재하지 않음")
            pass
        # print(f"{db_name}.{tbl_name}.{col_name} 삭제 완료")
        
    def init_db_tbl_col(self, db_name, tbl_name, col_name):
        '''DB.TABLE.COLUMN 초기화'''
        sql = f"UPDATE {db_name}.{tbl_name} SET {col_name}=NULL"
        self.cur.execute(sql)
        self.conn.commit()
        print(f"{db_name}.{tbl_name}.{col_name} 초기화 완료")

    def test_query(self, query):
        sql = query
        self.cur.execute(sql)
        result = self.cur.fetchone()
        return result

    def drop_all_diff(self):
        '''모든 일일주가 테이블의 diff 컬럼 삭제'''
        # 종목 리스트 가져오기
        sql = "SELECT code, stock from status.scrap_stock_status"
        self.cur.execute(sql)
        stock_list = self.cur.fetchall()
        stock_list = pd.DataFrame(stock_list)
        # print(stock_list)

        for idx in range(len(stock_list)):
            stock = stock_list['stock'][idx]
            self.drop_db_tbl_col(db_name='daily_price', tbl_name=stock, col_name='diff')



if __name__ == "__main__":
    sdb = db_sql()
    # sdb.drop_db_tbl(db_name='backtest_book', tbl_name='`per+roe`')
    # sdb.drop_db_tbl(db_name='backtest_result', tbl_name='evaluation')
    # sdb.drop_db(db_name='backtest_book')
    # sdb.drop_db(db_name='backtest_portfolio')
    # sdb.drop_db(db_name='backtest_result')
    # sdb.init_db_tbl_col(db_name='status', tbl_name='analyze_all_status', col_name='universe_analyzed')
    # sdb.init_db_tbl_col(db_name='status', tbl_name='analyze_stock_status', col_name='fundamental_analyzed')
    # sdb.init_db_tbl_col(db_name='status', tbl_name='analyze_stock_status', col_name='valuation_analyzed')
    sdb.drop_all_diff()
    print("테스트 완료")
