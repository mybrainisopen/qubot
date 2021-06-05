import os
import time
import pandas as pd
from pandas import DataFrame
from bs4 import BeautifulSoup, NavigableString
from config.logging_process import Logging_process
from config.selenium_process import Selenium_process
from config.logger import logger

# logger = Logging_process('nasdaq')


class Crawler_BS4(object):
    """crawler 부분"""

    def __init__(self, driver):
        """driver를 받아 BeautifulSoup 객체를 생성한다."""

        self.logger = logger
        self.html = driver.page_source
        self.soup = BeautifulSoup(self.html, "html.parser")
        # logger.info("soup 생성")

    def get_page_html(self):

        try:
            soup = self.soup

            thead = soup.find('thead')
            tbody = soup.findAll('tbody')[1]
        except Exception as error:
            self.logger.info(error)
            return None, None

        return thead, tbody

    def get_data(self, tbody):

        out_list = []

        for tr in tbody.children:

            if isinstance(tr, NavigableString):
                continue

            date = tr.find('td', {'class': 'tb_td'})
            date = date.get_text().strip()
            date = date.replace('.', '-')

            close_point = tr.find('td', {'class': 'tb_td2'})
            close_point = close_point.get_text().strip()
            close_point = close_point.replace(',', '')
            close_point = float(close_point)

            open_point = tr.find('td', {'class': 'tb_td4'})
            open_point = open_point.get_text().strip()
            open_point = open_point.replace(',', '')
            open_point = float(open_point)

            high_point = tr.find('td', {'class': 'tb_td5'})
            high_point = high_point.get_text().strip()
            high_point = high_point.replace(',', '')
            high_point = float(high_point)

            low_point = tr.find('td', {'class': 'tb_td6'})
            low_point = low_point.get_text().strip()
            low_point = low_point.replace(',', '')
            low_point = float(low_point)

            out_list.append((date, close_point, open_point, high_point, low_point))

        return out_list

    def data_to_df(self, df, data_list):

        index = len(df)

        for i in range(len(data_list)):

            date = data_list[i][0]
            close_point = data_list[i][1]
            open_point = data_list[i][2]
            high_point = data_list[i][3]
            low_point = data_list[i][4]

            if date in df[df.columns[0]].values:
                return df, False

            df.at[index, df.columns[0]] = date
            df.at[index, df.columns[1]] = close_point
            df.at[index, df.columns[2]] = open_point
            df.at[index, df.columns[3]] = high_point
            df.at[index, df.columns[4]] = low_point

            index += 1

        return df, True

    def process(self, df):

        _, tbody = self.get_page_html()
        # print(tbody)

        out_list = self.get_data(tbody)
        # print(out_list)

        df = self.data_to_df(df, out_list)

        return df


class World_Stock_Market(object):

    def __init__(self, url, df):

        self.df = df
        self.sp = Selenium_process(url)

    def read_csv(self, load_path):

        try:
            df = pd.read_csv(load_path, encoding='utf-8')
            # logger.info("have file : {} ".format(load_path))
            # load 하면 column 이 하나 더 생겨서 삭제함
            df = df.iloc[:, 1:]
            return df
        except:
            # logger.info("have not file : {} ".format(load_path))
            pass
            return self.df

    def run(self, save_csv):

        # logger.info("run start")

        # driver 로딩
        driver = self.sp.run_chromedriver()
        self.main_page = driver.current_window_handle
        time.sleep(3)

        load_path = os.path.join('data', 'csv_file', save_csv)
        df = self.read_csv(load_path)

        print(df)

        # //*[@id="dayLink1"]
        # //*[@id="dayLink2"]
        # //*[@id="dayPaging"]/a[11]
        # //*[@id="dayPaging"]/a[12]
        # //*[@id="dayPaging"]/a[12]
        max_count_page = 1
        count_page = 0

        stop = False
        while (stop == False):

            cbs4 = None

            # 기본값 range(1, 11)
            for n in range(1, 3):

                n = count_page * 10 + n

                page_xpath = '//*[@id="dayLink{}"]'.format(n)
                page = driver.find_element_by_xpath(page_xpath)

                try:
                    page.click()
                except Exception as error:
                    # logger.info(error)
                    stop = True
                    break

                time.sleep(3)

                self.main_page = driver.current_window_handle

                time.sleep(3)

                try:
                    cbs4 = Crawler_BS4(driver)

                    time.sleep(3)
                except Exception as error:
                    # logger.info(error)
                    stop = True
                    break

                try:
                    df, conti = cbs4.process(df)
                    # logger.info('id="dayLink{}" 의 데이터를 DataFrame 으로 저장함'.format(n))
                except Exception as error:
                    # logger.info(error)
                    stop = True
                    break

                if not conti:
                    stop = True
                    break

                # logger.info('id="dayLink{}" 처리 완료'.format(n))

                time.sleep(3)
                # 여기까지가 for 문 내용

            count_page += 1
            self.logger.info("{} page 종료".format(count_page))

            if count_page >= max_count_page:
                break

            class_name = 'next'
            next_page = driver.find_element_by_class_name(class_name)

            try:
                next_page.click()
            except Exception as error:
                # logger.info(error)
                pass
                break

            time.sleep(3)

            self.main_page = driver.current_window_handle

            time.sleep(3)

        df = df.sort_values(by=self.df.columns[0], ascending=False)
        df = df.reindex(range(len(df)))
        print(df)
        print(df.columns)
        print(df.index)
        # df.to_csv(os.path.join("data", "csv_file", save_csv))
        # logger.info('save csv')

        self.sp.down_chromedriver(driver)
