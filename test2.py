import pandas as pd
import requests
from bs4 import BeautifulSoup

user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Whale/2.8.107.16 Safari/537.36'
headers = {'User-Agent': user_agent}

# url = 'https://finance.naver.com/item/sise_day.nhn?code=005930'
# webpage = requests.get(url, headers=headers)
# soup = BeautifulSoup(webpage.content, 'html.parser')
# # df = pd.read_html(url, header=0)[0]

# print(soup)


def make_price_dataframe(code, timeframe, count):
    url = 'http://fchart.stock.naver.com/sise.nhn?symbol='+code+'&timeframe=day&count=1500&requestType=0'
    # url = 'http://fchart.stock.naver.com/sise.nhn?symbol=' + code + '&timeframe=day&count=1500&requestType=0'
    price_url = url+'&symbol='+code+'&timeframe'+timeframe+'&count='+count
    price_data = requests.get(price_url)
    price_data_bs = BeautifulSoup(price_data.text, 'lxml')
    item_list = price_data_bs.find_all('item')
    date_list = []
    price_list = []
    for item in item_list:
        temp_data = item['data']
        datas = temp_data.split('|')
        date_list.append(datas[0])
        price_list.append(datas[4])
    price_df = pd.DataFrame({'종가': price_list}, index=date_list)
    return price_df


price_df = make_price_dataframe(code='005930', timeframe='day', count='10')
print(price_df)