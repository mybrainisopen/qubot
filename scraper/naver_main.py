import pandas as pd
from pandas import DataFrame

# from doller import Naver_finance_crawler
from nasdaq import World_Stock_Market

# 달러 환율
# url = "https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_USDKRW"
# df = DataFrame(columns = ['day', '매매기준율'])
# nfc = Naver_finance_crawler(url, df)
# save_csv = 'doller.csv'
# nfc.run(save_csv)

# 국내 금 시세
# url = "https://finance.naver.com/marketindex/goldDailyQuote.nhn?"
# df = DataFrame(columns = ['day', '매매기준율'])
# nfc = Naver_finance_crawler(url, df)
# save_csv = 'gold.csv'
# nfc.run(save_csv)

# 유가 시세 (WTI)
# url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_CL&fdtc=2"
# df = DataFrame(columns = ['day', '종가'])
# nfc = Naver_finance_crawler(url, df)
# save_csv = 'oil.csv'
# nfc.run(save_csv)

# 구리
# url = "https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_CDY"
# df = DataFrame(columns = ['day', '종가'])
# nfc = Naver_finance_crawler(url, df)
# save_csv = 'Cu.csv'
# nfc.run(save_csv)

# 나스닥
url = "https://finance.naver.com/world/sise.nhn?symbol=NAS@IXIC"
df = DataFrame(columns = ['day', '종가', '시가', '고가', '저가'])
wsm = World_Stock_Market(url, df)
# save_csv = 'nasdaq.csv'
# wsm.run(save_csv)