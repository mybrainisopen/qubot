import scraper.scrap_all
import analyzer.analyze_all
import backtester.backtester
import os
# import logging
import sys
from datetime import datetime

today = datetime.today().strftime('%Y%m%d')

os.system('chcp 65001')  # os 모듈 사용시 한글 깨짐 방지
sys.stdout = open(f'C:\\Project\\qubot\\log\\log_{today}.txt', 'w')

scraper = scraper.scrap_all.ScrapAll()
analyzer = analyzer.analyze_all.AnalyzeAll()
# backtester = backtester.backtester.backtester()

scraper.scrap_check()
analyzer.analysis_check()
# backtester.initialize_db()
# backtester.executor()

os.system('Pause')