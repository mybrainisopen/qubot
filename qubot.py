import scraper.scrap_all
import analyzer.analyze_all
import backtester.backtester
import os

scraper = scraper.scrap_all.scrap_all()
analyzer = analyzer.analyze_all.analyze_all()
# backtester = backtester.backtester.backtester()

scraper.scrap_check()
analyzer.analysis_check()
# backtester.initialize_db()
# backtester.executor()

os.system('Pause')