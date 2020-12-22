import scraper.scrap_all
import analyzer.analyze_all

scraper = scraper.scrap_all.scrap_all()
analyzer = analyzer.analyze_all.analyze_all()

scraper.scrap_check()
analyzer.analysis_check()
