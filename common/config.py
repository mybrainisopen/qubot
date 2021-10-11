# DB 접속 정보
db_ip = 'localhost'
db_port = '3306'
db_id = 'root'
db_pw = 'MBiO85!!'

# Open-DART API Key
dart_key1 = '4f134a1717d81bfa47aff61e06c0bba489e906c6'  # brainopen@naver.com
dart_key2 = '9074f410d12c8a74ec0a67e84588147bcf8c985f'  # mybrainisopen@naver.com
dart_key3 = '6ebaa73ce283fe7e497c3008c351d6f4046efe07'  # qubot@naver.com
dart_key4 = '983eb27498d3b8c74e66b581905d4904616c8cae'  # pleiades0625@gmail.com
dart_key5 = '7f51ea8a8025f0335e1a84ae7886830e0f4e21c0'  # pleiades-js@hanmail.net
dart_key6 = '012993723c7797fed9521400326c0bbeb8379aa6'  # brainopen62@gmail.com
dart_key7 = 'b159f3c3a5ea825bda0247ac8ffba8904b93a6a2'  # mybrainisopen@hanmail.net
dart_key8 = 'c2d1e620450ee2e65a4fc08d4c0cd9149975c507'  # pleiades85@kaist.ac.kr
dart_key9 = '63eb7f64507d4006798eaccdc58ac770001268c2'  # qubot0625@gmail.com
dart_key = [dart_key1, dart_key2, dart_key3, dart_key4, dart_key5, dart_key6, dart_key7, dart_key8, dart_key9]

# User-Agent
user_agent = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.106 Whale/2.8.107.16 Safari/537.36'

# 거래비용 설정
tax_rate = 0.003  # 0.3%
fee_rate = 0.00015  # 0.015%
slippage = 0.003  # 0.3%

# 로그 출력레벨 설정 (DEBUG < INFO < WARNING < ERROR < CRITICAL)
log_lv = "INFO"

# 로그 출력현실
log_print = '[%(asctime)s][%(levelname)s] %(message)s', '%Y-%m-%d %H:%M'

# 경로설정
log_directory = 'C:\\Project\\qubot\\log'


""" 
모든상장종목 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13
코스닥 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=kosdaqMkt
코스피 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=stockMkt
코넥스 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=13&marketType=konexMkt
관리종목 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=01
불성실공시법인 : http://kind.krx.co.kr/corpgeneral/corpList.do?method=download&searchType=05

다우산업 DOW :  https://finance.naver.com/world/sise.nhn?symbol=DJI@DJI // 일자, 종가, 시가, 고가, 저가
나스닥종합 NASDAQ : https://finance.naver.com/world/sise.nhn?symbol=NAS@IXIC // 일자, 종가, 시가, 고가, 저가
S&P500 SNP500 : https://finance.naver.com/world/sise.nhn?symbol=SPI@SPX // 일자, 종가, 시가, 고가, 저가
상해종합 SHANGHAI : https://finance.naver.com/world/sise.nhn?symbol=SHS@000001 // 일자, 종가, 시가, 고가, 저가
니케이225 NIKKEI225 : https://finance.naver.com/world/sise.nhn?symbol=NII@NI225 // 일자, 종가, 시가, 고가, 저가

원달러 USDKRW : https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_USDKRW   // 날짜, 매매기준율
원유로 EURKRW : https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_EURKRW   // 날짜, 매매기준율
원엔 JPYKRW : https://finance.naver.com/marketindex/exchangeDailyQuote.nhn?marketindexCd=FX_JPYKRW     // 날짜, 매매기준율

CD금리 CD : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CD91   // 날짜, 종가
콜금리 CALL : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CALL   // 날짜, 종가
국고채3년 GOVT03Y : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_GOVT03Y   // 날짜, 종가
회사채3년 CORP03Y : https://finance.naver.com/marketindex/interestDailyQuote.nhn?marketindexCd=IRR_CORP03Y   // 날짜, 종가

국내금 KOREA_GOLD : https://finance.naver.com/marketindex/goldDailyQuote.nhn?  // 날짜, 종가(매매기준율)
국제금 WORLD_GOLD: https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=CMDT_GC&fdtc=2   // 날짜, 종가
두바이유 DUBAI_OIL : https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_DU&fdtc=2  // 날짜, 종가
유가WTI WTI : https://finance.naver.com/marketindex/worldDailyQuote.nhn?marketindexCd=OIL_CL&fdtc=2  // 날짜, 종가

구리 Cu : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_CDY   // 날짜, 종가
납 Pb : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_PDY   // 날짜, 종가
아연 Zn : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_ZDY   // 날짜, 종가
니켈 Ni : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_NDY   // 날짜, 종가
알루미늄합금 Al : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_AAY   // 날짜, 종가
주석 Sn : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SDY   // 날짜, 종가

옥수수 corn : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_C   // 날짜, 종가
대두 soybean : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_S   // 날짜, 종가
대두박 soybean_cake : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_SM   // 날짜, 종가
대두유 soybean_oil: https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_BO    // 날짜, 종가
소맥 wheat : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_W   // 날짜, 종가
쌀 rice : https://finance.naver.com/marketindex/worldDailyQuote.nhn?fdtc=2&marketindexCd=CMDT_RR   // 날짜, 종가
"""
