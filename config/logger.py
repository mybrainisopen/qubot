import logging
import config.setting as setting
from datetime import datetime

# 오늘날짜변수 생성
today = datetime.today().strftime('%Y%m%d')

# 로그 생성
logger = logging.getLogger()

# 로그의 출력 기준 설정 (DEBUG < INFO < WARNING < ERROR < CRITICAL)
if setting.log_lv == 'DEBUG':
    logger.setLevel(logging.DEBUG)
elif setting.log_lv == 'INFO':
    logger.setLevel(logging.INFO)
elif setting.log_lv == 'WARNING':
    logger.setLevel(logging.WARNING)
elif setting.log_lv == 'ERROR':
    logger.setLevel(logging.ERROR)
elif setting.log_lv == 'CRITICAL':
    logger.setLevel(logging.CRITICAL)

# log 출력 형식
formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s', '%Y-%m-%d %H:%M')

# log 출력
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# log를 파일에 출력
file_handler = logging.FileHandler(f'C:\\Project\\qubot\\log\\log_{today}.txt')  # 인코딩 옵션 줄 수 있음
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)




