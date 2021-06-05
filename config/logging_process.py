import logging

class Logging_process(object):

    def __init__(self, file_name):
        self.file_name = file_name
        self.logger = logging.getLogger(self.file_name)
        self.logger.setLevel(logging.INFO)
        self.formatter = logging.Formatter('[%(asctime)s][%(levelname)s] %(message)s', '%Y-%m-%d %H:%M')

        #log console 출력
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(self.formatter)
        self.logger.addHandler(console_handler)

        #log file 저장
        file_handler = logging.FileHandler("data\\logs\\test.log", encoding = 'utf-8')
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(self.formatter)
        self.logger.addHandler(file_handler)

    def info(self, message):
        self.logger.info(message)