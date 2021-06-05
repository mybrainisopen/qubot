import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from config.logging_process import Logging_process
# logger = Logging_process("selenium_process")

class Selenium_process(object):

    def __init__(self, url):
        self.url = url
        self.chromedriver_path = os.path.join("libs", "chromedriver_win32", "chromedriver.exe")
        # logger.info(self.chromedriver_path)

    def run_chromedriver(self):
        try:
            driver = webdriver.Chrome(self.chromedriver_path)
            driver.get(self.url)
            # logger.info("Selenium driver 생성")
        except Exception as error:
            # logger.info(error)
        return driver

    def down_chromedriver(self, driver):
        driver.quit()

    def find_by_id(self, driver, click_id):
        find_id = driver.find_element_by_id(click_id)
        return find_id

    def find_list_by_id(self, driver, click_id):
        list_by_id = driver.find_elements_by_id(click_id)
        return list_by_id

    def find_list_by_css(self, driver, click_css):
        list_by_css = driver.find_elements_by_css_selector(click_css)
        return list_by_css

    def find_by_class(self, driver, class_name):
        find_class = driver.find_element_by_class_name(class_name)
        return find_class

    def find_list_by_class(self, driver, class_name):
        list_by_class = driver.find_elements_by_class_name(class_name)
        return list_by_class