# -*- coding: utf-8 -*-
from pywinauto import application
import time
import os

os.system('taskkill /IM coStarter* /F /T')
os.system('taskkill /IM CpStart* /F /T')
os.system('wmic process where "name like \'%coStarter%\'" call terminate')
os.system('wmic process where "name like \'%CpStart%\'" call terminate')
time.sleep(5)

app = application.Application()
app.start('C:\CREON\STARTER\coStarter.exe /prj:cp /id:mbio0625 /pwd:genius8% /pwdcert:genius85!! /autostart')  # id / pw/ 공인인증서pw
time.sleep(60)
