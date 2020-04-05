from modules.finnhub import *
from modules.progress_bar import *
from time import time,sleep
import mysql.connector as mysql

pr = Printer()
api = Finnhub(token="bpvto4vrh5rddd65d090", printer=pr)

pr.progressprint("Test1")
sleep(2)
pr.reprint("Stopped the test.")
sleep(2)
pr.progressprint("Test2")
sleep(2)
pr.rewrite("Done!")

for i in range(100):
	api_data = api.request_atr('MFA', 'D', 1585411200, 1586031555, timeperiod=2)
