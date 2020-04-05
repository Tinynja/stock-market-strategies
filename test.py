from modules.finnhub import *
from modules.progress_bar import *
from time import time
import mysql.connector as mysql

pr = Printer()
api = Finnhub(token="bpvto4vrh5rddd65d090", printer=pr)
mydb = mysql.connect(host="192.168.1.2", user="root", passwd="root", database="stock_history")
cursor = mydb.cursor()

for i in range(200):
	pr.prefix = f"{i}: "
	api_data = api.request_atr('MFA', 'D', int(dt.timestamp(dt(2020,3,28,12))), int(time()), timeperiod=2)