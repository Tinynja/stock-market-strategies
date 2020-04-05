from modules.finnhub import *
from modules.progress_bar import *
from time import time,sleep
import mysql.connector as mysql

pr = Printer()

pr.progressprint("Test1")
sleep(2)
pr.reprint("Stopped the test.")
sleep(2)
pr.progressprint("Test2")
sleep(2)
pr.rewrite("Done!")