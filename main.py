# Built-in libraries
from os import listdir
from csv import DictReader
import argparse
import re
from datetime import datetime as dt
from time import time

# Pip libraries
import mysql.connector as mysql

# User libraries
from modules.finnhub import *
import modules.cursor_methods
from modules.progress_bar import *

# Argument parser to determine what the script should do
parser = argparse.ArgumentParser()
parser.add_argument("TOKEN", help="finnhub Stock API token")
parser.add_argument("RESOLUTION", help="duration of each datapoint (1, 5, 15, 30, 60, D, W, M)")
parser.add_argument("START", help="datetime of the first datapoint (YYYY-mm-dd HH:MM:SS)", type=lambda s: dt.strptime(s, "%Y-%m-%d %H:%M:%S"))
parser.add_argument("END", help="datetime of the last datapoint (YYYY-mm-dd HH:MM:SS)", type=lambda s: dt.strptime(s, "%Y-%m-%d %H:%M:%S"))
parser.add_argument("TIME_PERIOD", help="time period to use for ATR indicator")
parser.add_argument("-a", "--all", help="don't skip stocks already saved in db", action="store_true")
args = parser.parse_args()

# Initiate connection and start printer thread
pr = Printer()
mydb = mysql.connect(host="192.168.1.2", user="root", passwd="root", database="stock_history")
cursor = mydb.cursor()
api = Finnhub(token=args.TOKEN, printer=pr)

# Make sure all columns are created
cursor.verifycolumn('stock', 'symbol', 'varchar', 10, default=None, nullable=True, after='id', printer=pr)
cursor.verifycolumn('stock', 'exchange', 'varchar', 10, default=None, nullable=True, after='symbol', printer=pr)
cursor.verifycolumn('history', 'date', 'datetime', default=None, nullable=False, after='id_symbol', printer=pr)
cursor.verifycolumn('history', 'resolution', 'varchar', 2, default=None, nullable=False, after='date', printer=pr)
cursor.verifycolumn('history', 'open', 'decimal', '10,3', default=None, nullable=True, after='resolution', printer=pr)
cursor.verifycolumn('history', 'high', 'decimal', '10,3', default=None, nullable=True, after='open', printer=pr)
cursor.verifycolumn('history', 'low', 'decimal', '10,3', default=None, nullable=True, after='high', printer=pr)
cursor.verifycolumn('history', 'close', 'decimal', '10,3', default=None, nullable=True, after='low', printer=pr)
cursor.verifycolumn('history', 'volume', 'decimal', '10,1', default=None, nullable=True, after='close', printer=pr)
cursor.verifycolumn('history', 'atr_'+str(args.TIME_PERIOD), 'decimal', '15,8', default=None, nullable=True, printer=pr)
cursor.verifycolumn('error', 'resolution', 'varchar', 2, default=None, nullable=True, after='id_symbol', printer=pr)
cursor.verifycolumn('error', 'start', 'datetime', default=None, nullable=True, after='resolution', printer=pr)
cursor.verifycolumn('error', 'end', 'datetime', default=None, nullable=True, after='start', printer=pr)
cursor.verifycolumn('error', 'time_period', 'int', default=None, nullable=True, after='end', printer=pr)
cursor.verifycolumn('error', 'message', 'varchar', 255, default=None, nullable=True, after='time_period', printer=pr)

err_count = 0

for f_name in listdir('Exchanges'):
	# Check if stock exists in 
	exchange = re.findall('.+(?=\.)', f_name)[0]
	with open("Exchanges/"+f_name, "r", newline="") as f:
		reader = DictReader(f, delimiter="\t")
		symbols = [row["Symbol"] for row in reader]
		symbols_indb = cursor.executefetch(f"""SELECT symbol FROM stock WHERE symbol in ({','.join(['%s']*len(symbols))}) and exchange = '{exchange}'""", symbols)
		if symbols_indb:
			symbols_indb = [d[0] for d in symbols_indb]
		for s in symbols:
			if (args.all or
					s not in symbols_indb or
					not cursor.executecount(f"""
						SELECT atr_{args.TIME_PERIOD}
						FROM history
						WHERE
							id_symbol = (SELECT id FROM stock WHERE symbol = %s) and
							resolution = %s
						LIMIT 1""", (s,args.RESOLUTION))):
				pr.prefix = f"Processing ATR({args.TIME_PERIOD}) for {s} ({exchange}): "
				# Verify stock information in database
				pr.progressprint("Verifying stock information", prefix=True)
				known_ids = cursor.executefetch("SELECT id FROM stock WHERE symbol = %s", (s,), singleton=True)
				if known_ids:
					cursor.execute("UPDATE stock SET exchange = %s WHERE id = %s", (exchange, known_ids))
				else:
					cursor.execute("INSERT INTO stock (symbol, exchange) VALUES (%s, %s)", (s, exchange))
				# Retrieve data from api
				api_data = api.request_atr(
					s,
					'D',
					int(dt.timestamp(args.START)),
					int(dt.timestamp(args.END)),
					timeperiod=args.TIME_PERIOD)
				# Check if data exists in database
				if api_data[0] == 0:
					pr.progressprint("Checking existing data in database", prefix=True)
					api_data = api_data[1]
					# known_ids = cursor.executemanyfetch(f"SELECT id,id_symbol,date FROM history WHERE date = %s AND resolution = %s", [(t, args.RESOLUTION) for t in api_data])
					known_ids = cursor.executefetch(f"""
						SELECT id,id_symbol,date,resolution
						FROM history
						WHERE
							date IN ({','.join(['%s']*len(api_data))}) and
							id_symbol = (SELECT id FROM stock WHERE symbol = '{s}') and
							resolution = '{args.RESOLUTION}'""", [t for t in api_data])
					if known_ids:
						pr.progressprint("Updating existing data in database", prefix=True)
						known_ids = [(d[0], d[1], d[2].strftime("%Y-%m-%d %H:%M:%S"), d[3]) for d in known_ids]
						cursor.executemany(f"""
							INSERT INTO history (id, id_symbol, date, resolution, open, high, low, close, volume, atr_{args.TIME_PERIOD})
							VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
							ON DUPLICATE KEY UPDATE
								open = VALUES(open),
								high = VALUES(high),
								low = VALUES(low),
								close = VALUES(close),
								volume = VALUES(volume),
								atr_{args.TIME_PERIOD} = VALUES(atr_{args.TIME_PERIOD})""",
								[d + tuple(api_data[d[2]]) for d in known_ids])
						# cursor.executemany(f"""
						# 	UPDATE history 
						# 	SET
						# 		id_symbol = (SELECT id FROM stock WHERE symbol = '{s}'),
						# 		open = %s,
						# 		high = %s,
						# 		low = %s,
						# 		close = %s,
						# 		volume = %s,
						# 		atr_{args.TIME_PERIOD} = %s
						# 	WHERE id = %s;""", [api_data[d[2]] + [d[0]] for d in known_ids])
						# Remove data that has been updated in the database from api_data
						for d in known_ids: api_data.pop(d[2])
					pr.progressprint("Inserting new data into database", prefix=True)
					cursor.executemany(f"""
						INSERT INTO history (id_symbol, date, resolution, open, high, low, close, volume, atr_{args.TIME_PERIOD})
						VALUES ((SELECT id FROM stock WHERE symbol = '{s}'), %s, '{args.RESOLUTION}', %s, %s, %s, %s, %s, %s)""",
						[[t] + api_data[t] for t in api_data])
					pr.reprint("Success.", prefix=True)
				else:
					err_count += 1
					cursor.execute(f"""
						INSERT INTO error (id_symbol, resolution, start, end, time_period, message)
						VALUES ((SELECT id FROM stock WHERE symbol = %s), %s, %s, %s, %s, %s)""",
						(s, args.RESOLUTION, args.START, args.END, args.TIME_PERIOD, api_data[2]))
					# Do some error handling stuff
				mydb.commit()
			else:
				pr.print(f"Skipped {s} ({exchange}).")
pr.print("Done!" + ["", f" {err_count} errors occurred and have been saved to database."][err_count>0])

# Cleanup
mydb.close()