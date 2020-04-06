# Built-in libraries
from os import listdir
import argparse
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
parser.add_argument("-c", "--nocolumncheck", help="skip checking if necessary columns are in database", action="store_true")
parser.add_argument("-s", "--skipknownerrors", help="skip symbols that have error records in database", action="store_true")
args = parser.parse_args()

# Initiate connection and start printer thread
pr = Printer()
mydb = mysql.connect(host="192.168.1.2", user="root", passwd="root", database="stock_history")
cursor = mydb.cursor()
api = Finnhub(token=args.TOKEN, printer=pr)

# Make sure all columns are created
if not args.nocolumncheck:
	cursor.verifycolumn('stock', 'symbol', 'varchar', 20, default=None, nullable=True, after='id', printer=pr)
	cursor.verifycolumn('stock', 'description', 'varchar', 255, default=None, nullable=True, after='symbol', printer=pr)
	cursor.verifycolumn('stock', 'exchange', 'varchar', 20, default=None, nullable=True, after='description', printer=pr)
	cursor.verifycolumn('history', 'date', 'datetime', default=None, nullable=False, after='id_symbol', printer=pr)
	cursor.verifycolumn('history', 'resolution', 'varchar', 2, default=None, nullable=False, after='date', printer=pr)
	cursor.verifycolumn('history', 'open', 'decimal', '10,3', default=None, nullable=True, after='resolution', printer=pr)
	cursor.verifycolumn('history', 'high', 'decimal', '10,3', default=None, nullable=True, after='open', printer=pr)
	cursor.verifycolumn('history', 'low', 'decimal', '10,3', default=None, nullable=True, after='high', printer=pr)
	cursor.verifycolumn('history', 'close', 'decimal', '10,3', default=None, nullable=True, after='low', printer=pr)
	cursor.verifycolumn('history', 'volume', 'decimal', '10,1', default=None, nullable=True, after='close', printer=pr)
	cursor.verifycolumn('history', 'atr_'+str(args.TIME_PERIOD), 'decimal', '15,8', default=None, nullable=True, printer=pr)
	cursor.verifycolumn('error', 'symbol', 'varchar', 20, default=None, nullable=False, after='id', printer=pr)
	cursor.verifycolumn('error', 'resolution', 'varchar', 2, default=None, nullable=True, after='symbol', printer=pr)
	cursor.verifycolumn('error', 'start', 'datetime', default=None, nullable=True, after='resolution', printer=pr)
	cursor.verifycolumn('error', 'end', 'datetime', default=None, nullable=True, after='start', printer=pr)
	cursor.verifycolumn('error', 'time_period', 'int', default=None, nullable=True, after='end', printer=pr)
	cursor.verifycolumn('error', 'message', 'varchar', 255, default=None, nullable=True, after='time_period', printer=pr)

query_exchanges = {'TSX':'TO', 'TSXV':'V', 'US':'US'}
err_count = 0

for exchange in query_exchanges:
	# Check if stock exists in 
	stocks = api.get_stocks(exchange=query_exchanges[exchange])
	if stocks[0] == 0:
		stocks = stocks[1]
		symbols_indb = cursor.executefetch(f"""SELECT symbol FROM stock WHERE symbol in ({','.join(['%s']*len(stocks))}) and exchange = '{exchange}'""", [s['symbol'] for s in stocks])
		if symbols_indb:
			symbols_indb = [d[0] for d in symbols_indb]
		for s in stocks:
			symbol = s['symbol']
			if ((args.all or
					symbol not in symbols_indb or
					not cursor.executecount(f"""
						SELECT atr_{args.TIME_PERIOD}
						FROM history
						WHERE
							id_symbol = (SELECT id FROM stock WHERE symbol = %s) and
							resolution = %s
						LIMIT 1""", (symbol,args.RESOLUTION)))
					and not (args.skipknownerrors and cursor.executefetch("SELECT COUNT(*) FROM error WHERE symbol = %s", (symbol,), singleton=True))):
				pr.prefix = f"Processing ATR({args.TIME_PERIOD}) for {symbol} ({exchange}): "
				# Retrieve data from api
				api_data = api.get_atr(
					symbol,
					'D',
					int(dt.timestamp(args.START)),
					int(dt.timestamp(args.END)),
					timeperiod=args.TIME_PERIOD)
				# Check if data exists in database
				if api_data[0] == 0:
					# Verify stock information in database
					pr.progressprint("Verifying stock information", prefix=True)
					known_ids = cursor.executefetch("SELECT id FROM stock WHERE symbol = %s", (symbol,), singleton=True)
					if known_ids:
						cursor.execute("UPDATE stock SET exchange = %s, description = %s WHERE id = %s", (exchange, s['description'], known_ids))
					else:
						cursor.execute("INSERT INTO stock (symbol, description, exchange) VALUES (%s, %s, %s)", (symbol, s['description'], exchange))
					pr.progressprint("Checking existing data in database", prefix=True)
					api_data = api_data[1]
					known_ids = cursor.executefetch((
						"SELECT id,id_symbol,date,resolution "
						"FROM history "
						"WHERE "
							f"date IN ({','.join(['%s']*len(api_data))}) and "
							f"id_symbol = (SELECT id FROM stock WHERE symbol = '{symbol}') and "
							f"resolution = '{args.RESOLUTION}'"), [t for t in api_data])
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
						for d in known_ids: api_data.pop(d[2])
					pr.progressprint("Inserting new data into database", prefix=True)
					cursor.executemany(f"""
						INSERT INTO history (id_symbol, date, resolution, open, high, low, close, volume, atr_{args.TIME_PERIOD})
						VALUES ((SELECT id FROM stock WHERE symbol = '{symbol}'), %s, '{args.RESOLUTION}', %s, %s, %s, %s, %s, %s)""",
						[[t] + api_data[t] for t in api_data])
					pr.reprint("Success.", prefix=True)
				else:
					err_count += 1
					cursor.execute(f"""
						INSERT INTO error (symbol, resolution, start, end, time_period, message)
						VALUES (%s, %s, %s, %s, %s, %s)""",
						(symbol, args.RESOLUTION, args.START, args.END, args.TIME_PERIOD, api_data[2]))
				mydb.commit()
			else:
				pr.print(f"Skipped {symbol} ({exchange}).")
	else:
		pr.print(stocks[0])

pr.print("Done!" + ["", f" {err_count} errors occurred and have been saved to database."][err_count>0])

# Cleanup
mydb.close()