# Built-in libraries
from urllib.request import urlopen
from urllib.error import HTTPError
from time import time,sleep
import json
from datetime import datetime as dt
from decimal import Decimal

class Finnhub:
	def __init__(self, token="", ratelimit=90, printer=None):
		self.token = token

		self.ratelimit = ratelimit
		self.ratelimit_remaining = self.ratelimit
		self.ratelimit_resettime = time()
		self.burstlimit = 30
		self.burstlimit_remaining = self.burstlimit
		self.burstlimit_resettime = time()

		self.printer = printer

	def request_atr(self, symbol, resolution, start, end, timeperiod=14):
		resp = self._request_atr(symbol, resolution, start, end, timeperiod)
		if resp[0] == 0:
			self.printer.rewrite("Successfully retrieved data from Finnhub.", prefix=True)
		else:
			self.printer.reprint(f"Requesting data from Finnhub.... ERROR: {resp[2]}", prefix=True)
		return resp

	def _request_atr(self, symbol, resolution, start, end, timeperiod):
		# Requests ATR values, and returns the raw reponse as a json dictionnary
		# Error codes:
		#   2 := HTTPError
		#   3 := Invalid API key
		#   4 := API limit reached
		#   5 := no_data
		#   6 := sTimeperiod is too long for serie
		weight = 1
		
		self.printer.progressprint(f"Waiting for ratelimit reset ({dt.fromtimestamp(self.ratelimit_resettime)})", prefix=True)
		self.ratelimit_wait(weight)
		self.printer.progressprint("Requesting data from Finnhub", prefix=True)
		try:
			resp = urlopen(f"http://finnhub.io/api/v1/indicator?indicator=atr&symbol={symbol}&resolution={resolution}&from={start}&to={end}&timeperiod={timeperiod}&token={self.token}")
		except HTTPError as err:
			resp = err.fp

		if resp.status == 200:
			self.ratelimit_remaining = int(resp.getheader("x-ratelimit-remaining"))
			self.ratelimit_resettime = int(resp.getheader("x-ratelimit-reset"))
			resp = resp.read().decode('utf-8')
			if resp.find("{") != -1:
				resp = json.loads(resp)#, parse_float=Decimal)
				if resp["s"] == "ok":
					resp['t'] = [dt.fromtimestamp(resp['t'][i]).strftime("%Y-%m-%d %H:%M:%S") for i in range(len(resp['t']))]
					resp['atr'] = [None if atr == 0 else atr for atr in resp['atr']]
					i = 0
					for d in [list(a) for a in zip(resp['o'], resp['h'], resp['l'], resp['c'], resp['v'], resp['atr'])]:
						resp[resp['t'][i]] = d
						i += 1
					for k in ['s', 't', 'o', 'h', 'l', 'c', 'v', 'atr']:
						resp.pop(k)
					return 0, resp, "OK"
				elif resp["s"] == "no_data":
					return 5, resp, "no_data"
			else:
				return 6, resp, "Timeperiod is too long for series"
		else:
			data = resp.read().decode('utf-8').lower()
			if "api limit reached" in data:
				return 4, resp.fp, "API limit reached"
			elif "invalid api key" in data:
				return 3, resp.fp, "Invalid API key"
			else:
				return 2, resp, "HTTPError"

	def ratelimit_wait(self, weight):
		# Continuously check for ratelimit until request weight is allowed
		while not self.ratelimit_check(weight):
			sleep(1)
	
	def ratelimit_check(self, weight=0):
		 # In order to have an accurate check of the ratelimit, we
		 # must update it first, otherwise, the information is outdated.
		 # Since this is only a check, weight should not be passed to ratelimit_update.
		self.ratelimit_update()
		if (self.ratelimit_remaining-weight >= 0) and (self.burstlimit_remaining-1 >= 0):
			return True
		else:
			return False

	def ratelimit_update(self, weight=None, resettime=None):
		# Resets ratelimits based on current time and sets a dummy resettime
		# to avoid a potential update loop. Also serves to 
		time_current = time()
		if (time_current > self.ratelimit_resettime):
			self.ratelimit_remaining = self.ratelimit
			if resettime is None:
				self.ratelimit_resettime += 65
		if time_current > self.burstlimit_resettime:
			self.burstlimit_remaining = self.burstlimit
			self.burstlimit_resettime += 2
		if weight is not None:
			self.ratelimit_remaining -= weight
			self.burstlimit_remaining -= weight > 0
		if resettime is not None:
			self.ratelimit_resettime = resettime