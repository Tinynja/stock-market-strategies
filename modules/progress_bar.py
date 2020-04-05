# Built-in libraries
import threading
from itertools import cycle

class Printer():
	def __init__(self):
		self.finished_event = threading.Event()
		self.finished = True
		self.current_output = ""
		self.prefix = ""
	
	def rewrite(self, text, prefix=False):
		self.clearline()
		if prefix:
			print(self.prefix + str(text), end = '', flush=True)
			self.current_output = self.prefix + str(text)
		else:
			print(text, end = '')
			self.current_output = str(text)
	
	def reprint(self, *args, prefix=False):
		self.clearline()
		if prefix:
			args = list(args)
			print(self.prefix + str(args.pop(0)), *args)
		else:
			print(*args)
		self.current_output = ""
	
	def print(self, *args, prefix=False):
		self.progressstop()
		if self.current_output:
			print()
		if prefix:
			args = list(args)
			print(self.prefix + str(args.pop(0)), *args)
		else:
			print(*args)
		self.current_output = ""
	
	def clearline(self):
		self.progressstop()
		if self.current_output:
			print('\r' + ' '*len(self.current_output) + '\r', end='', flush=True)

	def progressprint(self, text, prefix=False):
		self.clearline()
		self.progress_bar_thread = threading.Thread(target=self.run_progress_bar, args=(text,prefix))
		self.progress_bar_thread.start()
		self.finished = False
		self.current_output = self.prefix + text + '....'

	def progressstop(self):
		if not self.finished:
			self.finished_event.set()
			self.progress_bar_thread.join()
			self.finished_event.clear()
			self.finished = True

	def run_progress_bar(self, text, prefix):
		chars = cycle(('.   ','..  ','... ', '....'))
		while not self.finished_event.is_set():
			if prefix:
				current_output = self.prefix + text + next(chars)
			else:
				current_output = text + next(chars)
			print('\r' + current_output, end='', flush=True)
			self.finished_event.wait(0.2)