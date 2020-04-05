# Pip libraries
from mysql.connector.cursor import CursorBase
from mysql.connector.cursor_cext import CMySQLCursor

# Basic commands
def executefetch(self, stmt, args=None, singleton=False, unpack=False):
	self.execute(stmt,args)
	data = self.fetchall()
	if len(data) == 1 and (singleton or unpack):
		data = data[0]
		if len(data) == 1 and singleton:
			return data[0]
		else:
			return data
	else:
		return data
setattr(CursorBase, 'executefetch', executefetch)
setattr(CMySQLCursor, 'executefetch', executefetch)

def executemanyfetch(self, stmt, args=None):
	data = []
	for a in args:
		d = self.executefetch(stmt, a, unpack=True)
		if d: data.append(d)
	return data
setattr(CursorBase, 'executemanyfetch', executemanyfetch)
setattr(CMySQLCursor, 'executemanyfetch', executemanyfetch)

def executecount(self, stmt, data=None):
	self.executefetch(stmt,data)
	return self.rowcount
setattr(CursorBase, 'executecount', executecount)
setattr(CMySQLCursor, 'executecount', executecount)

# Higher-level commands
def columninfo(self, table, column, info=None):
	data = self.executefetch(f"""
    SELECT *
    FROM information_schema.columns
		WHERE
			TABLE_SCHEMA = (SELECT DATABASE()) AND
			TABLE_NAME = %s AND
			COLUMN_NAME = %s
		""", (table, column), unpack=True)
	if data and info:
		old_data = data
		data = []
		if isinstance(info, str):
			info = (info,)
		for i in info:
			data.append(old_data[self.column_names.index(i.upper())])
	if len(data) == 1:
		return data[0]
	else:
		return data
setattr(CursorBase, 'columninfo', columninfo)
setattr(CMySQLCursor, 'columninfo', columninfo)

def verifycolumn(self, table, column, data_type, data_type_spec=None, default=None, nullable=True, after=None, printer=None):
	if printer is not None:
		printer.prefix = f"Verifying column {table}.{column}: "
	else:
		print(f"Verifying column {table}.{column}:")
	information_schema = {}
	# Set the strings to be used when adding a column
	strings = {}
	# data_type
	if data_type is not None:
		if data_type.lower() in ("varchar", "decimal"):
			strings["data_type"] = f" {data_type}({data_type_spec})"
		else:
			strings["data_type"] = f" {data_type}"
	else:
		strings["data_type"] = ""
	# default
	if default is not None:
		if isinstance(default, str):
			strings["default"] = f" DEFAULT '{default}'"
		else:
			strings["default"] = f" DEFAULT {default}"
	else:
		strings["default"] = ""
	# nullable
	if nullable:
		strings["nullable"] = " NULL"
	else:
		strings["nullable"] = " NOT NULL"
	# after
	if after is not None:
		strings["after"] = f" AFTER {after}"
	else:
		strings["after"] = ""

	if printer is not None:
		printer.progressprint("Retrieving column info", prefix=True)
	else:
		print("Retrieving column info...")
	# Retrieve column info
	data = self.columninfo(table, column)
	column_names = self.column_names
	# Make changes if changes need to be made
	if data:
		if (data[column_names.index('DATA_TYPE')].lower() != data_type or
				(data_type.lower() == "varchar" and data[column_names.index('CHARACTER_MAXIMUM_LENGTH')] != data_type_spec) or
				(data_type.lower() == "decimal" and [data[column_names.index('NUMERIC_PRECISION')], data[column_names.index('NUMERIC_SCALE')]] != [int(i) for i in data_type_spec.replace(' ','').split(',')]) or
				data[column_names.index('COLUMN_DEFAULT')] != default or
				data[column_names.index('IS_NULLABLE')].lower() != ['no', 'yes'][nullable] or
				(after and self.columninfo(table, after, 'ordinal_position') != data[column_names.index('ORDINAL_POSITION')]-1)):
			if printer is not None:
				printer.progressprint("Modifying column", prefix=True)
			else:
				print("Modifying column...")
			self.execute(f"""
				ALTER TABLE {table}
				CHANGE COLUMN {column} {column}{strings["data_type"]}{strings["nullable"]}{strings["default"]}{strings["after"]}""")
			if printer is not None:
				printer.reprint("OK", prefix=True)
			else:
				print("OK")
		else:
			if printer is not None:
				printer.reprint("OK", prefix=True)
			else:
				print("OK")
	else:
		if printer is not None:
			printer.progressprint("Creating column", prefix=True)
		else:
			print("Creating column...")
		self.execute(f"""
			ALTER TABLE {table}
			ADD COLUMN {column}{strings["data_type"]}{strings["nullable"]}{strings["default"]}{strings["after"]}""")
		if printer is not None:
			printer.reprint("OK", prefix=True)
		else:
			print("OK")
setattr(CursorBase, 'verifycolumn', verifycolumn)
setattr(CMySQLCursor, 'verifycolumn', verifycolumn)