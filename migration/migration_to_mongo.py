from lib.mongodb import operations
from lib.config_handler import handler

#obj = operations()
dbname = handler('mongo','dbname')
colname = handler('mongo', 'colname')
print(dbname)
print(colname)

