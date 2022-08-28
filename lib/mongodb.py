import pymongo
from lib.config_handler import handler
from pymongo import MongoClient

class operations():
    #Set Database Connection
    def __init__(self):
        host = handler('database','host')
        port = handler('database','port')
        self.conn = MongoClient(host, port)

    def insert_one(self,dbname,colname,data):
        db = self.conn[dbname]
        col = db[colname]
        col.insert(data)
        return True

    #Find Data Into The Collection
    def find_data(self, dbname, colname):
        db = self.conn[dbname]
        col = db[colname]
        records = col.find()
        return records

    #Insert an array of records, inserted return true else false
    def bulk_insert(self,dbname,colname,data_array):
        db = self.conn[dbname]
        col = db[colname]
        col.insert_many(data_array)
        return True

    #Find query of mongodb return an json
    def find_in_mongo(self,dbname,colname,condition):
        db = self.conn[dbname]
        col = db[colname]
        records = col.find(condition)
        return records

    #Update database with json condition
    def update_to_mongo(self,dbname,colname,condition,data):
        db = self.conn[dbname]
        col = db[colname]
        col.update(condition,{"$set":data},upsert=True)
        return True

    #Single record update
    def update_it(self, dbname, colname, condition, data):
        db = self.conn[dbname]
        col = db[colname]
        col.update(condition, {"$set":data})
        return True

    #Close Connection
    def closeConnection(self):
        self.conn.close()
