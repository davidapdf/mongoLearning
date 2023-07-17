import json
from pymongo import MongoClient, InsertOne

class Database(object):
    string = open("connectString.txt","r")
    URI = string.readline()
    DATABASE = None

    @staticmethod
    def initialize(dataBase):
        client = MongoClient(Database.URI)
        Database.DATABASE = client[dataBase]
    
    @staticmethod
    def insert(collection,data):
        Database.DATABASE[collection].insert(data)

    @staticmethod
    def insertBulk(collection,dataJson):
        requesting = []
        for jsonObj in dataJson:
            myDict = json.loads(jsonObj)
            requesting.append(InsertOne(myDict))
        Database.DATABASE[collection].bulk_write(requesting)

    @staticmethod
    def find(collection, query):
        return Database.DATABASE[collection].find(query)
    
    @staticmethod
    def find_one(collection):
        return Database.DATABASE[collection].find_one()
    
    @staticmethod
    def drop(collection):
        return Database.DATABASE[collection].drop()
    
    @staticmethod
    def list_collection():
        return Database.DATABASE.list_collection_names()
    
    @staticmethod
    def count_result(collection, query):
        return Database.DATABASE[collection].count_documents(query)
    
    @staticmethod
    def aggregate_result(collection, query):
        return Database.DATABASE[collection].aggregate(query)
    