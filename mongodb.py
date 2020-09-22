
from pymongo import MongoClient


class MongoManager:
    __instance = None

    @staticmethod
    def getInstance():
        if MongoManager.__instance == None:
            MongoManager()
        return MongoManager.__instance

    def __init__(self):
        if MongoManager.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            from configparser import ConfigParser
            configparser = ConfigParser()
            configparser.read('.config')
            address = configparser.get('mongodb', "address")
            MongoManager.__instance = MongoClient(address)



