import pymongo
print("Welcome to pymongo")
client = pymongo.MongoClient("mongodb://localhost:27017/")
print(client)
