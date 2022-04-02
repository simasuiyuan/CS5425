import pymongo
import pandas as pd
from typing import Union

class MONGODB_CONNECTOR(object):
    PROJECT_DB = "CS5425"
    PROJECT_TABLE = "image_embedding"

    def __init__(self, user:str="shared_user", token:str="2pZGb4axFMwpl3qR", database:str=PROJECT_DB):
        self.user = user
        self.token = token
        self.database = database
        self.connect()

    def connect(self):
        self.uri = f"mongodb+srv://{self.user}:{self.token}@bigdata.sbz3m.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"
        self.client = pymongo.MongoClient(self.uri)
        self.project_db = self.client[self.database]
        self.project_table = self.project_db[self.PROJECT_TABLE]
        print(f"available databases:{self.client.list_database_names()}")
        print(f"available collections/tables:{self.project_db.list_collection_names()} from {self.PROJECT_DB}")

    def close(self):
        self.client.close()

    def del_table(self, table_name:str):
        x =self.project_db[table_name].delete_many({})
        print(x.deleted_count, " documents deleted.")
    
    def create_insert_table(self, data:Union[dict, pd.DataFrame], table_name:str=PROJECT_TABLE):
        if type(data) == pd.DataFrame: data = data.to_dict("records")
        self.project_db[table_name].insert_many(data)

    def read_table(self, table_name:str=PROJECT_TABLE, type="pandas"): # type = pandas/origin
        if type == "pandas":
            return pd.DataFrame.from_records(self.project_db[table_name].find())
        elif type == "origin":
            return self.project_db[table_name].find()
        else:
            raise TypeError("unknown return type; avaliable types: pandas/origin")
    
    def find(self, query, table_name:str=PROJECT_TABLE, type="pandas"):# type = pandas/origin
        if type == "pandas":
            return pd.DataFrame.from_records(self.project_db[table_name].find(query))
        elif type == "origin":
            return self.project_db[table_name].find(query)
        else:
            raise TypeError("unknown return type; avaliable types: pandas/origin")

    def find_one(self, table_name:str=PROJECT_TABLE, type="pandas"):# type = pandas/origin
        if type == "pandas":
            return pd.DataFrame.from_records([self.project_db[table_name].find_one()])
        elif type == "origin":
            return self.project_db[table_name].find_one()
        else:
            raise TypeError("unknown return type; avaliable types: pandas/origin")