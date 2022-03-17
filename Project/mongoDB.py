#%%
import pymongo
import pandas as pd
# %%
password="2pZGb4axFMwpl3qR"

client = pymongo.MongoClient(f"mongodb+srv://shared_user:{password}@bigdata.sbz3m.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client.test
# %%
# create database
# mydb = client["CS5425"]
# print(client.list_database_names())
# """MongoDB waits until you have created a collection (table), 
# with at least one document (record) before it actually creates the database (and collection).
# """
# mycol = mydb["image_embedding"]
# image_dataset = pd.read_csv("./data/ig_data.csv").to_dict('records')
# x = mycol.insert_many(image_dataset)
print(client.list_database_names())
print(mydb.list_collection_names())
# %%
#print list of the _id values of the inserted documents:
print(x.inserted_ids)
# %%
print(mycol.find_one())

#%%