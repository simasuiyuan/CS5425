import pandas as pd
import numpy as np
from joblib import load
from sklearn.metrics.pairwise import cosine_similarity

from src.mongoDB_connector import MONGODB_CONNECTOR
from src.AzureCosmos_connector import COSMOS_CONNECTOR


def topKImages(query_embedding, k=20): 
    DB_connector = MONGODB_CONNECTOR(user="shared_user", token="2pZGb4axFMwpl3qR") 
    meta_data = DB_connector.read_table(type="pandas") 

    # load models 
    scaler = load('./models/experiment/scaler.joblib')
    pca = load('./models/experiment/pca.joblib')
    kmeans = load('./models/experiment/kmeans.joblib')
    
    # transform query embedding 
    query_feature = pca.transform(scaler.transform([query_embedding]))
    # get query cluster
    query_cluster = kmeans.predict(query_feature)[0]
    print('Query belongs to cluster {}'.format(query_cluster))

    bert_ecd = meta_data[meta_data["kmeans_cluster_id"] == query_cluster]['bert_ecd'].tolist()
    img_name = meta_data[meta_data["kmeans_cluster_id"] == query_cluster]['img_name'].tolist()

    similarities = cosine_similarity(query_feature, pca.transform(scaler.transform(bert_ecd)))[0]
    # similarities = cosine_similarity([query_embedding], bert_ecd)[0]
    topk_indices = np.argpartition(similarities, -k)[-k:]

    return np.array(img_name)[topk_indices], np.sort(similarities)[-k:]


def topKImages_Azure(query_embedding, k=20): 
    cosmosDB = COSMOS_CONNECTOR()
    emd_db = cosmosDB.get_database("image-embeddings-db")
    metaContainer= cosmosDB.get_container(emd_db, "clustered-meta-data")

    # load models 
    scaler = load('./models/full/scaler.joblib')
    pca = load('./models/full/pca.joblib')
    kmeans = load('./models/full/kmeans.joblib')
    
    # transform query embedding 
    query_feature = pca.transform(scaler.transform([query_embedding]))
    # get query cluster
    query_cluster = kmeans.predict(query_feature)[0]
    print('Query belongs to cluster {}'.format(query_cluster))

    cluster_meta_data = cosmosDB.query_by_clusterId(metaContainer, int(query_cluster), if_pandas=True)

    bert_ecd = cluster_meta_data['encd'].tolist()
    img_name = cluster_meta_data['img_name'].tolist()

    similarities = cosine_similarity(query_feature, pca.transform(scaler.transform(bert_ecd)))[0]
    # similarities = cosine_similarity([query_embedding], bert_ecd)[0]
    topk_indices = np.argpartition(similarities, -k)[-k:]

    return np.array(img_name)[topk_indices], np.sort(similarities)[-k:]
