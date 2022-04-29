import azure.functions as func
import logging
import numpy as np
import pandas as pd
from joblib import load
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from .AzureCosmos_connector import COSMOS_CONNECTOR

# This function is hardcoded to return top 10 results
def main(req: func.HttpRequest) -> func.HttpResponse:

    logging.info('Python HTTP trigger function processed a request.')

    query = req.params.get('query')

    if query:

        cosmosDB = COSMOS_CONNECTOR()
        emd_db = cosmosDB.get_database("image-embeddings-db")
        metaContainer= cosmosDB.get_container(emd_db, "clustered-meta-data")

        # Loading bert model from local fileshare
        sbert_model = SentenceTransformer("./processQuery/bert/") 
        query_embedding = sbert_model.encode(query) # Return encoded array

        # load models 
        scaler = load('./processQuery/models/scaler.joblib')
        pca = load('./processQuery/models/pca.joblib')
        kmeans = load('./processQuery/models/kmeans.joblib')

        # transform query embedding
        query_feature = pca.transform(scaler.transform([query_embedding]))
        # get query cluster
        query_cluster = kmeans.predict(query_feature)[0]
        # print('Query belongs to cluster {}'.format(query_cluster))

        cluster_meta_data = cosmosDB.query_by_clusterId(metaContainer, int(query_cluster), if_pandas=True)

        bert_ecd = cluster_meta_data['encd'].tolist()
        img_name = cluster_meta_data['img_name'].tolist()

        similarities = cosine_similarity(query_feature, pca.transform(scaler.transform(bert_ecd)))[0]
        # similarities = cosine_similarity([query_embedding], bert_ecd)[0]
        topk_indices = np.argpartition(similarities, -10)[-10:]

        result = np.array(img_name)[topk_indices]
        return func.HttpResponse("\n".join(str(x) for x in ["https://cs5425imagedata.blob.core.windows.net/images/" + image + ".jpg" for image in result]))
    else:
        return func.HttpResponse(
             "Please provide a query in the request params!",
             status_code=400
        )


