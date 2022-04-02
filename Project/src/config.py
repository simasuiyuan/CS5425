import os
import subprocess

from matplotlib import collections
from numpy import double

RESOURCE_GROUP = 'CS5425'
DATABASE_NAME = 'image-embeddings-db'
CONTAINER_NAME = ['bert-encodings', "clustered-meta-data"]

url_out = subprocess.Popen(f'az cosmosdb show --resource-group {RESOURCE_GROUP} --name {DATABASE_NAME} --query documentEndpoint --output tsv', 
        stdout=subprocess.PIPE, 
        stderr=subprocess.STDOUT,
        shell=True)
url_bytes, _ = url_out.communicate()
url = url_bytes.decode('utf-8').removesuffix('\n')


key_out = subprocess.Popen(f'az cosmosdb keys list --resource-group {RESOURCE_GROUP} --name {DATABASE_NAME} --query primaryMasterKey --output tsv', 
    stdout=subprocess.PIPE, 
    stderr=subprocess.STDOUT,
    shell=True)
key_bytes, _ = key_out.communicate()
key = key_bytes.decode('utf-8').removesuffix('\n')

azure_settings = {
    'host': os.environ.get('ACCOUNT_HOST', url),
    'master_key': os.environ.get('ACCOUNT_KEY', key),
    'resource-group': os.environ.get('RESOURCE_GROUP', RESOURCE_GROUP),
    'database_id': os.environ.get('COSMOS_DATABASE', DATABASE_NAME),
    'container_id': os.environ.get('COSMOS_CONTAINER', CONTAINER_NAME),
}

spark_setting = {
    "spark.cosmos.accountEndpoint" : os.environ.get('ACCOUNT_HOST', url),
    "spark.cosmos.accountKey" : os.environ.get('ACCOUNT_KEY', key),
    "spark.cosmos.database" : os.environ.get('COSMOS_DATABASE', DATABASE_NAME),
    "spark.cosmos.container" : os.environ.get('COSMOS_CONTAINER', CONTAINER_NAME),
}

azure_schemas = {
    'bert-encodings': {
        'id': "",
        'dataset': "",
        'url': "",
        'encd': [],
        'img_name': "",
    },
    'clustered-meta-data': {
        'id': "",
        'cluster_id': 0,
        'cluster_centroid': [],
        'dataset': "",
        'url': "",
        'encd': [],
        'img_name': "",
    }
}