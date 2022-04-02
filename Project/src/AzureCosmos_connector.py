from azure.cosmos import CosmosClient, exceptions
from azure.cosmos.partition_key import PartitionKey
from src.config import azure_settings, azure_schemas
from typing import Union
import pandas as pd

class COSMOS_CONNECTOR(object):
    def __init__(self, config: dict = azure_settings):
        self.host = azure_settings['host']
        self.master_key = azure_settings['master_key']
        self.resource_group = azure_settings['resource-group']
        self.database_id = azure_settings['database_id']
        self.container_id = azure_settings['container_id']
        self.connect()

    def connect(self):
        self.client = CosmosClient(self.host, credential=self.master_key)
    
    """database api
    """
    def list_databases(self):
        client = self.client 
        print('Databases:')
        databases = list(client.list_databases())
        if not databases:
            return
        for database in databases:
            print(database['id'])

    def create_database(self, database_id):
        client = self.client 
        try:
            client.create_database(id=id)
            print('Database with id \'{0}\' created'.format(database_id))

        except exceptions.CosmosResourceExistsError:
            print('A database with id \'{0}\' already exists'.format(database_id))

    def find_database(self, database_id):
        client = self.client 
        databases = list(client.query_databases({
            "query": "SELECT * FROM r WHERE r.id=@id",
            "parameters": [
                { "name":"@id", "value": database_id }
            ]
        }))

        if len(databases) > 0:
            print('Database with id \'{0}\' was found'.format(database_id))
        else:
            print('No database with id \'{0}\' was found'. format(database_id))

    def get_database(self, id):
        client = self.client 
        try:
            database = client.get_database_client(id)
            database.read()
            print('Database with id \'{0}\' was found, it\'s link is {1}'.format(id, database.database_link))
            return database
        except exceptions.CosmosResourceNotFoundError:
            print('A database with id \'{0}\' does not exist'.format(id))
            return



    """container api
    """
    def list_containers(self, db):
        print('Containers:')
        containers = list(db.list_containers())
        if not containers:
            return
        for container in containers:
            print(container['id'])

    """ use existing container not creating
    """
    def create_container(self, db, container_id):
        raise NotImplementedError("use existing container not creating")

    def manage_provisioned_throughput(self, db, container_id, newOffer:int=0):
        #A Container's Provisioned Throughput determines the performance throughput of a container.
        #A Container is loosely coupled to Offer through the Offer's offerResourceId
        #Offer.offerResourceId == Container._rid
        #Offer.resource == Container._self
        try:
            # read the container, so we can get its _self
            container = db.get_container_client(container=container_id)
            # now use its _self to query for Offers
            offer = container.read_offer()

            print('Found Offer \'{0}\' for Container \'{1}\' and its throughput is \'{2}\''.format(offer.properties['id'], 
            container.id, offer.properties['content']['offerThroughput']))
        except exceptions.CosmosResourceExistsError:
            print('A container with id \'{0}\' does not exist'.format(id))

        #The Provisioned Throughput of a container controls the throughput allocated to the Container
        if newOffer > 0:
            #The following code shows how you can change Container's throughput
            offer = container.replace_throughput(newOffer)
            print('Replaced Offer. Provisioned Throughput is now \'{0}\''.format(offer.properties['content']['offerThroughput']))
    

    def find_container(self, db, container_id):
        containers = list(db.query_containers(
            {
                "query": "SELECT * FROM r WHERE r.id=@id",
                "parameters": [
                    { "name":"@id", "value": container_id }
                ]
            }
        ))
        if len(containers) > 0:
            print('Container with id \'{0}\' was found'.format(container_id))
        else:
            print('No container with id \'{0}\' was found'. format(container_id))

    def get_container(self, db, container_id):
        try:
            container = db.get_container_client(container_id)
            container.read()
            print('Container with id \'{0}\' was found, it\'s link is {1}'.format(container.id, container.container_link))
            return container
        except exceptions.CosmosResourceNotFoundError:
            print('A container with id \'{0}\' does not exist'.format(id))
            return


    """document api
    """
    def get_full_document_to_pd(self, container, if_pandas: bool=True) -> pd.DataFrame:
        dflist = []
        for item in container.query_items(
            query='SELECT * FROM c',
            enable_cross_partition_query = True):
            dflist.append(dict(item))
        if if_pandas:
            return pd.DataFrame(dflist)
        return dflist

    def query_by_Id(self, container, Id: str, if_pandas: bool=True) -> pd.DataFrame:
        items = list(container.query_items(
            query="SELECT * FROM r WHERE r.id=@id",
            parameters=[
            { "name":"@id", "value": Id }
        ],
            enable_cross_partition_query=True
        ))
        if if_pandas:
            pd.Series(items)
        return items
    
    def query_by_multiId(self, container, Ids: list, if_pandas: bool=True) -> pd.DataFrame:
        Ids = str(tuple(Ids))
        items = list(container.query_items(
            query=f"SELECT * FROM r WHERE r.id IN {Ids}",
            enable_cross_partition_query=True
        ))
        if if_pandas:
            return pd.DataFrame(items)
        return items

    def query_by_clusterId(self, container, cluster_id: list, if_pandas: bool=True) -> pd.DataFrame:
        items = list(container.query_items(
            query=f"SELECT * FROM r WHERE r.cluster_id=@id",
            parameters=[
            { "name":"@id", "value": cluster_id }
            ],
            enable_cross_partition_query=True
        ))
        if if_pandas:
            return pd.DataFrame(items)
        return items

    def bulk_insert_(self, container, Id: str, content: Union[dict, pd.DataFrame, pd.Series], ):
        if type(content) in [pd.DataFrame, pd.Series]:
            content = content.to_dict('records')
        else: 
            content = [content]
        num=1
        for row in content:
            try:
                container.upsert_item(
                    {
                        'id': row["id"],
                        'cluster_id': row["kmeans_cluster_id"],
                        'cluster_centroid':row["kmeans_cluster_centroid"],
                        'dataset': row["dataset"],
                        'url': row['url'],
                        'encd': row['encd'],
                        'img_name': row['img_name']
                    }
                )      
            except exceptions.CosmosHttpResponseError as e:
                print("Failed to insert {}, row number {}".format(row['id'], num))
                print(e)
            else:
                print("Inserted {}, row number {}".format(row['id'], num))
            num += 1