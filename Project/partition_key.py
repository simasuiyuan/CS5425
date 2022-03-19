#%%
from matplotlib.pyplot import axis
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
image_dataset = pd.read_csv("./data/ig_data.csv")
# %%
image_dataset.head()
# image_dataset["bert_ecd"].str.len()
#%%
def split_embeddings(embeddings):
    return [float(x.strip()) for x in embeddings[1:-1].split(",")]

image_dataset["bert_ecd"] = image_dataset["bert_ecd"].apply(split_embeddings)
# %%
feature_dataset = image_dataset.copy()
column_names = [f"bert_ecd_{i}" for i in range(768)]
feature_dataset[column_names] = pd.DataFrame(image_dataset['bert_ecd'].to_list(), columns=column_names)
feature_dataset.head()

#%%
""" PCA => K-Means: curse of Dimensionality 
"""
from sklearn.decomposition import PCA
from sklearn.metrics import adjusted_rand_score
from sklearn.preprocessing import StandardScaler
# %%
features = feature_dataset[column_names].values
features[:5]
# %%
scaler = StandardScaler()
scaled_features = scaler.fit_transform(features)
#%%
pca = PCA().fit(scaled_features)

%matplotlib inline
import matplotlib.pyplot as plt
plt.rcParams["figure.figsize"] = (12,6)

fig, ax = plt.subplots()
xi = np.arange(1, 701, step=20)
y = np.cumsum(pca.explained_variance_ratio_)

plt.ylim(0.0,1.1)
plt.plot(xi, y[xi], marker='o', linestyle='--', color='b')

plt.xlabel('Number of Components')
plt.xticks(np.arange(0, 701, step=20)) #change from 0-based array index to 1-based human-readable label
plt.ylabel('Cumulative variance (%)')
plt.title('The number of components needed to explain variance')

plt.axhline(y=0.95, color='r', linestyle='-')
plt.text(0.5, 0.85, '95% cut-off threshold', color = 'red', fontsize=16)

ax.grid(axis='x')
plt.show()

pca = PCA(0.95).fit(scaled_features)
reduced_features = pca.transform(scaled_features)
reduced_features[:5]
# %%
""" K-Means (Partitional Clustering)
"""
from kneed import KneeLocator
from sklearn.cluster import KMeans
from sklearn.cluster import DBSCAN

# %%
kmeans_kwargs = {
    "init": "random",
    "n_init": 10,
    "max_iter": 300,
    "random_state": 42
}
sse = []
k_samples = range(1, 103, 3)

for k in k_samples:
    kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
    kmeans.fit(reduced_features)
    sse.append(kmeans.inertia_)

plt.style.use("fivethirtyeight")
plt.plot(k_samples, sse)
plt.xticks(k_samples)
plt.xlabel("Number of Clusters")
plt.ylabel("SSE")
plt.show()
# %%
# choosing the elbow point of the curve
kl = KneeLocator(k_samples, sse, curve="convex", direction="decreasing")
print(kl.elbow)

kmeans = KMeans(n_clusters=kl.elbow, **kmeans_kwargs)
kmeans.fit(reduced_features)

#%%
# kmeans.cluster_centers_.shape
def map_centroid(row):
    return kmeans.cluster_centers_[row["kmeans_cluster_id"]].tolist()
# %%
image_dataset["kmeans_cluster_id"] = kmeans.labels_
display(image_dataset.head(2))
image_dataset["kmeans_cluster_centroid"] = image_dataset.apply(map_centroid, axis=1)
display(image_dataset.head(2))
image_dataset['kmeans_cluster_id'].value_counts().plot(kind='bar')


#%%
# image_dataset[image_dataset["kmeans_cluster_id"]==0]

from sklearn.mixture import GaussianMixture
sub_features = reduced_features[kmeans.labels_==0]
kmeans_kwargs = {
    "init": "random",
    "n_init": 10,
    "max_iter": 300,
    "random_state": 42
}
sse = []
k_samples = range(1, 50, 3)

for k in k_samples:
    gmm_kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
    gmm_kmeans.fit(sub_features)
    sse.append(gmm_kmeans.inertia_)

plt.style.use("fivethirtyeight")
plt.plot(k_samples, sse)
plt.xticks(k_samples)
plt.xlabel("Number of Clusters")
plt.ylabel("SSE")
plt.show()
kl = KneeLocator(k_samples, sse, curve="convex", direction="decreasing")
print(kl.elbow)
#%%
# image_dataset.groupby("kmeans_cluster_id")

# %%
""" GMM
"""
from sklearn.mixture import GaussianMixture
n_components  = range(1, 11)

models = [GaussianMixture(n, covariance_type='full', random_state=0).fit(reduced_features)
          for n in n_components ]

plt.plot(n_components , [m.bic(reduced_features) for m in models], label='BIC')
plt.plot(n_components , [m.aic(reduced_features) for m in models], label='AIC')
plt.legend(loc='best')
plt.xlabel('n_components');
# %%
gmm = GaussianMixture(4, covariance_type='full', random_state=0)
gmm.fit(reduced_features)
gmm.means_
#%%
image_dataset["gmm_cluster_id"] = gmm.predict(reduced_features)
display(image_dataset.head(2))
image_dataset['gmm_cluster_id'].value_counts().plot(kind='bar')

#%%
#further cluster from GMM => secondary key
cluster_size_treshhold = 100

for gmm_idx in range(4):
    sub_features = reduced_features[gmm.predict(reduced_features)==gmm_idx]

    kmeans_kwargs = {
        "init": "random",
        "n_init": 10,
        "max_iter": 300,
        "random_state": 42
    }
    sse = []
    k_samples = range(1, 50, 3)

    for k in k_samples:
        gmm_kmeans = KMeans(n_clusters=k, **kmeans_kwargs)
        gmm_kmeans.fit(sub_features)
        sse.append(gmm_kmeans.inertia_)

    plt.style.use("fivethirtyeight")
    plt.plot(k_samples, sse)
    plt.xticks(k_samples)
    plt.xlabel("Number of Clusters")
    plt.ylabel("SSE")
    plt.show()

    kl = KneeLocator(k_samples, sse, curve="convex", direction="decreasing")
    print(kl.elbow)

    gmm_kmeans = KMeans(n_clusters=kl.elbow, **kmeans_kwargs)
    gmm_kmeans.fit(sub_features)

#%%

# %%
""" store PCA & clustering model
"""
from joblib import dump, load
dump(pca, './models/pca.joblib')
dump(kmeans, './models/kmeans.joblib')
# %%
# %load_ext autoreload
# %autoreload
# from src.mongoDB_connector import MONGODB_CONNECTOR
# # %%
# DB_connector = MONGODB_CONNECTOR(user="shared_user", token="2pZGb4axFMwpl3qR")
# # %%
# # DB_connector.create_insert_table(image_dataset)
# image_dataset.head()

# # type(image_dataset["bert_ecd"][0])


# # %%
# DB_connector.create_insert_table(image_dataset)
# # %%
