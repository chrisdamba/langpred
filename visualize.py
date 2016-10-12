import matplotlib.pyplot as plt
import os
import seaborn as sns

from pandas import DataFrame
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import pairwise_distances
from collections import Counter
from uncurl import parse


def percentages(df):
    df2 = df.join(df.div(df['num_letters'], axis='index'), rsuffix='_perc')
    percs = [col for col in df2.columns if col.endswith('_perc')]
    return df2[percs]


files = [f for f in os.listdir('.') if os.path.isfile(f)]
top_letters = []
for f in files:
    print(f)
    c = Counter()
    for article in parse(f):
        c['articles_count'] += 1
        for letter in article['content']:
            c[letter] += 1
            c['letters_count'] += 1
    d = dict(c.most_common(2000))
    top_letters.append(d)


df = DataFrame(top_letters)
df.fillna(0, inplace=True)
df = df.set_index('lang')

df3 = percentages(df)
num_clusters = 4

palette = sns.color_palette('colorblind', num_clusters)
# run KMeans clustering algorithm on letter percentages
# KMeans automatically groups your data together based on similarity
est = KMeans(num_clusters, max_iter=30000)
est.fit(df3.values)
y_kmeans = est.predict(df3.values)

# run Principal Component Analysis to reduce the number of columns from 10000 to 2
pca = PCA(n_components=2)
pca.fit(df3.values)
X_trans = pca.transform(df3.values)
plt.scatter(X_trans[:, 0], X_trans[:, 1], c=[palette[y] for y in y_kmeans], s=50)


cluster_dfs = {}
cluster_langs = {}
cluster_distances = {}

# Find the languages that are most similar
for cluster_num in range(4):
    indexes = [i for i in range(y_kmeans.shape[0]) if y_kmeans[i] == cluster_num]
    cluster_langs[cluster_num] = [langs[i] for i in indexes]
    cluster_dfs[cluster_num] = df3.loc[cluster_langs[cluster_num], :]

    # Calculate pairwise distances and display
    print('Cluster #{0'.format(cluster_num))

    '''
    Calculate the Euclidian distance between each pair of points - the smaller the distance the more similar the data points are
    '''
    cluster_distances[cluster_num] = pairwise_distances(cluster_dfs[cluster_num].values)
    n, m = cluster_distances[cluster_num].shape
    distances = set([])
    for i in range(n):
        for j in range(m):
            if i == j:
                continue
            distances.add((cluster_distances[cluster_num][i, j], tuple(sorted([i, j]))))
    for a in sorted(distances)[:20]:
        print_sim(a[0], a[1][0], a[1][1], cluster_langs[cluster_num])
    print()