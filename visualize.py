# -*- coding: utf-8 -*-
import bz2
import matplotlib.pyplot as plt
import numpy as np
import os
import pickle
import seaborn as sns
import sys
import re

from lang_map import code_lang_map
from pandas import DataFrame
from sklearn.decomposition import PCA
from sklearn.cluster import KMeans
from sklearn.metrics.pairwise import pairwise_distances
from collections import Counter
from uncurl import parse

def uprint(*objects, sep=' ', end='\n', file=sys.stdout):
    enc = file.encoding
    if enc == 'UTF-8':
        print(*objects, sep=sep, end=end, file=file)
    else:
        f = lambda obj: str(obj).encode(enc, errors='backslashreplace').decode(enc)
        print(*map(f, objects), sep=sep, end=end, file=file)

"""
Calculate the percentages from a pandas dataframe of letter counts and add the percentages as new columns 
to the given dataframe
"""
def percentages(df):
    df2 = df.join(df.div(df['letters_count'], axis='index'), rsuffix='_perc')
    percs = [col for col in df2.columns if col.endswith('_perc')]
    return df2[percs]


"""
Count the number of times each character occurs in each language and grab the top 2000 from each of them
"""
def get_top_letters():
    files = os.listdir('articles')
    top_letters = []    
    for f in files:
        print(f)
        c = Counter()
        for article in parse('articles/'+f):
            c['articles_count'] += 1
            for letter in article['content']:
                c[letter] += 1
                c['letters_count'] += 1
        d = dict(c.most_common(2000))
        d['lang'] = os.path.splitext(f)[0]
        top_letters.append(d)
    return top_letters

def parse(filename):
    data = ""
    # regex pattern for scrubbing extracted wikipedia article 
    article_rgx = re.compile(
    r'<doc id="(?P<id>\d+)" url="(?P<url>[^"]+)" title="(?P<title>[^"]+)">\n(?P<content>.+)\n<\/doc>', re.S | re.U)
    with open(filename, 'r', encoding='utf8') as f:
        for line in f:
            #line = line.decode('utf-8')
            data += line
            if line.count('</doc>'):
                m = article_rgx.search(data)
                if m:
                    yield m.groupdict()
                data = ""

def load_data():
    """Load the articles dictionary back from pickle."""

    f = open('letters.pickle', 'rb')
    data = pickle.load(f, encoding='latin1')
    f.close()
    return data

def dump_data():
    """Save the articles dictionary into pickle."""

    top_letters = get_top_letters()  
    with open('letters.pickle', 'wb') as handle:
        pickle.dump(top_letters, handle) 

def main():
    data = load_data()    
    df = DataFrame(data)
    df.fillna(0, inplace=True)
    df = df.set_index('lang')    
    
    df3 = percentages(df)
    df3.values[np.isnan(df3.values)] = np.median(df3.values[~np.isnan(df3.values)])
    
    num_clusters = 4
    palette = sns.color_palette('colorblind', num_clusters)
    
    # run KMeans clustering algorithm on letter percentages
    # KMeans automatically groups the data together based on similarity 
    est = KMeans(num_clusters, max_iter=30000)
    est.fit(df3.values)
    y_kmeans = est.predict(df3.values)

    # run Principal Component Analysis to reduce the number of columns from 10000 to 2
    pca = PCA(n_components=2)
    pca.fit(df3.values)
    X_trans = pca.transform(df3.values)

    #plot the results
    plt.scatter(X_trans[:, 0], X_trans[:, 1], c=[palette[y] for y in y_kmeans], s=50)

    
    cluster_dfs = {}
    cluster_langs = {}
    cluster_distances = {}
    langs = list(code_lang_map.keys())

    # Find the languages that are most similar
    for cluster_num in range(4):
        indexes = [i for i in range(y_kmeans.shape[0]) if y_kmeans[i] == cluster_num]
        print(indexes)
        cluster_langs[cluster_num] = [langs[i] for i in indexes]
        cluster_dfs[cluster_num] = df3.loc[cluster_langs[cluster_num], :]

        # Calculate pairwise distances and display
        print('Cluster #{0}'.format(cluster_num))


        # Calculate the Euclidian distance between each pair of points - the smaller the distance the more similar the data points are
        cluster_distances[cluster_num] = pairwise_distances(cluster_dfs[cluster_num].values)
        n, m = cluster_distances[cluster_num].shape
        distances = set([])
        for i in range(n):
            for j in range(m):
                if i == j:
                    continue
                distances.add((cluster_distances[cluster_num][i, j], tuple(sorted([i, j]))))
        for a in sorted(distances)[:20]:
            print(a[0])
            print(a[1][0])
            print(a[1][1])
            print(cluster_langs[cluster_num])
        print()
    
    
if __name__ == '__main__': 
    main()