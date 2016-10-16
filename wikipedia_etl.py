# -*- coding: utf-8 -*-

"""This application demonstrates the extraction, transform and load process of data from Wikipedia API using Python.

Sample Application Usage:

  $ python wikipedia_etl.py

"""
import bz2
import os
import pickle
import re
import requests
import subprocess
import sys
import time
from lang_map import code_lang_map
from os import listdir
from os.path import isfile, join
from pymongo import InsertOne, MongoClient
from shutil import copyfileobj
from WikiExtractor import process_dump


# connect to database
connection = MongoClient('localhost', 27017)

db = connection.test

# handle to wikipedia collection
wikipedia = db.wikipedia

# regex pattern for scrubbing extracted wikipedia article 
article_rgx = re.compile(
    r'<doc id="(?P<id>\d+)" url="(?P<url>[^"]+)" title="(?P<title>[^"]+)">\n(?P<content>.+)\n<\/doc>', re.S | re.U)


def insert_top_articles():
    """
    Insert in mongo the top 1000 articles per month of pageview count timeseries for a wikipedia project of each language for the last 7 months.

    """
    # initialize bulk insert operations list
    ops = []

    # clear existing wikipedia collection
    wikipedia.remove()

    for lang in code_lang_map.keys():
        for month in range(1, 7):
            try:
                url = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{0}.wikipedia/all-access/2016/{1}/all-days'.format(
                    lang, str(month).zfill(2))
                result = requests.get(url).json()

                if 'items' in result and len(result['items']) == 1:
                    r = result['items'][0]
                    for article in r['articles']:
                        article['lang'] = r['project'][:2]
                        ops.append(InsertOne(article))

            except:
                print('ERROR while fetching or parsing ' + url)

    wikipedia.bulk_write(ops)


def get_top_articles(lang):
    """
    Aggregate top 5000 articles from a daily pageview count timeseries of all projects for the last 6 months.

    """

    # initialize aggregation pipeline
    pipeline = [
        {"$match": {"lang": lang}},
        {
            "$group": {
                "_id": "$article",
                "lang": {"$first": "$lang"},
                "max_views": {"$max": "$views"}
            }
        },
        {
            "$project": {
                "page": "$_id", "_id": 0,
                "lang": 1,
                "max_views": 1
            }
        },
        {"$sort": {"max_views": -1}},
        {"$limit": 5000}
    ]

    result = list(wikipedia.aggregate(pipeline))
    return result


def parse_bz2(filename):
    data = ""
    with bz2.BZ2File(filename, 'r') as f:
        for line in f:
            line = line.decode('utf-8')
            data += line
            if line.count('</doc>'):
                m = article_rgx.search(data)
                if m:
                    yield m.groupdict()
                data = ""


def load_articles(lang, pagelist):
    
    url = "https://{0}.wikipedia.org/w/index.php?title=Special:Export&action=submit".format(lang)
    origin = "https://{0}.wikipedia.org".format(lang)
    referer = "https://{0}.wikipedia.org/wiki/Special:Export".format(lang)
    filename = "dumps/wikipedia-{0}.xml".format(lang)
    pages = '\n'.join(pagelist)

    headers = {
        "Origin": origin,
        "Accept-Encoding": "gzip,deflate,sdch",
        "User-Agent": "Mozilla/5.0 Chrome/35.0",
        "Content-Type": "application/x-www-form-urlencoded",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Cache-Control": "max-age=0",
        "Referer": referer,
        "Connection": "keep-alive",
        "DNT": "1"
    }
    payload = {
        'catname': '',
        'pages': pages,
        'curonly': '1',
        'wpDownload': '1',
    }

    res = requests.post(url, data=payload, headers=headers)
    with open(filename, 'wb') as f:
        f.write(res.content)

    with open(filename, 'rb') as input:
        with bz2.BZ2File(filename + '.bz2', 'wb', compresslevel=9) as output:
            copyfileobj(input, output)
    os.remove(filename)

    '''
    return list(parse_bz2(filename+'.bz2'))

    '''
    return filename + '.bz2'


def main():
    insert_top_articles()
    for lang in code_lang_map.keys():       
        pagelist = [r['page'] for r in get_top_articles(lang)]
        filename = load_articles(lang, pagelist)    
        process_dump(filename, None, "extracted/"+filename, 500000, True, 3)

if __name__ == '__main__': 
    main()
   