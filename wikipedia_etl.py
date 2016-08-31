# -*- coding: utf-8 -*-

"""This application demonstrates the extraction, transform and load process of data from Wikipedia API using Python.

Sample Application Usage:

  $ python wikipedia_etl.py

"""

import bz2
import pageviewapi
import pymongo
import re
import requests
import sys

from datetime import timedelta, date
from lang_map import code_lang_map
from pymongo import InsertOne, MongoClient
from shutil import copyfileobj

# connect to database
connection = MongoClient('localhost', 27017)

db = connection.test

# handle to wikipedia collection
wikipedia = db.wikipedia

# regex pattern for scrubbing extracted wikipedia article 
article_rgx = re.compile(r'<doc id="(?P<id>\d+)" url="(?P<url>[^"]+)" title="(?P<title>[^"]+)">\n(?P<content>.+)\n<\/doc>', re.S|re.U)


def insert_top_articles():  
	"""
	Insert in mongo the top 1000 articles per month of pageview count timeseries for a wikipedia project of each language for the last 7 months.

	"""
	# initialize bulk insert operations list
	ops = []

	# clear existing wikipedia collection
	wikipedia.remove()

	for lang in code_lang_map.keys():
		for month in range(1, 8):   
			try:
				url = 'https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{0}.wikipedia/all-access/2016/{1}/all-days'.format(lang, str(month).zfill(2))        
				result = requests.get(url).json()

				if 'items' in result and len(result['items']) == 1:
					r = result['items'][0]
					r['lang'] = lang
					ops.append(InsertOne(r))

			except:
				print('ERROR while fetching or parsing ' + url)

	wikipedia.bulk_write(ops)


def get_top_articles(lang):
	"""
	Aggregate top 5000 articles from a daily pageview count timeseries of all projects for the last 6 months.

	"""

	# initialize aggregation pipeline 
	pipeline = [    
		{ "$match": { "lang": lang } },
		{ "$unwind": "$articles" },
		{
			"$group": {
				"_id": {
					"lang": "$lang",
					"page": "$articles.article"
				},
				"max_views": { "$max": "$articles.views" }            
			}
		},
		{
			"$project": {
				"page": "$_id.page", "_id": 0,
				"lang": "$_id.lang",
				"max_views": 1
			}
		},
		{ "$sort": { "max_views": -1 } },
		{ "$limit": 5000 }
	]

	result = wikipedia.aggregate(pipeline)
	return result


def load_article(lang, pagename):

	url = "https://{0}.wikipedia.org/w/index.php?title=Special:Export&action=submit".format(lang)
	origin = "https://{0}.wikipedia.org".format(lang)
	referer = "https://{0}.wikipedia.org/wiki/Special:Export".format(lang)
	filename = "dumps/xml/wikipedia-{0}-{1}.xml".format(lang, pagename)
	lang_bz2 = "dumps/bz2/pages-articles-{0}.xml.bz2".format(lang)

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
		'pages': pagename,
		'curonly': '1',
		'wpDownload': '1',
	}

	try:
		res = requests.post(url, data=payload, headers=headers)
		with open(filename, 'wb') as f:
			f.write(res.content)
		
		with open(filename, 'rb') as input:
			with bz2.BZ2File(lang_bz2, 'wb', compresslevel=9) as output:
				copyfileobj(input, output)
	except:
		print('ERROR while fetching or parsing ' + url)

def parse(filename):
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


def main():
	# Populate wikipedia collection with data from Wikipedia PageView API.
	insert_top_articles()

	# Get aggregated top articles for each language.
	articles = [get_top_articles(lang) for lang in code_lang_map.keys()]

	# Grab articles data and write to bz2 file for each language
	for article in articles:
		load_article(article['lang'], article['page'])

	# Parse files
  
if __name__ == '__main__':
	main()
