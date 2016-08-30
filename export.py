# -*- coding: utf-8 -*-
import bz2
import pageviewapi
import re
import requests
import sys

from datetime import timedelta, date
from lang_map import code_lang_map
from shutil import copyfileobj

article_rgx = re.compile(r'<doc id="(?P<id>\d+)" url="(?P<url>[^"]+)" title="(?P<title>[^"]+)">\n(?P<content>.+)\n<\/doc>', re.S|re.U)


def daterange(start_date, end_date):
	for n in range(int ((end_date - start_date).days)):
		yield start_date + timedelta(n)

def get_top_articles():
	"""
	Get top 5000 articles from a daily pageview count timeseries of all projects for the last 6 months.

	"""
	lang_dict = {}
	articles_list = []
	start_date = date(2016, 1, 1)
	end_date = date(2016, 7, 1)

	for lang in code_lang_map.keys():
		for dt in daterange(start_date, end_date):			
			top_articles = pageviewapi.top(lang + '.wikipedia', dt.year, dt.month, dt.day)
			articles_list.append(top_articles)

	return articles_list


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

	res = requests.post(url, data=payload, headers=headers)
	with open(filename, 'wb') as f:
		f.write(res.content)
	
	with open(filename, 'rb') as input:
		with bz2.BZ2File(lang_bz2, 'wb', compresslevel=9) as output:
			copyfileobj(input, output)


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


if __name__ == '__main__':