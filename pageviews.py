# -*- coding: utf-8 -*-
import pageviewapi
import json
import sys
from mwviews.api import PageviewsClient
from lang_map import code_lang_map

def uprint(*objects, sep=' ', end='\n', file=sys.stdout):
    enc = file.encoding
    if enc == 'UTF-8':
        print(*objects, sep=sep, end=end, file=file)
    else:
        f = lambda obj: str(obj).encode(enc, errors='backslashreplace').decode(enc)
        print(*map(f, objects), sep=sep, end=end, file=file)
		
p = PageviewsClient()
articles = [pageviewapi.top(k + '.wikipedia', 2015, 11, 14, access='all-access') for k in code_lang_map.keys()]
#articles_list = [p.top_articles(k + '.wikipedia', limit=100) for k in code_lang_map.keys()]
with open('result.json', 'w') as fp:
    json.dump(articles, fp)
