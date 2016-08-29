import requests
import sys
import time

lang = sys.argv[1]
timestamp = time.strftime("%Y%m%d%H%M%S")
url = "https://{0}.wikipedia.org/w/index.php?title=Special:Export&action=submit".format(lang)
origin = "https://{0}.wikipedia.org".format(lang)
referer = "https://{0}.wikipedia.org/wiki/Special:Export".format(lang)
filename = "wikipedia-{0}-{1}.xml".format(lang, timestamp)

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
    'pages': 'politics\nClinton_email_scandal\nJava\nundefined',
    'curonly': '1',
    'wpDownload': '1',
}

res = requests.post(url, data=payload, headers=headers)
with open(filename, 'wb') as file:
    file.write(res.content)