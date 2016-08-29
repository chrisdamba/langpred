import bz2
import re

article = re.compile(r'<doc id="(?P<id>\d+)" url="(?P<url>[^"]+)" title="(?P<title>[^"]+)">\n(?P<content>.+)\n<\/doc>', re.S|re.U)

def parse(filename):
  data = ""
  with bz2.BZ2File(filename, 'r') as f:
    for line in f:
      line = line.decode('utf-8')
      data += line
      if line.count('</doc>'):
        m = article.search(data)
        if m:
          yield m.groupdict()
        data = ""