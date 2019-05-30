# -*- coding: utf-8 -*-
"""
Created on Sat Dec 29 11:44:39 2018

@author: hongzk
"""
# =============================================================================
# 豆瓣短评分析 
# =============================================================================
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import os
import sys
import time

pattern = re.compile('[^0-9-]+')
columns = ['name', 'nameLink', 'comment_time', 'comment', 'favNums']
df = pd.DataFrame(columns = columns)
for i in range(int(4700/20)):
    start_num = i*20
    parseResult = pd.DataFrame(columns = columns)
    target = 'https://movie.douban.com/subject/26100958/comments?start=%s&limit=20&sort=new_score&status=F'%start_num
    r = requests.get(target)
    html = r.text
    parseHtml = BeautifulSoup(html)
    comments = parseHtml.find_all('span', attrs={'class': 'short'})
    names = parseHtml.find_all('span', attrs={'class': 'comment-info'})
    favNums = parseHtml.find_all('span', attrs={'class': 'votes'})
    parseResult['comment'] = [i.text for i in comments]
    parseResult['name'] = [i.find('a').text for i in names]
    parseResult['nameLink'] = [i.find('a').get('href') for i in names]
    parseResult['comment_time'] = [re.sub(pattern, '', i.find('span', attrs={'class': 'comment-time'}).text) for i in names]
    parseResult['favNums'] = [i.text for i in favNums]
    df = df.append(parseResult)
    sys.stdout.write("  已完成:%.3f%%" %  float(i/int(4700/20)) + '\r')
    sys.stdout.flush()
df.to_excel('comment_fromdb.xlsx')


# =============================================================================
# 糗事百科
# =============================================================================
import requests
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import os
import sys
import time
import pytesser3
from PIL import ImageGrab
os.chdir(r'C:/Users/hongzk/Desktop')
print (pytesser3.image_file_to_string('ttt.png'))
target = 'https://www.qiushibaike.com/text/'
r = requests.get(target)
html = r.text
parseHtml = BeautifulSoup(html)
contents = parseHtml.find('div', attrs={'id': 'content-left'}).find_all("div", attrs={"class":re.compile("article block untagged mb15.*")})
for content in contents:
    message = content.find("a").find("div").find("span").text



