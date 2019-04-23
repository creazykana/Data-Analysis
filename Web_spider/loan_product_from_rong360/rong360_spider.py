# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 15:18:09 2019

@author: hongzk
"""
#from selenium import webdriver
#from selenium.webdriver.common.keys import Keys
#from bs4 import BeautifulSoup
#import re
#import pandas as pd
#import numpy as np
#import os
#import sys
#import time
#
##爬取城市名单信息
#class rong360Spider(object):
#    def __init__(self, spiderType):
#        self.spiderType = spiderType
#        if spiderType == 'bank_loan':
#            self.columns = ['city', 'loan_name', 'loan_link', 'mortgage', 'loan_work', 'loan_time',
#                       'loan_condition', 'interest', 'repay_per_month', 'loan_rate', 'success_apply_num']
#        else:
#            self.columns = ['city', 'and so on .....'] #need modify
#        
#    
#    def getCityName(self):
#        target = "https://www.rong360.com/cityNavi.html"
#        browser.get(target)
#        soup = BeautifulSoup(browser.page_source , 'html.parser')
#        body = soup.body
#        cityHtml = body.find('div', attrs={'id':'TabWordList'}).find_all('a')
#        spellNames = []
#        names = []
#        for cityInfo in cityHtml:
#            spellNames.append(cityInfo.get('domain'))
#            names.append(cityInfo.text)
#        cityDict = dict(zip(spellNames, names))
#    #    df = pd.DataFrame()
#    #    df['city'] = names
#    #    df['spell_name'] = spellNames
#        return cityDict, spellNames
#    
#    
#    #信息提取
#    def parsePage(self, city, item):
#        useInfo = item.find('div', attrs={'class':'item_info'})
#        loan_name = useInfo.find('h4').find('a').text
#        loan_link = useInfo.find('h4').find('a').get('href')
#        
#        detailInfo = useInfo.find('div', attrs={'class':'item_meta'})
#        detail1 = detailInfo.find('ul', attrs={'class':'meta_sep specs'}).find_all('li')
#        mortgage = detail1[0].text
#        loan_work = detail1[1].text
#        loan_time = detail1[2].text
#        detail2 = detailInfo.find('ul', attrs={'class':'meta_sep reqs'}).find_all('li')
#        loan_condition = ''
#        for i in detail2:
#            loan_condition = loan_condition + i.text + "/"
#        detail3 = detailInfo.find('ul', attrs={'class':'meta_sep lixi'}).find_all('li')
#        interest = detail3[0].text
#        repay_per_month = detail3[1].text
#        loan_rate = detail3[2].text
#        detail4 = detailInfo.find('ul', attrs={'class':'meta_sep view'})
#        success_apply_num = detail4.find_all('p')[2].text
#        return pd.DataFrame(data=[[city, loan_name, loan_link, mortgage, loan_work, loan_time,
#                                  loan_condition, interest, repay_per_month, loan_rate, success_apply_num]],
#                            columns=self.columns)
#    
#    
#    #定位到目标html
#    def getInfo(self, city, target, resultDf=pd.DataFrame()):
#        print('!!!')
#        if len(resultDf)==0:
#            resultDf=pd.DataFrame(columns=self.columns)
#            print('build a new resultDf is OK')
#        browser.get(target)
#        soup = BeautifulSoup(browser.page_source , 'html.parser')
#        body = soup.body
#        try:
#            result = body.find('ul', attrs={'class': 'a-product_list search_list'})
#            items = result.find_all('li', attrs={'class':'item'})
#            for item in items:
#                itemDf = parsePage(city, item)
#                resultDf = resultDf.append(itemDf)
#            if body.find('div', attrs={'class':'page'}).find('a', attrs={'class':'next-page'}).text == '下一页':
#                next_target = "https://www.rong360.com"+body.find('div', attrs={'class':'page'}).find('a', attrs={'class':'next-page'}).get('href')
#                resultDf = getInfo(city, next_target, resultDf)
#            return resultDf
#        except:
#            print(city+'+'+loan_limit+'+'+loan_term+' is not data!!!!!!!!!!')
#            return resultDf
#
#    def bank_loan_spider(self):
#        option = webdriver.ChromeOptions()
#        option.add_argument("headless")
#        browser = webdriver.Chrome(chrome_options=option)
#        df = pd.DataFrame(columns=self.columns)
#        cityDict, cityList = getCityName()
#        limitList = ['0.3', '1.0', '3.0', '5.0', '10.0', '20.0', '50.0', '100.0']
#        termList = ['3', '6', '12', '24', '36', '60', '120']
#    #    i=1
#        for city in cityList:
#            for loan_limit in limitList:
#                for loan_term in termList:
#                    try:
#                        target = 'https://www.rong360.com/%s/search.html?loan_limit=%s&loan_term=%s'%(city, str(loan_limit), str(loan_term))
#                        cityData = getInfo(city, target)
#                        df = df.append(cityData)
#    #                    time.sleep(0.5)
#    #                    sys.stdout.write("已爬虫:%.4f%%"%float(i/19544)+'\r')
#    #                    sys.stdout.flush()
#    #                    i = i+1
#                    except:
#    #                    print(city+'+'+loan_limit+'+'+loan_term+' is wrong!!!!!!!!!!')
#                        pass
#        
#        df['city_name'] = df['city'].map(cityDict)
#        df.to_excel(r'C:/Users/hongzk/bank_loan_info.xlsx', index=False)
#        
#    
#    def test(self, city, loan_limit, loan_term):
#        option = webdriver.ChromeOptions()
#        option.add_argument("headless")
#        browser = webdriver.Chrome(chrome_options=option)
#        df = pd.DataFrame(columns=self.columns)
#        try:
#            target = 'https://www.rong360.com/%s/search.html?loan_limit=%s&loan_term=%s&op_type=2'%(city, str(loan_limit), str(loan_term))
#            cityData = getInfo(city, target)
#        except:
#            print("This is wrong!!~~~~~~")
#            target = "https://www.rong360.com/shanghai/search.html?loan_limit=5.0&loan_term=12"
#        try:
#            
#            df = df.append(cityData)
#        except:
#            print('There are something wrong!!!')
#        df.to_excel(r'C:/Users/hongzk/test_loan_info.xlsx', index=False)


# =============================================================================
# 
# =============================================================================
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import os
import sys
import time    
columns = ['city', 'loan_name', 'loan_link', 'mortgage', 'loan_work', 'loan_time',
                       'loan_condition', 'interest', 'repay_per_month', 'loan_rate', 'success_apply_num']

    
def getCityName():
    target = "https://www.rong360.com/cityNavi.html"
    browser.get(target)
    soup = BeautifulSoup(browser.page_source , 'html.parser')
    body = soup.body
    cityHtml = body.find('div', attrs={'id':'TabWordList'}).find_all('a')
    spellNames = []
    names = []
    for cityInfo in cityHtml:
        spellNames.append(cityInfo.get('domain'))
        names.append(cityInfo.text)
    cityDict = dict(zip(spellNames, names))
#    df = pd.DataFrame()
#    df['city'] = names
#    df['spell_name'] = spellNames
    return cityDict, spellNames


#信息提取
def parsePage(city, item):
    useInfo = item.find('div', attrs={'class':'item_info'})
    loan_name = useInfo.find('h4').find('a').text
    loan_link = useInfo.find('h4').find('a').get('href')
    
    detailInfo = useInfo.find('div', attrs={'class':'item_meta'})
    detail1 = detailInfo.find('ul', attrs={'class':'meta_sep specs'}).find_all('li')
    mortgage = detail1[0].text
    loan_work = detail1[1].text
    loan_time = detail1[2].text
    detail2 = detailInfo.find('ul', attrs={'class':'meta_sep reqs'}).find_all('li')
    loan_condition = ''
    for i in detail2:
        loan_condition = loan_condition + i.text + "/"
    detail3 = detailInfo.find('ul', attrs={'class':'meta_sep lixi'}).find_all('li')
    interest = detail3[0].text
    repay_per_month = detail3[1].text
    loan_rate = detail3[2].text
    detail4 = detailInfo.find('ul', attrs={'class':'meta_sep view'})
    success_apply_num = detail4.find_all('p')[2].text
    return pd.DataFrame(data=[[city, loan_name, loan_link, mortgage, loan_work, loan_time,
                              loan_condition, interest, repay_per_month, loan_rate, success_apply_num]],
                        columns=columns)


#定位到目标html
def getInfo(city, target, resultDf=pd.DataFrame()):
    if len(resultDf)==0:
        resultDf=pd.DataFrame(columns=columns)
    browser.get(target)
    soup = BeautifulSoup(browser.page_source , 'html.parser')
    body = soup.body
    try:
        result = body.find('ul', attrs={'class': 'a-product_list search_list'})
        items = result.find_all('li', attrs={'class':'item'})
        for item in items:
            itemDf = parsePage(city, item)
            resultDf = resultDf.append(itemDf)
        if body.find('div', attrs={'class':'page'}).find('a', attrs={'class':'next-page'}).text == '下一页':
            next_target = "https://www.rong360.com"+body.find('div', attrs={'class':'page'}).find('a', attrs={'class':'next-page'}).get('href')
            resultDf = getInfo(city, next_target, resultDf)
        return resultDf
    except:
        return resultDf


if __name__ == '__main__':
    option = webdriver.ChromeOptions()
    option.add_argument("headless")
    browser = webdriver.Chrome(chrome_options=option)
    
    cityDict, cityList = getCityName()
    limitList = ['0.3', '1.0', '3.0', '5.0', '10.0', '20.0', '50.0', '100.0']
    termList = ['3', '6', '12', '24', '36', '60', '120']
    for i in range(1,11):
        cityList2 = cityList[(i-1)*35:i*35]
        df = pd.DataFrame(columns=columns+['loan_limit', 'loan_term'])
        for city in cityList2:
            for loan_limit in limitList:
                for loan_term in termList:
                    try:
                        target = 'https://www.rong360.com/%s/search.html?loan_limit=%s&loan_term=%s'%(city, str(loan_limit), str(loan_term))
                        cityData = getInfo(city, target)
                        cityData['loan_limit'] = loan_limit
                        cityData['loan_term'] = loan_term
                        df = df.append(cityData)
                    except:
                        print(city+'+'+loan_limit+'+'+loan_term+' is wrong!!!!!!!!!!')
                        pass
            print('%s is over ----------'%city)
        df['city_name'] = df['city'].map(cityDict)
        df.to_excel('data/bank_loan_info_%s.xlsx'%str(i), index=False)
        print('The %sth run is over !!!!!!!!!'%str(i))
    
