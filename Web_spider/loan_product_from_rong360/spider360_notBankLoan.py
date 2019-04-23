# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 11:00:22 2019

@author: hongzk
"""

# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 15:18:09 2019

@author: hongzk
"""
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import re
import pandas as pd
import numpy as np
import os
import sys
import time    
columns = ['city', 'loan_name', 'loan_link', 'mortgage', 'serve_type', 'loan_time',
           'seller', 'sell_company', 'loan_condition', 'gross_interest', 'monthly_fee', 'one_off_fee']

    
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


#定位到目标html
def getNotBankLoanInfo(city, target, resultDf=pd.DataFrame()):
    if len(resultDf)==0:
        resultDf=pd.DataFrame(columns=columns)
    browser.get(target)
    soup = BeautifulSoup(browser.page_source , 'html.parser')
    body = soup.body
    try:
        result_items = body.find_all('div', attrs={'class':'result-item'})
        for item in result_items:
            loan_name = item.find('div', attrs={'class':'title'}).find('a').text
            loan_link = item.find('div', attrs={'class':'title'}).find('a').get('data-href')
            mortgage = item.find('div', attrs={'class':'span'}).find_all('p')[0].text
            serve_type = item.find('div', attrs={'class':'span'}).find_all('p')[1].text
            loan_time = item.find('div', attrs={'class':'span'}).find_all('p')[2].text
            seller = item.find('div', attrs={'class':'span2'}).find_all('a')[0].text
            sell_company = item.find('div', attrs={'class':'span2'}).find_all('a')[1].text
            loan_condition = item.find('div', attrs={'class':'span2'}).find('p', attrs={'class':'desc'}).text    
            gross_interest = item.find('div', attrs={'class':'span noborder'}).find_all('p')[1].text
            monthly_fee = item.find('div', attrs={'class':'span noborder'}).find_all('p')[2].find_all('span')[1].text
            try:
                one_off_fee = item.find('div', attrs={'class':'span noborder'}).find_all('p')[2].find_all('span')[2].text
            except:
                one_off_fee=''
        resultDf = resultDf.append(pd.DataFrame(data=[[city, loan_name, loan_link, mortgage, serve_type, loan_time,
                                  seller, sell_company, loan_condition, gross_interest, monthly_fee, one_off_fee]],
                            columns=columns))
        if body.find('div', attrs={'class':'page'}).find('a', attrs={'class':'next-page'}).text == '下一页':
            next_target = "https://www.rong360.com"+body.find('div', attrs={'class':'page'}).find('a', attrs={'class':'next-page'}).get('href')
            resultDf = getNotBankLoanInfo(city, next_target, resultDf)
        return resultDf
    except:
        return resultDf


if __name__ == '__main__':
    option = webdriver.ChromeOptions()
    option.add_argument("headless")
    browser = webdriver.Chrome(chrome_options=option)
    df = pd.DataFrame(columns=columns)
    cityDict, cityList = getCityName()
    limitList = ['0.3', '1.0', '3.0', '5.0', '10.0', '20.0', '50.0', '100.0']
    termList = ['3', '6', '12', '24', '36', '60', '120']
    for city in cityList:
        for loan_limit in limitList:
            for loan_term in termList:
                try:
                    target = 'https://www.rong360.com/%s/search.html?loan_limit=%s&loan_term=%s&standard_type=2'%(city, str(loan_limit), str(loan_term))
                    cityData = getNotBankLoanInfo(city, target)
                    cityData = cityData.reset_index(drop=True)
                    df = df.append(cityData)
                except:
                    print(city+'+'+loan_limit+'+'+loan_term+' is wrong!!!!!!!!!!')
                    pass
        print('%s is over ----------'%city)
    df['city_name'] = df['city'].map(cityDict)
    df.to_excel('not_bank_loan_info.xlsx', index=False)
    

