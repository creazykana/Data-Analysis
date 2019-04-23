# -*- coding: utf-8 -*-
"""
Created on Mon Apr 22 11:47:02 2019

@author: hongzk
"""

import pandas as pd
import numpy as np
import re
import os
    

def dataClean(data):
    columns=['city', 'loan_name', 'loan_link', 'loan_info', 'success_apply_count', 'monthly_fee', 'one_off_fee', 'annual_rate', 'annual_rate_range']
    data = data.drop_duplicates(subset=['city', 'loan_name', 'loan_link']).reset_index(drop=True)
    df = pd.DataFrame(columns=columns)
    pattern = re.compile("[\u4e00-\u9fa5 %]+")
    for i in range(len(data)):
        city = data.loc[i, 'city_name']
        loan_name = re.sub("[\s]+", "", data.loc[i, 'loan_name'])
        loan_link = "https:"+data.loc[i, 'loan_link']
        loan_info = re.sub("[\s]+", "", data.loc[i, 'loan_rate'])
        success_apply_count = int(re.sub("[\u4e00-\u9fa5\s]+", "", data.loc[i, 'success_apply_num']))
        infoList = re.sub(pattern, "", data.loc[i, 'loan_rate']).split('+')
        if len(infoList)==2:
            monthly_fee = str(float(infoList[0])+float(infoList[1]))+"%"
            one_off_fee = "0%"
            annual_rate = str(round((float(infoList[0])+float(infoList[1]))*12, 1))+"%"
        elif len(infoList)==3:
            monthly_fee = str(float(infoList[0])+float(infoList[1]))+"%"
            one_off_fee = str(float(infoList[2]))+"%"
            annual_rate = str(round((float(infoList[0])+float(infoList[1]))*12+float(infoList[2]), 1))+"%"
        else:
            monthly_fee = "-"
            one_off_fee = "-"
            annual_rate = "-"
        if float(annual_rate[:-1])/100<=0.08:
            annual_rate_range = '0.00-0.08'
        elif 0.08<float(annual_rate[:-1])/100<=0.1:
            annual_rate_range = '0.08-0.10'
        elif 0.1<float(annual_rate[:-1])/100<=0.12:
            annual_rate_range = '0.1-0.12'
        elif 0.12<float(annual_rate[:-1])/100<=0.14:
            annual_rate_range = '0.12-0.14'
        elif 0.14<float(annual_rate[:-1])/100<=0.16:
            annual_rate_range = '0.14-0.16'
        elif 0.16<float(annual_rate[:-1])/100<=0.2:
            annual_rate_range = '0.16-0.20'
        else:
            annual_rate_range = '0.20以上'
        df = df.append(pd.DataFrame(data=[[city, loan_name, loan_link, loan_info, success_apply_count, monthly_fee, one_off_fee, annual_rate, annual_rate_range]], columns=columns))
    df.to_excel('../result/bank_loan_info.xlsx', index=False)
    return df
    

if __name__ == "__main__":
    orgData = pd.read_excel(r'../data/bank_loan_info.xlsx')
    data = dataClean(orgData)

    
    