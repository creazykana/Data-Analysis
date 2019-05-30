# -*- coding: utf-8 -*-
"""
Created on Wed May 29 09:54:35 2019

@author: hongzk
"""

import requests
import os
import pandas as pd
import numpy as np
import time

requests.packages.urllib3.disable_warnings()#解决requests因忽略证书验证而报错

def license_spide(license_number):
    headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
               "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
               "Referer": "https://www.tianyancha.com",
               "Host": "www.tianyancha.com"}
    cookies = {"auth_token":"eyJhbGciOiJIUzUxMiJ9.eyJzdWIiOiIxMzYzMjkzOTEwOCIsImlhdCI6MTU1OTAzNzI2MiwiZXhwIjoxNTkwNTczMjYyfQ.SsKPPuI5pw3X5rdlravsjwhC8RqADrXIjO-ywrvot29Ou8hjz3zpQcJisxMPElhPzGHHv_UsiDq2tlHnwYxHcg"}
    searchDict = {"key":license_number}
    
    s = requests.get('https://www.tianyancha.com/search', headers=headers, params=searchDict, cookies=cookies, verify=False)
    
    soup = BeautifulSoup(s.text, 'html.parser')
    body = soup.body
    if body.find("div", attrs={"class":"result-list no-result"}):
        print("There is not data of license number:%s"%str(license_number))
        return [license_number, "", "", "", "", "", "", "", "", "", ""]
    else:
        license_info = body.find("div", attrs={"class":"result-list sv-search-container"})
        content_info = license_info.find("div", attrs={"class":"content"})
        retail_name = content_info.find("div", attrs={"class":"header"}).find("a").text
        business_state = content_info.find("div", attrs={"class":"header"}).find("div").text
        
        manager = content_info.find("div", attrs="info row text-ellipsis").find_all("div")[0].find("a").text
        registered_capital = content_info.find("div", attrs="info row text-ellipsis").find_all("div")[1].find("span").text
        found_time = content_info.find("div", attrs="info row text-ellipsis").find_all("div")[2].find("span").text
        
        license_num_spide = content_info.find("div", attrs={"class":"match row text-ellipsis"}).find_all("span")[1].text
        
        city = license_info.find("span", attrs={"class":"site"}).text
        try:
            phone_number = content_info.select('div[class^=col]')[0].text
            email = content_info.select('div[class^=col]')[1].text
            company_size = ""
        except:
            try:
                company_size = content_info.select("div[class^=tag-list]")[0].text
                phone_number = ""
                email = ""
            except:
                company_size = ""
                phone_number = ""
                email = ""
        return [license_number, retail_name, business_state, manager, registered_capital, 
                found_time, license_num_spide, city, phone_number, email, company_size]
    


if __name__ == "__main__":
    os.chdir(r'E:/work_file/20190529_天眼查爬虫(requests)/code')
    data = pd.read_excel(r'../data/bdp.risk_multiple_names.xlsx')
    data = data.fillna("nod")
    columns = ["license_number", "retail_name", "business_state", "manager", "registered_capital",
               "found_time", "license_num_spide", "city", "phone_number", "email", "company_size"]
    spide_data = pd.DataFrame(columns=columns)
    for i in range(len(data)):
        time.sleep(1)
        license_number = data.loc[i, "business_license_number_dc"]
        if license_number=="nod":
            pass
        else:
            spide_list = license_spide(license_number)
            spide_df = pd.DataFrame(data=[spide_list], columns=columns)
            spide_data = spide_data.append(spide_df)
        print("we have finished %s/%s!"%(str(i+1), str(len(data))))
    spide_data.to_excel("../result/license_info_spide.xlsx", index=False)
    
