#### 融360爬虫  
网站链接：https://www.rong360.com/shenzhen/  
目标：爬取市面上的贷款产品信息  

###### url解析  
查询网页为：https://www.rong360.com/<city_name>/search.html?loan_limit=5.0&loan_term=12  
其中查询条件loan_limit对应贷款金额；loan_term对应贷款期限；standard_type=2对应非银行贷；op_type对应职业身份限制  

###### 使用的工具  
python + selenium + BeautifulSoup  
注：由于PhantomJS已停止更新，且新版selenium已不支持，可直接调用Chrome的无头模式进行爬虫  

###### 缺陷  
爬虫速度较慢(可学习分布式爬虫加快效率)  
