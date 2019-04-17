### 经营许可证组成规则：
18位统一社会信用码 https://wenku.baidu.com/view/00c4024e360cba1aa911da54.html  
15位统一社会信用码 https://wenku.baidu.com/view/19873704cc1755270722087c.html

15位的工商营业执照号组成结构为：首次机关码('\d{6}') + 顺序码('\d{8}') + 校验码('\d{1}')  
18位的工商营业执照号组成结构为：登记管理部门代码('[0-9A-Z]{1}') + 机构类别代码('[0-9A-Z]{1}') + 登记管理机关行政区划码('[0-9]{6}') + 组织机构代码('[0-9A-Z]{9}') + 校验码('[0-9A-Z]{1}')


#### 数据清洗逻辑：
1.将字符串中所有的全角字符转为半角字符，即把字符的unicode编码缩小到33~126  
2.利用符号及中文做分割，提取字段中有效片段(字母+数字)，取长度最长的片段且位数为15/18的作为清洗结果，否则为空  
3.为空的进一步清洗，匹配样式"15/18位执照号"+"1-1"，及"16位数字/字母" + "-1"，输出结果  

#### 数据分析逻辑：
1.分别对15位和18位的最后一位校验码的生成逻辑进行计算，与原执照号校验码进行比对判断  
2.通过执照号的行政区划码做行政区地址匹配  
3.匹配商户的企业类型等其他信息  