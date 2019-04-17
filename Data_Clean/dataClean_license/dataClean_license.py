# -*- coding: utf-8 -*-

from context.context import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import re
from match_dict import matchDict,matchDict2,district


def strQ2B(ustring):
    """中文特殊符号转英文特殊符号"""
    # 中文特殊符号批量识别
    fps = re.findall(pattern, ustring)
    # 对有中文特殊符号的文本进行符号替换

    if len(fps) > 0:
        ustring = ustring.replace(u'，', u',')
        ustring = ustring.replace(u'。', u'.')
        ustring = ustring.replace(u'：', u':')
        ustring = ustring.replace(u'“', u'"')
        ustring = ustring.replace(u'”', u'"')
        ustring = ustring.replace(u'【', u'[')
        ustring = ustring.replace(u'】', u']')
        ustring = ustring.replace(u'《', u'<')
        ustring = ustring.replace(u'》', u'>')
        ustring = ustring.replace(u'？', u'?')
        ustring = ustring.replace(u'；', u':')
        ustring = ustring.replace(u'、', u',')
        ustring = ustring.replace(u'（', u'(')
        ustring = ustring.replace(u'）', u')')
        ustring = ustring.replace(u'‘', u"'")
        ustring = ustring.replace(u'’', u"'")
        ustring = ustring.replace(u'’', u"'")
        ustring = ustring.replace(u'『', u"[")
        ustring = ustring.replace(u'』', u"]")
        ustring = ustring.replace(u'「', u"[")
        ustring = ustring.replace(u'」', u"]")
        ustring = ustring.replace(u'﹃', u"[")
        ustring = ustring.replace(u'﹄', u"]")
        ustring = ustring.replace(u'〔', u"{")
        ustring = ustring.replace(u'〕', u"}")
        ustring = ustring.replace(u'—', u"-")
        ustring = ustring.replace(u'·', u".")

    """全角转半角"""
    # 转换说明：
    # 全角字符unicode编码从65281~65374 （十六进制 0xFF01 ~ 0xFF5E）
    # 半角字符unicode编码从33~126 （十六进制 0x21~ 0x7E）
    # 空格比较特殊，全角为 12288（0x3000），半角为 32（0x20）
    # 除空格外，全角/半角按unicode编码排序在顺序上是对应的（半角 + 0x7e= 全角）,所以可以直接通过用+-法来处理非空格数据，对空格单独处理。

    rstring = ""
    for uchar in ustring:
        inside_code = ord(uchar)  # 返回字符对应的ASCII数值，或者Unicode数值
        if inside_code == 12288:  # 全角空格直接转换
            inside_code = 32
        elif (inside_code >= 65281 and inside_code <= 65374):  # 全角字符（除空格）根据关系转化
            inside_code -= 65248
        try:
            lstr = chr(inside_code)
        except:
            lstr = chr(32)
        rstring += lstr  # 用一个范围在0～255的整数作参数，返回一个对应的字符
    return rstring


def checkBadnum(license):
    badNum = True
    for i in range(len(license) - 1):
        if license[i] != license[i + 1]:
            badNum = False
            break
    if badNum:
        return 'null'
    else:
        return license


def parseRow(license):
    license = re.sub(pattern_chinese, ' ', license)  # 汉字换空格
    license = strQ2B(license.upper())  # 全角转半角、特殊字符转空格
    licenseList = re.split(pattern_split, license)
    licenseResult = ''
    for i in licenseList:
        if licenseResult == '':
            licenseResult = i
        elif len(i) > len(licenseResult):
            licenseResult = i
        else:
            pass
    licenseResult = checkBadnum(licenseResult)  # 将异常数据替代为null
    if re.match(re.compile(u'^\d{15}$'), licenseResult):
        licenseNum = licenseResult
        codeType = '15'
    elif re.match(re.compile(u"^[A-Z0-9]{2}\d{6}[A-Z0-9]{10}$"), licenseResult):
        licenseNum = licenseResult
        codeType = '18'
    elif re.search(re.compile(u"^\d{16}[\-\/\\\\]{1}.*$"), license):
        licenseNum = licenseResult[:-1]
        codeType = '151'  # 需修改
    elif re.match(re.compile(u"^\d{17}$"), licenseResult):
        licenseNum = licenseResult[:-2]
        codeType = '152'  # 需修改
    elif re.search(re.compile(u"^[A-Z0-9]{2}\d{6}[A-Z0-9]{10}\d{1}[\-\/\\\\]{1}.*$"), license):
        licenseNum = licenseResult[:-1]
        codeType = '181'  # 需修改
    elif re.match(re.compile(u"^[A-Z0-9]{2}\d{6}[A-Z0-9]{10}\d{2}$"), licenseResult):
        licenseNum = licenseResult[:-2]
        codeType = '182'  # 需修改
    else:
        licenseNum = 'null'
        codeType = '0'
    return licenseNum, codeType


def verify_code_15(num, p=10):
    for a in num[:-1]:
        a = np.int(a)
        p = (p + a) % 10 * 2
        if p == 0:
            p = 20
        p = p % 11
    for i in range(10):
        if (p + i) % 10 == 1:
            return i


def verify_code_18(num):
    characters = '0123456789ABCDEFGHJKLMNPQRTUWXY'
    digit = range(31)
    numDict = {i: j for i, j in zip(characters, digit)}
    codeDict = {j: i for i, j in zip(characters, digit)}
    c = [numDict[i] for i in num[:-1]]
    w = [(3 ** (i - 1)) % 31 for i in range(1, 18)]
    verify_code = 31 - (np.dot(c, w)) % 31
    if verify_code == 31:
        verify_code = 0
    return codeDict[verify_code]


def getArea(xz_code):
    province = district['province'].get(np.int(xz_code[:2] + '0000'), '')
    city = district['city'].get(np.int(xz_code[:4] + '00'), '')
    county = district['county'].get(np.int(xz_code[:6]), '')
    register_area = province + city + county
    return register_area


def checkVerifyCode(license, types):
    if types in ('15', '151', '152'):
        verify_code = verify_code_15(license)
        area = getArea(license[:6])
        print area
        if int(license[6]) <= 3:
            business_type = '内资企业'
        elif int(license[6]) <= 5:
            business_type = '外资企业'
        else:
            business_type = '个体工商户'
    elif types in ('18', '181', '182'):
        if re.search('[IOZSV]+', license):
            return 'wrong', 'null', 'null'
        else:
            verify_code = verify_code_18(license)
            area = getArea(license[2:8])
            try:
                check1 = matchDict[license[0]]
            except:
                check1 = 'wrong'
            try:
                check2 = matchDict2[license[0:2]]
            except:
                check2 = 'wrong'
            business_type = check1 + "(" + check2 + ")"
    else:
        return 'null', 'null', 'null'
    print area
    if license[-1] == str(verify_code):
        return 'right', business_type.decode('utf-8'), area.decode('utf-8')
    else:
        return 'wrong', business_type.decode('utf-8'), area.decode('utf-8')


def getLicense(row):
    cust_code = row[0]
    licenseCode = row[1]
    print cust_code
    licenseNum, licenseType = parseRow(licenseCode)
    verify_code_check, business_type, area = checkVerifyCode(licenseNum, licenseType)

    return (cust_code, licenseCode, licenseNum, licenseType, verify_code_check, area, business_type)




if __name__ == "__main__":
    pattern = re.compile(u'[，。：“”【】《》？；、（）‘’『』「」﹃﹄〔〕—·]')
    pattern_chinese = re.compile(u"[\u4e00-\u9fa5]+")  # 所有汉字
    pattern_split = re.compile(u"[^0-9A-Z]+")

    data = hc.sql(sql)
    dataClean = data.rdd.map(getLicense)

    schema = StructType([StructField("cust_code", StringType(), True), \
                         StructField("indu_comm_business_licence_number", StringType(), True), \
                         StructField("business_license_number_dc", StringType(), True), \
                         StructField("license_type", StringType(), True), \
                         StructField("verify_code_check", StringType(), True), \
                         StructField("register_area", StringType(), True), \
                         StructField("other_info", StringType(), True)])

    checkResult = hc.createDataFrame(dataClean, schema)
    checkResult.write.saveAsTable("bdp.test", mode="overwrite")
    checkResult.show(10)
