# -*- coding: utf-8 -*

from context.context import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import re
import sys

reload(sys)
sys.setdefaultencoding('utf-8')

nameStr = '''赵 钱 孙 李 周 吴 郑 王 冯 陈 褚 卫
蒋 沈 韩 杨 朱 秦 尤 许 何 吕 施 张
孔 曹 严 华 金 魏 陶 姜 戚 谢 邹 喻
柏 水 窦 章 云 苏 潘 葛 奚 范 彭 郎
鲁 韦 昌 马 苗 凤 花 方 俞 任 袁 柳
酆 鲍 史 唐 费 廉 岑 薛 雷 贺 倪 汤
滕 殷 罗 毕 郝 邬 安 常 乐 于 时 傅
皮 卞 齐 康 伍 余 元 卜 顾 孟 平 黄
和 穆 萧 尹 姚 邵 湛 汪 祁 毛 禹 狄
米 贝 明 臧 计 伏 成 戴 谈 宋 茅 庞
熊 纪 舒 屈 项 祝 董 梁 杜 阮 蓝 闵
席 季 麻 强 贾 路 娄 危 江 童 颜 郭
梅 盛 林 刁 锺 徐 邱 骆 高 夏 蔡 田
樊 胡 凌 霍 虞 万 支 柯 昝 管 卢 莫
经 房 裘 缪 干 解 应 宗 丁 宣 贲 邓
郁 单 杭 洪 包 诸 左 石 崔 吉 钮 龚
程 嵇 邢 滑 裴 陆 荣 翁 荀 羊 於 惠
甄 麴 家 封 芮 羿 储 靳 汲 邴 糜 松
井 段 富 巫 乌 焦 巴 弓 牧 隗 山 谷
车 侯 宓 蓬 全 郗 班 仰 秋 仲 伊 宫
宁 仇 栾 暴 甘 钭 历 戎 祖 武 符 刘
景 詹 束 龙 叶 幸 司 韶 郜 黎 蓟 溥
印 宿 白 怀 蒲 邰 从 鄂 索 咸 籍 赖
卓 蔺 屠 蒙 池 乔 阳 郁 胥 能 苍 双
闻 莘 党 翟 谭 贡 劳 逄 姬 申 扶 堵
冉 宰 郦 雍 却 璩 桑 桂 濮 牛 寿 通
边 扈 燕 冀 僪 浦 尚 农 温 别 庄 晏
柴 瞿 阎 充 慕 连 茹 习 宦 艾 鱼 容
向 古 易 慎 戈 廖 庾 终 暨 居 衡 步
都 耿 满 弘 匡 国 文 寇 广 禄 阙 东
欧 殳 沃 利 蔚 越 夔 隆 师 巩 厍 聂
晁 勾 敖 融 冷 訾 辛 阚 那 简 饶 空
曾 毋 沙 乜 养 鞠 须 丰 巢 关 蒯 相
查 后 荆 红 游 竺 权 逮 盍 益 桓 公
万俟 司马 上官 欧阳 夏侯 诸葛 闻人 东方 赫连 皇甫 尉迟 公羊
澹台 公冶 宗政 濮阳 淳于 单于 太叔 申屠 公孙 仲孙 轩辕 令狐
钟离 宇文 长孙 慕容 司徒 司空 召 有 舜 叶赫那拉 丛 岳
寸 贰 皇 侨 彤 竭 端 赫 实 甫 集 象
翠 狂 辟 典 良 函 芒 苦 其 京 中 夕
之 章佳 那拉 冠 宾 香 果 依尔根觉罗 依尔觉罗 萨嘛喇 赫舍里 额尔德特
萨克达 钮祜禄 他塔喇 喜塔腊 讷殷富察 叶赫那兰 库雅喇 瓜尔佳 舒穆禄 爱新觉罗 索绰络 纳喇
乌雅 范姜 碧鲁 张廖 张简 图门 太史 公叔 乌孙 完颜 马佳 佟佳
富察 费莫 蹇 称 诺 来 多 繁 戊 朴 回 毓 鉏
税 荤 靖 绪 愈 硕 牢 买 但 巧 枚 撒
泰 秘 亥 绍 以 壬 森 斋 释 奕 姒 朋
求 羽 用 占 真 穰 翦 闾 漆 贵 代 贯
旁 崇 栋 告 休 褒 谏 锐 皋 闳 在 歧
禾 示 是 委 钊 频 嬴 呼 大 威 昂 律
冒 保 系 抄 定 化 莱 校 么 抗 祢 綦
悟 宏 功 庚 务 敏 捷 拱 兆 丑 丙 畅
苟 随 类 卯 俟 友 答 乙 允 甲 留 尾
佼 玄 乘 裔 延 植 环 矫 赛 昔 侍 度
旷 遇 偶 前 由 咎 塞 敛 受 泷 袭 衅
叔 圣 御 夫 仆 镇 藩 邸 府 掌 首 员
焉 戏 可 智 尔 凭 悉 进 笃 厚 仁 业
肇 资 合 仍 九 衷 哀 刑 俎 仵 圭 夷
徭 蛮 汗 孛 乾 帖 罕 洛 淦 洋 邶 郸
郯 邗 邛 剑 虢 隋 蒿 茆 菅 苌 树 桐
锁 钟 机 盘 铎 斛 玉 线 针 箕 庹 绳
磨 蒉 瓮 弭 刀 疏 牵 浑 恽 势 世 仝
同 蚁 止 戢 睢 冼 种 涂 肖 己 泣 潜
卷 脱 谬 蹉 赧 浮 顿 说 次 错 念 夙
斯 完 丹 表 聊 源 姓 吾 寻 展 出 不
户 闭 才 无 书 学 愚 本 性 雪 霜 烟
寒 少 字 桥 板 斐 独 千 诗 嘉 扬 善
揭 祈 析 赤 紫 青 柔 刚 奇 拜 佛 陀
弥 阿 素 长 僧 隐 仙 隽 宇 祭 酒 淡
塔 琦 闪 始 星 南 天 接 波 碧 速 禚
腾 潮 镜 似 澄 潭 謇 纵 渠 奈 风 春
濯 沐 茂 英 兰 檀 藤 枝 检 生 折 登
驹 骑 貊 虎 肥 鹿 雀 野 禽 飞 节 宜
鲜 粟 栗 豆 帛 官 布 衣 藏 宝 钞 银
门 盈 庆 喜 及 普 建 营 巨 望 希 道
载 声 漫 犁 力 贸 勤 革 改 兴 亓 睦
修 信 闽 北 守 坚 勇 汉 练 尉 士 旅
五 令 将 旗 军 行 奉 敬 恭 仪 母 堂
丘 义 礼 慈 孝 理 伦 卿 问 永 辉 位
让 尧 依 犹 介 承 市 所 苑 杞 剧 第
零 谌 招 续 达 忻 六 鄞 战 迟 候 宛
励 粘 萨 邝 覃 辜 初 楼 城 区 局 台
原 考 妫 纳 泉 老 清 德 卑 过 麦 曲
竹 百 福 言 第五 佟 爱 年 笪 谯 哈 墨 连
南宫 赏 伯 佴 佘 牟 商 西门 东门 左丘 梁丘 琴
后 况 亢 缑 帅 微生 羊舌 海 归 呼延 南门 东郭
百里 钦 鄢 汝 法 闫 楚 晋 谷梁 宰父 夹谷 拓跋
壤驷 乐正 漆雕 公西 巫马 端木 颛孙 子车 督 仉 司寇 亓官 三小
鲜于 锺离 盖 逯 库 郏 逢 阴 薄 厉 稽 闾丘
公良 段干 开 光 操 瑞 眭 泥 运 摩 伟 铁 迮 禅 鬼 死
阿达来提 阿尔祖 阿丽同 阿米娜 阿娜尔 阿依帕夏 阿依夏木 阿依努尔 艾拉 巴哈尔 拜合蒂 
布里布里 塔吉古丽 提拉 吉乃斯太 乔丽潘 齐曼古丽 哈斯也提 罕扎代 哈古丽 海力且木 
迪丽拜尔 迪丽达尔 迪丽热巴 热娜 热依罕 茹仙 热孜宛 再乃甫 祖木热提 佐合热 孜巴 
萨阿代提 萨巴海提 沙拉买提 赛乃姆 赛娜瓦尔 夏热拜提 希仁 凯赛尔 坎曼尔 古海尔 
古再丽 古丽扎尔 古丽罕 古丽苏如合 古丽仙 古丽娜尔 古丽斯坦 麦尔哈巴 麦丽开 麦合布拜 
穆巴热克 穆坎妲斯 娜扎开提 努斯热提 尼格尔 尼鲁帕尔 尼叶蒂 赫木热 约日耶提 伊巴代提 
伊排提 尤丽吐孜 阿达莱提 阿丽屯 阿依夏姆 布力布力 乔力潘 奇曼古丽 哈斯叶提 罕古丽 
海丽且木 迪里拜尔 冉娜 热依汉 热孜婉 再乃普 祖姆热提 祖合热 沙巴海提 萨拉买提 赛乃木 
西仁 开萨尔 果海尔 古赞丽 古丽扎热 古丽苏茹合 古丽鲜 穆巴热科 妮尕尔 尼露拜尔 玉丽吐孜 
阿依谢姆 乔丽番 哈斯耶提 海丽且姆 萨拉麦提 希林
阿巴拜科日 阿不都热依木 阿不力孜 阿迪力 阿尔斯兰 阿扎提 阿勒玛斯 阿里木 阿曼 艾海提
艾合坦木 艾合买提 艾尔希鼎 艾尔肯 艾孜买提 艾克拜尔 艾力凯木 艾力 艾买提 艾尼瓦尔
艾沙 巴图尔 巴拉提 巴克 拜合提亚尔 布尔斯兰 包尔汉 帕孜力 普拉提 皮达 塔西布拉克
塔力普 托合提 坦吾皮克 吐尔地 吐尔逊 吐尔干 吐尔洪 铁木尔 提力瓦力迪 贾马力 健索尔
居热提 居麦 杰力力 哈力克 达尼西 达瓦买提 达伍提 迪亚尔 热合曼 热扎克 热介甫 热克甫
热赫木 如孜 如苏力 扎伊提 孜亚 沙比提 沙比尔 萨迪克 萨拉木 沙吾提 沙依提 赛排尔 赛尔旦
苏皮 苏里唐 色提瓦力迪 色依提 赛福鼎 谢力甫 谢木斯 仙拜 肖开提 雪合来提 许库尔 西力甫
哈孜 艾里甫 艾尼 卡德尔 卡热 喀斯木 喀伍力 阔其喀尔 库尔班 哈热买提 佧马力 佧米力 坎吉
克热木 麦吉侬 麦合苏提 麦合木提 麦苏木 买买提 穆太力普 穆合塔尔 穆再排尔 穆萨 穆罕买提
莫明 南比 尼加提 尼扎木 阿西木 哈克木 艾山 吾守尔 赫克木 赫克买提 奥斯曼 乌图克
乌齐坤 约麦尔 吾尔开希 瓦力斯 维力 艾则孜 艾依克 伊斯拉木 伊斯拉哈提 伊善 依干拜尔迪
伊里哈木 伊马木 伊敏 伊纳耶提 雅库甫 亚力坤 尧乐博斯 玉素甫 阿布都热依木 阿卜力孜 阿迪里
阿力玛斯 阿力木 艾合太木 艾合麦提 艾尔西丁 艾孜麦提 艾科拜尔 艾勒坎木 艾麦提 安外尔 艾萨
巴土尔 布尔汉 帕则力 塔力甫 吐尔迪 图尔荪 图尔干 图尔贡 加马力 居尔艾提 居玛 吉利力 哈里克
达尼什 热杰普 肉孜 肉索力 萨比提 萨比尔 司的克 斯拉木 萨吾提 萨依提 苏力坦 赛依提 赛皮丁
谢热甫 鲜拜 肖开特 雪合拉提 秀库尔 西热甫 喀迪尔 哈尔 哈斯木 哈米力 柯日木 麦麦提 穆台力甫
穆海麦提 莫敏 乃比 哈基姆 艾散 吾吐克 吾尔凯西 瓦热斯 伊禅 伊盖木拜尔迪 伊力哈木 依明 亚库甫
亚里坤 尧勒巴斯 阿地力 艾合旦木 艾克巴尔 塔里甫 图尔迪 吐尔孙 图尔洪 玉孜 哈迪尔 麦买提
吴尔凯西 额敏 亚库普 牙力昆 图尔逊 吐尔贡'''.decode('utf8')

pattern = re.compile(u"[^\u4e00-\u9fa5]+")  # 所有非汉字
pattern_split = re.compile(u"^[\u4e00-\u9fa5]+[0-9]+[\u4e00-\u9fa5]+")
pattern_split2 = re.compile(u"^[\u4e00-\u9fa5]+[(（][\u4e00-\u9fa5]+[)）][\u4e00-\u9fa5]+")
pattern_xj = re.compile(u"^[\u4e00-\u9fa5]+[·.，][\u4e00-\u9fa5]+")
nameList = re.split(pattern, nameStr)# 百家姓放入list中
singleNameDic = {}
multiNameList = []
for name in nameList:
    if len(name) == 1:
        singleNameDic[name] = 1 #对单个字的姓做赋值1，添加进去字典里
    if len(name) > 1:
        multiNameList.append(name) # 多个字的名字放进另一个nameList中


def getChinese(name):
    return re.sub(pattern, '', name)


def dropDot(name):
    return re.sub(re.compile("[ ]+"), '', name)


def isLastName(name, mod='both'):
    length = len(name)
    if mod == 'single':
        if singleNameDic.has_key(name[0]):
            return True
        else:
            return False
    elif mod == 'multi':
        for pre in multiNameList:
            if name.startswith(pre):
                return True
        return False
    elif mod == 'both':
        if singleNameDic.has_key(name[0]):
            return True
        for pre in multiNameList:
            if name.startswith(pre):
                return True
        return False
    else:
        return False


def hasNonCustomerKey(name):  # 判断是不是门店名称
    if name.endswith(u'店'):
        return True
    specialList = [u'门店', u'烟酒', u'商店', u'便利', u'供销', u'副食', u'商行', u'经销', u'代销', u'食杂', u'小卖', u'无字', u'糖烟', u'百货',
                   u'报刊', u'批发', u'超市', u'零售', u'消费', u'门市', u'烟摊', u'公司', u'食品', u'糖酒', u'特产', u'市部', u'摊点', u'调味',
                   u'电话', u'茶庄', u'餐馆', u'酒楼', u'宾馆', u'饭庄', u'茶馆', u'饭馆', u'酒家', u'烟柜', u'烟厂', u'购物', u'商城', u'网吧',
                   u'报亭', u'杂货', u'便民', u'代送', u'集团', u'售货', u'加油站', u'自卫', u'维修']
    for tmp in specialList:
        if tmp in name:
            return True
    return False


def hasInvalidName(name):
    invalidNames = [u'不访问', u'无名称', u'已停', u'停业', u'注销', u'集团', u'唐久', u'金虎', u'加油站', u'无名', u'暂停', u'烟车',
                    u'门面', u'歇业', u'官庄', u'出网', u'电结', u'临时', u'西南', u'城关', u'线办', u'流动', u'东区', u'取消', u'一星',
                    u'邮政', u'个体', u'蜀岗', u'批零', u'推车', u'售货', u'特业', u'夹江县', u'集市']
    for tmp in invalidNames:
        if tmp in name or tmp==name:
            return True
    return False


def splitChineseName(name):
    specialNames = [u'彩票', u'投注', u'号', u'单元', u'亭', u'部队', u'超市', u'国窖', u'连锁', u'店']
    switch = False
    for tem in specialNames:
        if tem in name:
            switch = True
    if re.match(pattern_split2, name):
        pattern3 = re.compile(u"[^\u4e00-\u9fa50-9()（）]+")
    elif re.match(pattern_split, name) or switch:
        pattern3 = re.compile(u"[^\u4e00-\u9fa50-9]+")
    else:
        pattern3 = re.compile(u"[^\u4e00-\u9fa5]+")
    result1 = re.split(pattern3, name)
    print result1
    result2 = re.split(pattern3, name.replace(' ', ''))
    print result2
    if result1 == None and result2 == None:
        return []
    if result1 == None:
        return result2
    if result2 == None:
        return result1
    result1.extend(result2)
    return list(set(result1))


def parseChineseName(name, province, cust_names, retail_names, invalid_names):
    if len(name) <= 1:
        invalid_names.append(name)
    elif len(name) >= 2 and len(name) <= 3:
        if not hasNonCustomerKey(name):
            cust_names.append(name)
        else:
            invalid_names.append(name)
    elif len(name) >= 4 and len(name) <= 6:
        if hasNonCustomerKey(name):
            retail_names.append(name)
        elif isLastName(name, 'multi') and not hasNonCustomerKey(name):
            cust_names.append(name)
        elif not isLastName(name, 'multi') and not hasNonCustomerKey(name):
            if province in ('新疆区', '内蒙区') or u'·' in name:
                cust_names.append(name)
            else:
                invalid_names.append(name)
        else:
            invalid_names.append(name)
    elif len(name) >= 7:
        if isLastName(name, 'multi') and not hasNonCustomerKey(name):
            cust_names.append(name)
        elif len(name) <= 10 and province == '新疆区' and not hasNonCustomerKey(name):
            cust_names.append(name)
        else:
            retail_names.append(name)
    else:
        invalid_names.append(name)


def drop_duplicated_name(name, result):
    if len(result) == 0:
        return True
    else:
        for exit_name in result:
            if exit_name in name:
                return False
        return True


def parseRetailName(retail_names):
    retail_names = list(set(retail_names))
    if len(retail_names) <= 1:
        return retail_names
    else:
        result_names = []
        retail_names.sort(key=lambda i: len(i), reverse=False)
        for name in retail_names:
            if drop_duplicated_name(name, result_names) and u'代送' not in name:
                result_names.append(name)
        result_names.sort(key=lambda i: len(i), reverse=True)
        return result_names


def parseCustName(cust_names):
    if len(cust_names) == 0:  # 后续增加对从店铺名中抽取姓名的补充
        return cust_names
    elif len(cust_names) == 1:
        if not hasInvalidName(cust_names[0]):
            return cust_names
        else:
            return []
    else:
        result = []
        for cust in cust_names:
            if cust not in result and not hasInvalidName(cust):
                result.append(cust)
        return result


def numToChinese(name):
    chageDict = {'0': u'零', '1': u'一', '2': u'二', '3': u'三', '4': u'四', '5': u'五', '6': u'六', '7': u'七', '8': u'八',
                 '9': u'九'}
    name = dropDot(name)
    specialNames = [u'彩票', u'投注', u'号', u'单元', u'亭', u'部队', u'超市', u'国窖', u'连锁', u'店']
    switch = False
    for tem in specialNames:
        if tem in name:
            switch = True
    if switch:
        newName = ''
        for x in name:
            if x in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
                newName = newName + chageDict[x]
            else:
                newName = newName + x
        return newName
    else:
        return name


def parseCustomerName(name, province, cust_names, retail_names, invalid_names):
    name = dropDot(name)
    chineseName = getChinese(name)
    if len(chineseName) == len(name):  # 无非中文字符
        parseChineseName(name, province, cust_names, retail_names, invalid_names)
    else:
        if len(chineseName) < 2:
            invalid_names.append(name)
        elif len(chineseName) >= 2 and len(chineseName) <= 3:
            if not hasNonCustomerKey(chineseName):
                cust_names.append(chineseName)
            else:
                retail_names.append(name)
        elif len(chineseName) > 3:
            if re.match(pattern_xj, name):  # 符合新疆姓名格式
                if hasNonCustomerKey(name):
                    retail_names.append(chineseName)
                else:
                    cust_names.append(chineseName)
            else:
                parseChineseName(chineseName, province, cust_names, retail_names, invalid_names)
                splitNames = splitChineseName(name)
                for split_name in splitNames:
                    parseChineseName(split_name, province, cust_names, retail_names, invalid_names)


def getCustFirst(alist, names, province):
    if len(alist) == 0:
        tmp_cust = []
        tmp_retail = []
        tmp_invalid = []
        retailNames = splitChineseName(names[1])
        for split_name in retailNames:
            parseChineseName(split_name, province, tmp_cust, tmp_retail, tmp_invalid)
        if len(tmp_cust) > 0:
            return tmp_cust[0]
        else:
            return 'null'
    elif len(alist) >= 1:
        return alist[0]


def getRetailFirst(alist):
    if len(alist) == 0:
        return 'null'
    elif len(alist) >= 1:
        return alist[0]


def parseRow(row):
    cust_code = row[0]
    names = row[1]
    province = row[2]
    cust_names = []
    retail_names = []
    invalid_names = []
    types = 0
    for name in names:
        parseCustomerName(name, province, cust_names, retail_names, invalid_names)
        if len(getChinese(name)) != len(name):
            types += 1

    result_cust_name = parseCustName(cust_names)
    result_retail_name = parseRetailName(retail_names)
    result_invalid_name = list(set(invalid_names))
    str_cust_name = getCustFirst(result_cust_name, names, province)
    str_retail_name = getRetailFirst(result_retail_name)
    # str_invalid_name = listToStr(result_invalid_name)
    org_artificial_person = names[0]
    org_cust_name = names[1]
    cust_orig_name = names[2]
    return (cust_code, str_cust_name, str_retail_name, result_cust_name, result_retail_name, result_invalid_name,
            len(result_cust_name), len(result_retail_name),
            len(result_invalid_name), types, org_artificial_person, org_cust_name, cust_orig_name)






def runParseName(table_name, mod):
    df = hc.sql(sql)
    checkRdd = df.rdd.map(parseRow)

    schema = StructType([StructField("cust_code", StringType(), True), \
                         StructField("dc_artificial_person", StringType(), True), \
                         StructField("dc_retail_name", StringType(), True), \
                         StructField("cust_names", ArrayType(StringType()), True), \
                         StructField("retail_names", ArrayType(StringType()), True), \
                         StructField("invalid_names", ArrayType(StringType()), True), \
                         StructField("len_cust_names", IntegerType(), True), \
                         StructField("len_retail_names", IntegerType(), True), \
                         StructField("len_name", IntegerType(), True), \
                         StructField("types", IntegerType(), True), \
                         StructField("artificial_person", StringType(), True), \
                         StructField("cust_name", StringType(), True), \
                         StructField("cust_orig_name", StringType(), True)])

    checkResult = hc.createDataFrame(checkRdd, schema)
    checkResult.write.saveAsTable("bdp.test", mode="overwrite")
    checkResult.show(10)


if __name__ == '__main__':
    # runParseName()