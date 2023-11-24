import json
from datetime import datetime
import time
import random
from urllib.request import urlopen
from Functions import *


def get_content_from_internet(url, max_try_num=10, sleep_time=5):
    """
    使用python自带的urlopen函数，从网页上抓取数据
    :param url: 要抓取数据的网址
    :param max_try_num: 最多尝试抓取次数
    :param sleep_time: 抓取失败后停顿的时间
    :return: 返回抓取到的网页内容
    .decode('gbk')我是直接在这里编码了
    """
    get_success = False  # 是否成功抓取到内容
    # 抓取内容
    for i in range(max_try_num):
        try:
            content = urlopen(url=url, timeout=10).read().decode(
                'utf-8')  # 使用python自带的库，从网络上获取信息，timeout=10给10秒时间读取，10秒内读取不到则放弃
            get_success = True  # 成功抓取到内容
            break
        except Exception as e:
            print('抓取数据报错，次数：', i + 1, '报错内容：', e)
            time.sleep(sleep_time)

    # 判断是否成功抓取内容
    if get_success:
        return content
    else:
        raise ValueError('使用urlopen抓取网页数据不断报错，达到尝试上限，停止程序，请尽快检查问题所在')


def get_his_cwyg_report_date(max_number=2):
    """
    max_year # 获取最近几年的预告数据
    获取财务预告数据的报告期
    get_his_cwyg_report_date函数中的max_number，指的是获取过去多少年的财务预告数据。
    :return:
    """
    report_month_list = ['-03-31', '-06-30', '-09-30', '-12-31']
    report_date_list = []
    for i in range(max_number):
        report_year = str(datetime.now().year - i)
        # print(report_year)
        for report_month in report_month_list:
            report_date = report_year + report_month
            report_date_list.append(report_date)

    return report_date_list


def get_his_cwsj_from_eastmoney_marketcenter(path):
    """
    用于实盘获取
    # ===数据网址
    # raw_url = 'http://datacenter-web.eastmoney.com/securities/api/data/v1/get?callback=
    # jQuery1123022427926859779657_1634230238241&sortColumns=NOTICE_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=50
    # &pageNumber=1&reportName=RPT_PUBLIC_OP_NEWPREDICT&columns=ALL&token=894050c76af8597a853f5b408b759f5d&filter=
    # (REPORT_DATE%3D%272021-09-30%27)'
    get_his_cwsj_from_eastmoney_marketcenter中的path保存路径需要自己设定，建议用report_date来命名文件。
    """
    # ===存储数据的DataFrame
    all_df = pd.DataFrame()
    #
    report_date_list = get_his_cwyg_report_date()

    for report_date in report_date_list:
        page_num = 1
        raw_url = 'http://datacenter-web.eastmoney.com/securities/api/data/v1/get?callback='
        raw_url2 = '&sortColumns=NOTICE_DATE%2CSECURITY_CODE&sortTypes=-1%2C-1&pageSize=500&pageNumber='
        ts = int(time.time()) - 1
        ts2 = int(time.time())
        cb = 'jQuery1123022427926859779657_'
        db = '&reportName=RPT_PUBLIC_OP_NEWPREDICT&columns=ALL&token=894050c76af8597a853f5b408b759f5d&filter=' \
             '(REPORT_DATE%3D%27' + report_date + '%27)'  # 改时间（报告期）试试

        # ===开始逐页遍历，获取股票数据
        while True:
            # 构建url
            url = raw_url + cb + str(ts) + raw_url2 + str(page_num) + db
            # print(page_num)
            # print(url)
            print('开始抓取页数：', page_num)

            # 抓取数据
            # get_content_from_internet需要直接解码为'utf-8'
            # 实盘函数不一样
            content = get_content_from_internet(url)
            # print(content)
            # # 将数据转换成dict格式
            content = content.strip()
            content = content.replace(cb + str(ts), '')[:-2]
            content = content.replace('(', '')
            content = json.loads(content)
            content = content['result']
            if content == None:
                print('抓取到页数的尽头，退出循环')
                break
            page = content['pages']
            content = content['data']
            df = pd.DataFrame(content, dtype='float')
            # # 重命名
            rename_dict = {'SECUCODE': '股票代码', 'SECURITY_NAME_ABBR': '股票名称', 'NOTICE_DATE': '发布日期',
                           'REPORT_DATE': '报告期', 'PREDICT_FINANCE': '预告指标名称', 'PREDICT_AMT_LOWER': '预告指标下限',
                           'PREDICT_AMT_UPPER': '预告指标上限', 'ADD_AMP_LOWER': '预告指标同比增长下限', 'ADD_AMP_UPPER': '预告指标同比增长上限'
                , 'PREDICT_CONTENT': '预告内容', 'CHANGE_REASON_EXPLAIN': '预告简况', 'PREDICT_TYPE': '预告指标类型'
                , 'PREYEAR_SAME_PERIOD': '上年同期值(元)', 'TRADE_MARKET': '所属板块'}
            df.rename(columns=rename_dict, inplace=True)

            # 添加交易日期
            df['发布日期'] = pd.to_datetime(df['发布日期'])  # 在课程视频中使用的是上一行代码，现在改成本行代码，程序更加稳健
            df.sort_values('发布日期', ascending=False, inplace=True)
            # 取需要的列
            df = df[['股票代码', '股票名称', '发布日期', '报告期', '预告指标名称', '预告指标下限', '预告指标上限', '预告指标同比增长下限', '预告指标同比增长上限',
                     '预告内容', '预告简况', '预告指标类型', '上年同期值(元)', '所属板块']]
            # 合并数据
            all_df = all_df.append(df, ignore_index=True)
            # print(all_df[-100:])
            # 将页数+1
            page_max = 10
            page_num += 1
            # print(page_num)

            time.sleep(random.randint(1, 5))

    all_df.reset_index(inplace=True, drop=True)
    all_df.sort_values('发布日期', ascending=False, inplace=True)
    all_df.to_csv(
        # 地址需要变动
        path,
        index=False,
        encoding='utf-8-sig',  # utf-8-sig写入CSV不会出现乱码
        mode='w'
    )

    # ===返回结果
    return all_df


if __name__ == '__main__':
    date = str(datetime.now().date()).replace("-", "")
    df = get_his_cwsj_from_eastmoney_marketcenter(
        r'F:\MyQuantsProjects\MarketValPlus\data\fin_data\fin_test\财务数据%s.csv' % date)
    # print(df)
