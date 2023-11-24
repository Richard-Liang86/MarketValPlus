# 此脚本用于购买网站数据后合并数据
import requests

from Functions import *
from Config import *
import json  # python自带的json数据库
import platform
from random import randint  # python自带的随机数库
from datetime import datetime, time
from multiprocessing import Pool, freeze_support, cpu_count
from urllib.request import urlopen  # python自带爬虫库
import re  # 正则表达式库
import time

current_path = os.path.dirname(__file__)
os.chdir(current_path)

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数


# =====创建随机数的函数
def _random(n=16):
    """
    创建一个n位的随机整数
    :param n:
    :return:
    """
    start = 10 ** (n - 1)
    end = (10 ** n) - 1

    return str(randint(start, end))


# num = 30  # 股票最多不能超过640，指数、etf等没有限制
# k_type = 'day'  # day, week, month分别对用日线、周线、月线
# 先读取用于更新的文件，然后找到对应的主csv文件，然后把新文件拼接到旧文件后边，然后保存
def update_stock(code, num=25, k_type='day'):
    # 读取旧数据
    old_path = root_path + r'\data\daily_data\stock\%s.csv' % code
    df_all = pd.read_csv(old_path, encoding='gbk', skiprows=1)
    # 判断一下股票数据是否只欠一天数据，如果只欠一天数据，那么就到东方财富下载一天数据，如果欠多天数据，就到腾讯下载数据
    timedelta = datetime.now().date() - datetime.strptime(df_all.iloc[-1]['交易日期'], '%Y-%m-%d').date()
    # timedelta = datetime.now().date() - datetime.strptime(df_all.iloc[-1]['交易日期'], '%Y/%m/%d').date()
    # print(timedelta.days)

    if timedelta.days != 0:
        # 需要更新数据
        # 如果只差了一天的数据，那就只更新一天的数据
        if timedelta.days == 1:
            df_new = get_today_stock_values_from_eastmoney(code)
            # 合并、去掉重复行
            df_all = pd.concat([df_all, df_new], axis=0, ignore_index=True)
            df_all['交易日期'] = pd.to_datetime(df_all['交易日期'], infer_datetime_format=True)
            df_all.drop_duplicates(subset=['交易日期'], keep='last', inplace=True)
            df_all = df_all.astype(
                {'开盘价': 'float', '最高价': 'float', '最低价': 'float', '收盘价': 'float', '前收盘价': 'float', '成交量': 'float',
                 '成交额': 'float'})
            # print(df_all)
            # exit()
        else:
            url = 'http://web.ifzq.gtimg.cn/appstock/app/fqkline/get?_var=kline_%sqfq&param=%s,%s,,,%s,qfq&r=0.%s'
            url = url % (k_type, code, k_type, num, _random())
            # ===获取数据
            max_try_count = 3
            try_count = 0
            k_data = ''
            # 需要更新多天的数据
            while True:
                try:
                    content = urlopen(url).read().decode()  # 使用python自带的库，从网络上获取信息
                    # ===将数据转换成dict格式
                    content = content.split('=', maxsplit=1)[-1]
                    content = json.loads(content)  # 自己去仔细看下这里面有什么数据
                    # ===将数据转换成DataFrame格式
                    k_data = content['data'][code]
                    if k_type in k_data:
                        k_data = k_data[k_type]
                    elif 'qfq' + k_type in k_data:  # qfq是前复权的缩写
                        k_data = k_data['qfq' + k_type]
                    else:
                        raise ValueError('已知的key在dict中均不存在，请检查数据')
                except Exception as e:
                    print(e)  # 把exception输出出来
                    print(code + '警告！获取数据失败，停止1秒再尝试')
                    try_count += 1
                    time.sleep(1)
                    if try_count >= max_try_count:
                        print(code + '超过最大尝试次数，获取失败')
                        # 此处需要执行相关程序，通知某些人
                        break
                    else:
                        continue
                else:  # 如果没有报错
                    break
            df_new = pd.DataFrame(k_data)
            # ===对数据进行整理
            rename_dict = {0: '交易日期', 1: '开盘价', 2: '收盘价', 3: '最高价', 4: '最低价', 5: '成交量', 6: 'info'}
            # 其中amount单位是手，说明数据不够精确
            df_new.rename(columns=rename_dict, inplace=True)
            df_new['交易日期'] = df_new['交易日期'].str.replace('/', '-')
            df_new['成交量'] = df_new.成交量.astype('float')
            df_new['收盘价'] = df_new.收盘价.astype('float')
            # print(df_new.dtypes)
            df_new['成交量'] = df_new['成交量'] * 100
            df_new['成交额'] = df_new['收盘价'] * df_new['成交量']
            df_new = df_new[['交易日期', '开盘价', '最高价', '最低价', '收盘价', '成交量', '成交额']]
            # 合并、去掉重复行
            df_all = pd.concat([df_all, df_new], axis=0, ignore_index=True)
            df_all.drop_duplicates(subset=['交易日期'], keep='first', inplace=True)
            df_all.sort_values(by=['交易日期'], ascending=1)
            df_all['前收盘价'] = df_all['前收盘价'].fillna(df_all['收盘价'].shift(1))
            # df_all[['股票代码', '股票名称', '流通市值', '总市值', '净利润TTM', '现金流TTM', '净资产', '总资产', '总负债', '净利润(当季)']] = df_all[
            #     ['股票代码', '股票名称', '流通市值', '总市值', '净利润TTM', '现金流TTM', '净资产', '总资产', '总负债', '净利润(当季)']].fillna(
            #     method='ffill')
            df_all[['股票代码', '股票名称', '流通市值', '总市值']] = df_all[
                ['股票代码', '股票名称', '流通市值', '总市值']].fillna(
                method='ffill')
            df_all = df_all.astype(
                {'开盘价': 'float', '最高价': 'float', '最低价': 'float', '收盘价': 'float', '前收盘价': 'float', '成交量': 'float',
                 '成交额': 'float'})
            df_all['交易日期'] = pd.to_datetime(df_all['交易日期'], infer_datetime_format=True)

    # df_all = df_all[['股票代码', '股票名称', '交易日期', '开盘价', '最高价', '最低价', '收盘价', '前收盘价', '成交量', '成交额', '流通市值', '总市值']]

    # print(df_all[-30:])

    # 数据头必须写回来~
    old_path = root_path + '/data/daily_data/stock_for_select_targets/%s.csv' % code
    # 数据头必须写回来~
    pd.DataFrame(columns=['数据由邢不行整理']).to_csv(old_path, index=False, encoding='gbk')
    # # 覆盖保存
    df_all.to_csv(old_path, mode='a', encoding='gbk', index=False)

    # =====从东方财富获取当时股票市值数据


def get_today_data_from_gtimg(code_list):
    # 构建url
    url = 'https://web.sqt.gtimg.cn/q=' + code_list[0] + '?r=0.' + _random(18)
    # 抓取数据
    content = get_content_from_internet(url)
    content = content.decode('gbk')

    # 将数据转换成DataFrame
    content = content.strip()  # 去掉文本前后的空格、回车等
    data_line = content.split('\n')  # 每行是一个股票的数据  ；将content内容根据'\n'换行，做切分，然后将每个部分放入列表中
    data_line = [i.split('~') for i in
                 data_line]  # 批量将'var hq_str_'替换，即删除，然后元素一逗号切割，形成列表中套列表的数据
    df = pd.DataFrame(data_line, dtype='float')

    # 对DataFrame进行整理
    df['stock_code'] = df[0].str[2:10]
    df['candle_end_time'] = df[30].astype(str).str[:14]  # 股票市场的K线，是普遍以当跟K线结束时间来命名的
    df['candle_end_time'] = pd.to_datetime(df['candle_end_time'])
    rename_dict = {1: 'stock_name', 3: 'close', 4: 'pre_close', 5: 'open', 33: 'high', 34: 'low',
                   36: 'amount', 37: 'volume'}  # 自己去对比数据，会有新的返现
    # 其中amount单位是股，volume单位是元
    df.rename(columns=rename_dict, inplace=True)
    # df['status'] = df['status'].str.strip('";')
    df = df[['stock_code', 'stock_name', 'candle_end_time', 'open', 'high', 'low', 'close', 'pre_close', 'amount',
             'volume']]
    return df


def get_content_from_internet(url, max_try_num=30, sleep_time=20):
    get_success = False
    for i in range(max_try_num):
        try:
            content = urlopen(url=url, timeout=10).read()
            # content = requests.get(url, params=params, headers=headers)
            get_success = True
            break
        except Exception as e:
            print('抓取数据报错，次数：', i + 1, '报错内容：', e)
            time.sleep(sleep_time)

    if get_success:
        return content
    else:
        raise ValueError('使用urlopen抓取网页数据不断报错，达到尝试上限，停止程序，请尽快检查问题所在')


def get_today_stock_values_from_eastmoney(stock_code):
    '''
    :param file_location: 数据所在文件夹
    :return:
    '''
    # # ===获取上证指数最近一个交易日的日期。
    df = get_today_data_from_gtimg(code_list=['sh000001'])
    sh_date = df.iloc[0]['candle_end_time'].date()  # 上证指数最近交易日
    # 获取随机数
    num21 = _random(n=21)
    num12 = _random(n=12)
    num121 = _random(n=12)
    df = pd.DataFrame()
    n = 0

    print('开始获取%s的市值数据' % stock_code)
    if stock_code.startswith('sh'):
        stock_code_ = '1.' + stock_code[2:8]
    else:
        stock_code_ = '0.' + stock_code[2:8]
    url = 'http://push2.eastmoney.com/api/qt/stock/get?ut=fa5fd1943c7b386f172d6893dbfba10b&invt=2&fltt=2&fields=' \
          'f80,f46,f44,f45,f43,f60,f47,f48,f58,f116,f117&secid=%s&cb=jQuery%s_%s&_=%s' % (
              stock_code_, num21, num12, num121)
    # 抓取网页数据
    content = get_content_from_internet(url, max_try_num=10, sleep_time=5)
    content = content.decode('utf-8')
    content = re.search(r'"data":(.+)}', content).group(1)
    if content == 'null':
        pass
    else:
        content = eval(content)
        # time_= re.split(r'[:,]',content['f80'][1:-1])
        # time_ = time_[1][:8]
        if content['f46'] == '-':
            pass
        else:
            df.loc[n, '股票代码'] = stock_code
            df.loc[n, '股票名称'] = content['f58']
            df.loc[n, '交易日期'] = str(pd.to_datetime(sh_date).date())
            df.loc[n, '开盘价'] = content['f46']
            df.loc[n, '最高价'] = content['f44']
            df.loc[n, '最低价'] = content['f45']
            df.loc[n, '收盘价'] = content['f43']
            df.loc[n, '前收盘价'] = content['f60']
            df.loc[n, '成交量'] = content['f47'] * 100
            df.loc[n, '成交额'] = content['f48']
            df.loc[n, '流通市值'] = content['f117']
            df.loc[n, '总市值'] = content['f116']
            n += 1

    return df


if __name__ == '__main__':
    # 测试用sh600000
    # df = get_today_stock_values_from_eastmoney('sh600000')
    # print(df)
    # update_stock('sh600000')
    # exit()

    # 读取所有股票代码的列表 最新的数据需要放到指定的目录
    stock_code_list = get_stock_code_list_in_one_dir(root_path + '/data/daily_data/stock')  # 获取目录内的所有股票的文件的路径
    stock_code_list = [code for code in stock_code_list]
    print('股票数量：', len(stock_code_list))

    # 开启多线程，处理股票数据
    # 添加对windows多进程的支持
    # https://docs.python.org/zh-cn/3.7/library/multiprocessing.html
    if 'Windows' in platform.platform():
        freeze_support()

    # 标记开始时间
    start_time = datetime.now()

    multiple_process = True
    # 标记开始时间
    if multiple_process:
        # 开始并行
        with Pool(max(cpu_count() - 1, 1)) as pool:
            # 使用并行批量获得data frame的一个列表
            df_list = pool.map(update_stock, sorted(stock_code_list))
    else:
        exit()

    # 看一下花了多久
    print('完成任务，所需时间： ' + str(datetime.now() - start_time))
