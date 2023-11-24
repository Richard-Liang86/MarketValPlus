from random import randint
import requests, os, json, re, time
import pandas as pd

index_list = ['sh000300']
all_data_path = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径

headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36 Core/1.94.201.400 QQBrowser/11.9.5325.400'

}
for index in index_list:
    print(index)
    url = 'https://proxy.finance.qq.com/ifzqgtimg/appstock/app/newfqkline/get'
    start_time = '1900-01-01'
    end_time = ''
    df_list = []
    while True:
        params = {
            '_var': 'kline_dayqfq',
            'param': f'{index},day,{start_time},{end_time},2000,qfq',
            'r': f'0.{randint(10 ** 15, (10 ** 16) - 1)}',
        }
        res = requests.get(url, params=params, headers=headers)
        res_json = json.loads(re.findall('kline_dayqfq=(.*)', res.text)[0])
        if res_json['code'] == 0:
            _df = pd.DataFrame(res_json['data'][index]['day'])
            df_list.append(_df)
            if _df.shape[0] <= 1:
                break
            end_time = _df.iloc[0][0]
        time.sleep(2)
    df = pd.concat(df_list, ignore_index=True)
    # ===对数据进行整理
    rename_dict = {0: 'candle_end_time', 1: 'open', 2: 'close', 3: 'high', 4: 'low', 5: 'amount', 6: 'info'}
    # 其中amount单位是手，说明数据不够精确
    df.rename(columns=rename_dict, inplace=True)
    df['candle_end_time'] = pd.to_datetime(df['candle_end_time'])
    df.drop_duplicates('candle_end_time', inplace=True)  # 去重
    df.sort_values('candle_end_time', inplace=True)
    df['candle_end_time'] = df['candle_end_time'].dt.strftime('%Y-%m-%d')
    if 'info' not in df:
        df['info'] = None
    df = df[['candle_end_time', 'open', 'high', 'low', 'close', 'amount', 'info']]
    to_csv_path = all_data_path + r'\..\data\index_data'
    if not os.path.exists(to_csv_path):  # 判断文件夹是否存在
        os.makedirs(to_csv_path)  # 不存在则创建
    df.to_csv(to_csv_path + r'\%s.csv' % index, index=False, encoding='gbk')
