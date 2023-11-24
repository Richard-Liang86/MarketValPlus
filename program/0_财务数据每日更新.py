import os
from datetime import datetime

import pandas as pd

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数


def update_stock_data(new_path, old_data_path, stock_data_path):
    # 读取最新财务数据的csv文件
    global file_path
    df = pd.read_csv(new_path, encoding='utf-8-sig', skiprows=0,
                     usecols=['股票代码', '股票名称', '预告指标名称', '发布日期', '报告期', '预告指标下限', '预告指标上限', '预告指标同比增长下限', '预告指标同比增长上限',
                              '上年同期值(元)'])
    df['stock_code'] = df['股票代码'].str.split('.').str[1].str.lower() + df['股票代码'].str.split('.').str[0]
    df['预告指标'] = (df['预告指标下限'] + df['预告指标上限']) / 2
    df['预告指标同比增长'] = (df['预告指标同比增长下限'] + df['预告指标同比增长上限']) / 2
    df['报告期'] = df['报告期'].str[:10]
    df.sort_values(['发布日期', '股票代码'], ascending=False, inplace=True)  # ===将数据存入数据库之前，先排序、reset_index
    # print(df)

    # 遍历每一条信息，更新财务信息
    for code, group in df.groupby(['stock_code', '发布日期']):

        old_stock_path = old_data_path + r'\%s/' % code[0]

        # 只有当该数据为已有股票数据才处理，否则略过，不操作新股
        if os.path.exists(old_stock_path):
            print(code)
            # print(group)
            data_list = group['预告指标名称'].tolist()

            # 读取旧文件
            for path, folder, files in os.walk(old_stock_path):
                file_path = old_stock_path + files[0]
            df_old = pd.read_csv(file_path, encoding='gbk', skiprows=1)
            # print(df_old)

            stock_dict = {}
            if '每股收益' in data_list:
                stock_dict['R_basic_eps@xbx'] = group[group['预告指标名称'] == '每股收益']['预告指标'].values[0]
                report_date = group[group['预告指标名称'] == '每股收益']['报告期'].values[0].replace('-', '')
                stock_dict['report_date'] = report_date
                publish_date = group[group['预告指标名称'] == '每股收益']['发布日期'].values[0]
                stock_dict['publish_date'] = publish_date

                # last_reportdate = int(str(int(report_date[:4]) - 1) + report_date[4:])
                # print(last_reportdate)
                # last_eps = df_old[df_old['report_date'] == last_reportdate]['R_basic_eps@xbx'].tolist()[-1]
                # print('上一期每股收益：' + str(last_eps))
                # print('本期每股收益' + str(stock_dict['R_basic_eps@xbx']))
            elif '扣非后每股收益' in data_list:
                stock_dict['R_basic_eps@xbx'] = group[group['预告指标名称'] == '扣非后每股收益']['预告指标'].values[0]
                report_date = group[group['预告指标名称'] == '扣非后每股收益']['报告期'].values[0].replace('-', '')
                stock_dict['report_date'] = report_date
                publish_date = group[group['预告指标名称'] == '扣非后每股收益']['发布日期'].values[0]
                stock_dict['publish_date'] = publish_date
                # last_reportdate = int(str(int(report_date[:4]) - 1) + report_date[4:])
                # last_eps = df_old[df_old['report_date'] == last_reportdate]['R_basic_eps@xbx'].tolist()[-1]
                # print('上一期每股收益：' + str(last_eps))
                # print('本期每股收益' + str(stock_dict['R_basic_eps@xbx']))

            if '归属于上市公司股东的净利润' in data_list:
                stock_dict['R_np_atoopc@xbx'] = group[group['预告指标名称'] == '归属于上市公司股东的净利润']['预告指标'].values[0]

                # 如果此时每股收益为空，可以通过估算来啊得到每股收益，每股收益≈归属于上市公司股东的净利润/总股数
                if stock_dict.get('R_basic_eps@xbx') is None:
                    stock_data_path_1 = stock_data_path + r'\%s.csv' % code[0]
                    # print(stock_data_path_1)
                    if os.path.exists(stock_data_path_1):
                        df_stock = pd.read_csv(stock_data_path_1, encoding='gbk', skiprows=1, usecols=['收盘价', '总市值'])
                        df_stock['stock_num'] = df_stock['总市值'] / df_stock['收盘价']
                        stock_num = df_stock.iloc[-1]['stock_num']
                        stock_dict['R_basic_eps@xbx'] = stock_dict['R_np_atoopc@xbx'] / stock_num
                        # print('本期每股收益：' + str(stock_dict['R_basic_eps@xbx']))
                        # 从旧数据中找去去年同期的收益
                        report_date = group[group['预告指标名称'] == '归属于上市公司股东的净利润']['报告期'].values[0].replace('-', '')
                        stock_dict['report_date'] = report_date
                        publish_date = group[group['预告指标名称'] == '归属于上市公司股东的净利润']['发布日期'].values[0]
                        stock_dict['publish_date'] = publish_date
                        # last_reportdate = int(str(int(report_date[:4]) - 1) + report_date[4:])
                        # # 不一定能取到值
                        # last_eps = df_old[df_old['report_date'] == last_reportdate]['R_basic_eps@xbx'].tolist()
                        # if last_eps:
                        #     print('上一期每股收益：' + str(last_eps[-1]))
            elif '净利润' in data_list:
                stock_dict['R_np_atoopc@xbx'] = group[group['预告指标名称'] == '净利润']['预告指标'].values[0]

                # 如果此时每股收益为空，可以通过估算来啊得到每股收益，每股收益≈归属于上市公司股东的净利润/总股数
                if stock_dict.get('R_basic_eps@xbx') is None:
                    stock_data_path_1 = stock_data_path + r'\%s.csv' % code[0]
                    # print(stock_data_path_1)
                    if os.path.exists(stock_data_path_1):
                        df_stock = pd.read_csv(stock_data_path_1, encoding='gbk', skiprows=1, usecols=['收盘价', '总市值'])
                        df_stock['stock_num'] = df_stock['总市值'] / df_stock['收盘价']
                        stock_num = df_stock.iloc[-1]['stock_num']
                        stock_dict['R_basic_eps@xbx'] = stock_dict['R_np_atoopc@xbx'] / stock_num
                        # print('本期每股收益：' + str(stock_dict['R_basic_eps@xbx']))
                        report_date = group[group['预告指标名称'] == '净利润']['报告期'].values[0].replace('-', '')
                        stock_dict['report_date'] = report_date
                        publish_date = group[group['预告指标名称'] == '净利润']['发布日期'].values[0]
                        stock_dict['publish_date'] = publish_date
            elif '扣除非经常性损益后的净利润' in data_list:
                stock_dict['R_np_atoopc@xbx'] = group[group['预告指标名称'] == '扣除非经常性损益后的净利润']['预告指标'].values[0]

                # 如果此时每股收益为空，可以通过估算来啊得到每股收益，每股收益≈归属于上市公司股东的净利润/总股数
                if stock_dict.get('R_basic_eps@xbx') is None:
                    stock_data_path_1 = stock_data_path + r'\%s.csv' % code[0]
                    # print(stock_data_path_1)
                    if os.path.exists(stock_data_path_1):
                        df_stock = pd.read_csv(stock_data_path_1, encoding='gbk', skiprows=1, usecols=['收盘价', '总市值'])
                        df_stock['stock_num'] = df_stock['总市值'] / df_stock['收盘价']
                        stock_num = df_stock.iloc[-1]['stock_num']
                        stock_dict['R_basic_eps@xbx'] = stock_dict['R_np_atoopc@xbx'] / stock_num
                        # print('本期每股收益：' + str(stock_dict['R_basic_eps@xbx']))
                        report_date = group[group['预告指标名称'] == '扣除非经常性损益后的净利润']['报告期'].values[0].replace('-', '')
                        stock_dict['report_date'] = report_date
                        publish_date = group[group['预告指标名称'] == '扣除非经常性损益后的净利润']['发布日期'].values[0]
                        stock_dict['publish_date'] = publish_date

            if '营业收入' in data_list:
                stock_dict['R_revenue@xbx'] = group[group['预告指标名称'] == '营业收入']['预告指标'].values[0]

                # 按比例计算出每股收益
                if stock_dict.get('R_basic_eps@xbx') is None:
                    # 从旧数据中找去去年同期的收益
                    report_date = group[group['预告指标名称'] == '营业收入']['报告期'].values[0].replace('-', '')
                    stock_dict['report_date'] = report_date
                    publish_date = group[group['预告指标名称'] == '营业收入']['发布日期'].values[0]
                    stock_dict['publish_date'] = publish_date
                    last_reportdate = int(str(int(report_date[:4]) - 1) + report_date[4:])
                    # 不一定能取到值
                    last_eps = df_old[df_old['report_date'] == last_reportdate]['R_basic_eps@xbx'].tolist()
                    last_eps = [float(x) for x in last_eps]

                    if last_eps:
                        last_revenue = group[group['预告指标名称'] == '营业收入']['上年同期值(元)'].values[0]
                        if last_revenue:
                            ratio = stock_dict['R_revenue@xbx'] / last_revenue
                            stock_dict['R_basic_eps@xbx'] = last_eps[0] * ratio
                        # print('上一期每股收益：' + str(last_eps[-1]))
                        # print('本期每股收益：' + str(stock_dict['R_basic_eps@xbx']))
            elif '主营业务收入' in data_list:
                stock_dict['R_revenue@xbx'] = group[group['预告指标名称'] == '主营业务收入']['预告指标'].values[0]

                # 按比例计算出每股收益
                if stock_dict.get('R_basic_eps@xbx') is None:
                    # 从旧数据中找去去年同期的收益
                    report_date = group[group['预告指标名称'] == '主营业务收入']['报告期'].values[0].replace('-', '')
                    stock_dict['report_date'] = report_date
                    publish_date = group[group['预告指标名称'] == '主营业务收入']['发布日期'].values[0]
                    stock_dict['publish_date'] = publish_date
                    last_reportdate = int(str(int(report_date[:4]) - 1) + report_date[4:])
                    # 不一定能取到值
                    last_eps = df_old[df_old['report_date'] == last_reportdate]['R_basic_eps@xbx'].tolist()
                    if last_eps:
                        last_revenue = group[group['预告指标名称'] == '主营业务收入']['上年同期值(元)'].values[0]
                        if last_revenue:
                            ratio = stock_dict['R_revenue@xbx'] / last_revenue
                            stock_dict['R_basic_eps@xbx'] = last_eps * ratio
                        # print('上一期每股收益：' + str(last_eps[-1]))
                        # print('本期每股收益：' + str(stock_dict['R_basic_eps@xbx']))
            elif '扣除后营业收入' in data_list:
                stock_dict['R_revenue@xbx'] = group[group['预告指标名称'] == '扣除后营业收入']['预告指标'].values[0]

                # 按比例计算出每股收益
                if stock_dict.get('R_basic_eps@xbx') is None:
                    # 从旧数据中找去去年同期的收益
                    report_date = group[group['预告指标名称'] == '扣除后营业收入']['报告期'].values[0].replace('-', '')
                    stock_dict['report_date'] = report_date
                    publish_date = group[group['预告指标名称'] == '扣除后营业收入']['发布日期'].values[0]
                    stock_dict['publish_date'] = publish_date
                    last_reportdate = int(str(int(report_date[:4]) - 1) + report_date[4:])
                    # 不一定能取到值
                    last_eps = df_old[df_old['report_date'] == last_reportdate]['R_basic_eps@xbx'].tolist()
                    if last_eps:
                        last_revenue = group[group['预告指标名称'] == '扣除后营业收入']['上年同期值(元)'].values[0]
                        if last_revenue:
                            ratio = stock_dict['R_revenue@xbx'] / last_revenue
                            stock_dict['R_basic_eps@xbx'] = last_eps * ratio
                        print('上一期每股收益：' + str(last_eps[-1]))
                        print('本期每股收益：' + str(stock_dict['R_basic_eps@xbx']))
            stock_dict['R_basic_eps@xbx'] = round(stock_dict['R_basic_eps@xbx'], 3)
            stock_dict['stock_code'] = code[0]

            print(stock_dict)

            # 只有两个关键数据都存在的时候，才更新财务数据
            if stock_dict.get('R_basic_eps@xbx') and stock_dict.get('R_np_atoopc@xbx'):
                df_old = df_old.append(stock_dict, ignore_index=True)
                df_old.drop_duplicates(subset=['report_date', 'publish_date'], keep='last', inplace=True)
                print('数据添加成功')

            print(df_old[-10:][['stock_code', 'report_date', 'publish_date', 'R_basic_eps@xbx', 'R_np_atoopc@xbx',
                                'R_revenue@xbx']])

            print(file_path)
            # 数据头必须写回来~
            pd.DataFrame(columns=['数据由邢不行整理']).to_csv(file_path, index=False, encoding='gbk')
            # 覆盖保存
            df_old.to_csv(file_path, mode='a', encoding='gbk', index=False)
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')


if __name__ == '__main__':
    date = str(datetime.now().date()).replace("-", "")
    new_data_path = r'F:\MyQuantsProjects\MarketValPlus\data\fin_data\fin_test\最新财务数据%s.csv' % date
    old_data_path = r'F:\MyQuantsProjects\MarketValPlus\data\fin_data\fin_xbx'
    stock_data_path = r'F:\MyQuantsProjects\MarketValPlus\data\daily_data\stock_for_select_targets'
    update_stock_data(new_data_path, old_data_path, stock_data_path)
