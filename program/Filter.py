"""
《邢不行-2023新版|Python股票量化投资课程》
author: 邢不行
微信: xbx9585

选股使用的过滤的脚本
"""

from Config import *


def filter_and_rank(df, stg_name):
    """
    过滤股票 & 选股票
    :param df: 原始数据
    :param stg_name: 策略名称，根据名称选择需要运行的策略
    :return: 返回选股结果
    """
    # =====针对不同的策略进行选股
    # 优化方向1：用量价因子对小市值进行优化
    # if stg_name == '小市值_量价优化':
    #     # 计算总市值排名
    #     df['总市值排名'] = df.groupby('交易日期')['总市值'].rank(ascending=True, method='min')
    #     # 计算alpha95排名  衡量低波动
    #     df['alpha95排名'] = df.groupby('交易日期')['alpha95'].rank(ascending=True, method='min')
    #     # 计算Ret20排名  衡量超跌
    #     df['Ret_20排名'] = df.groupby('交易日期')['Ret_20'].rank(ascending=True, method='min')
    #     # 计算复合因子
    #     df['复合因子'] = df['alpha95排名'] + df['总市值排名'] + df['Ret_20排名']
    #
    # # 优化方向2：用基本面数据对小市值进行优化
    # if stg_name == '小市值_基本面优化':
    #     # 计算ROE百分比排名，去除ROE较差的20%的股票
    #     df['ROE排名'] = df.groupby('交易日期')['ROE'].rank(ascending=False, method='min', pct=True)
    #     df = df[df['ROE排名'] < 0.8]
    #     # 计算总市值排名
    #     df['总市值排名'] = df.groupby('交易日期')['总市值'].rank(ascending=True, method='min')
    #     # 计算归母净利润同比增速排名
    #     df['归母净利润同比增速排名'] = df.groupby('交易日期')['归母净利润同比增速_60'].rank(ascending=False, method='min')
    #     # 计算复合因子
    #     df['复合因子'] = df['总市值排名'] + df['归母净利润同比增速排名']
    #
    # # 优化方向3：对小市值进行过滤
    # if stg_name == '小市值_过滤优化':
    #     # 计算ROE百分比排名
    #     df['ROE排名'] = df.groupby('交易日期')['ROE'].rank(ascending=False, method='min', pct=True)
    #     # 计算归母净利润同比增速百分比排名
    #     df['归母净利润同比增速_60排名'] = df.groupby('交易日期')['归母净利润同比增速_60'].rank(ascending=False, method='min',
    #                                                                    pct=True)
    #     # 去除ROE较差的20%的股票
    #     df = df[df['ROE排名'] < 0.8]
    #     # 保留计算归母净利润同比增速排名靠前20%的股票
    #     df = df[df['归母净利润同比增速_60排名'] < 0.2]
    #     # 计算总市值排名
    #     df['总市值排名'] = df.groupby('交易日期')['总市值'].rank(ascending=True, method='min')
    #     # 计算复合因子
    #     df['复合因子'] = df['总市值排名']
    #
    # # 优化方向4：在限定股票池内选股
    # if stg_name == '小市值_限定股票池':
    #     # 指数名称，可以替换的
    #     index_name = '中证1000'  # 沪深300   中证1000
    #     # 只保留成分股数据
    #     df = df[df[f'{index_name}成分股'] == 'Y']
    #     # 计算总市值排名
    #     df['总市值排名'] = df.groupby('交易日期')['总市值'].rank(ascending=True, method='min')
    #     # 计算复合因子
    #     df['复合因子'] = df['总市值排名']

    # 优化方向5：PEG选股
    if stg_name == '小市值_PEG':
        # 计算市盈率的导数用于市盈率的排序，1/PE越大越好
        df['1/PE'] = 1 / df['PE']
        df['1/PE排名'] = df.groupby('交易日期')['1/PE'].rank(ascending=False, method='min', pct=True)
        df = df[df['1/PE排名'] < 0.5]
        # 计算ROE百分比排名，去除ROE较差的20%的股票
        df['ROE排名'] = df.groupby('交易日期')['ROE'].rank(ascending=False, method='min', pct=True)
        df = df[df['ROE排名'] < 0.5]

        # 选出PEG前30%名的股票
        df['1/PEG'] = 1 / df['PEG']
        df['1/PEG排名'] = df.groupby('交易日期')['1/PEG'].rank(ascending=False, method='min', pct=True)
        df = df[df['1/PEG排名'] < 0.5]
        #
        # # 计算alpha95排名  衡量低波动
        # df['alpha95排名'] = df.groupby('交易日期')['alpha95'].rank(ascending=True, method='min')
        # # 计算Ret20排名  衡量超跌
        # df['Ret_20排名'] = df.groupby('交易日期')['Ret_20'].rank(ascending=True, method='min')

        # 计算总市值排名
        df['总市值排名'] = df.groupby('交易日期')['总市值'].rank(ascending=True, method='min')
        # df = df[df['总市值排名'] < 0.3]

        # 计算归母净利润同比增速排名
        df['归母净利润同比增速排名'] = df.groupby('交易日期')['归母净利润同比增速_60'].rank(ascending=False, method='min')

        # 计算复合因子
        df['复合因子'] = df['归母净利润同比增速排名'] + df['总市值排名']
        # df['复合因子'] = df['总市值排名']

    # 对因子进行排名
    df['排名'] = df.groupby('交易日期')['复合因子'].rank()

    # 选取排名靠前的股票
    df = df[df['排名'] <= select_stock_num]

    return df
