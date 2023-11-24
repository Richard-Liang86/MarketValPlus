"""
《邢不行-2023新版|Python股票量化投资课程》
author: 邢不行
微信: xbx9585

数据整理需要计算的因子脚本，可以在这里修改/添加别的因子计算
"""


def cal_tech_factor(df, extra_agg_dict):
    """
    计算量价因子
    :param df:
    :param extra_agg_dict:
    :return:
    """
    # =保留总市值因子
    extra_agg_dict['总市值'] = 'last'

    # =20日涨跌幅
    df['Ret_20'] = df['收盘价_复权'].pct_change(20)
    extra_agg_dict['Ret_20'] = 'last'

    # =alpha95因子
    df['alpha95'] = df['成交额'].rolling(5).std()
    extra_agg_dict['alpha95'] = 'last'

    # =保留成分股信息
    extra_agg_dict['沪深300成分股'] = 'last'
    extra_agg_dict['中证1000成分股'] = 'last'

    # 可以在这里放自己的两家因子

    return df


def calc_fin_factor(df, extra_agg_dict):
    """
    计算财务因子
    :param df:              原始数据
    :param finance_df:      财务数据
    :param extra_agg_dict:  resample需要用到的
    :return:
    """

    # ====计算常规的财务指标
    # =归母净利润同比增速 相较于60个交易日前的变化
    df['R_np_atoopc@xbx_单季同比_60'] = df['R_np_atoopc@xbx_单季同比'].shift(60)
    df['归母净利润同比增速_60'] = df['R_np_atoopc@xbx_单季同比'] - df['R_np_atoopc@xbx_单季同比_60']
    extra_agg_dict['归母净利润同比增速_60'] = 'last'

    # ===计算单季度ROE
    df['ROE'] = df['R_np_atoopc@xbx_单季'] / df['B_total_equity_atoopc@xbx']
    extra_agg_dict['ROE'] = 'last'

    # ===计算市盈率
    df['PE'] = df['收盘价'] / df['R_basic_eps@xbx']
    extra_agg_dict['PE'] = 'last'
    df['PEG'] = df['PE'] / (df['ROE'] * 100)
    extra_agg_dict['PEG'] = 'last'

    return df
