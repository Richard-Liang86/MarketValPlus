"""
《邢不行-2023新版|Python股票量化投资课程》
author: 邢不行
微信: xbx9585

配置参数
"""
import os

# ===操作 回测 或者 选股
operation = '回测'

# ===策略名
strategy_name = '小市值_PEG'  # 小市值_量价优化  小市值_基本面优化  小市值_过滤优化  小市值_限定股票池

# ===复权配置
fuquan_type = '后复权'

# ===选股参数设定
period_type = 'W'  # W代表周，M代表月
date_start = '2016-01-01'  # 需要从10年开始，因为使用到了ttm的同比差分，对比的是3年持续增长的数据
date_end = '2023-11-24' # 每次都要调整好日期----------记记记得修改！！！！！
select_stock_num = 6  # 选股数量
c_rate = 1.2 / 10000  # 手续费
t_rate = 1 / 1000  # 印花税

# ===获取项目根目录
_ = os.path.abspath(os.path.dirname(__file__))  # 返回当前文件路径
root_path = os.path.abspath(os.path.join(_, '..'))  # 返回根目录文件夹
path_for_selc = '/data/daily_data/stock_for_select_targets/%s.csv'
path_for_cal = '/data/daily_data/stock_for_select_targets/%s.csv'

# 导入财务数据路径
finance_data_path = root_path + '/data/fin_data/fin_xbx'
# finance_data_path = 'D:/Data/stock_data/stock-fin-data-xbx/'

# 因为财务数据众多，将本策略中需要用到的财务数据字段罗列如下
raw_fin_cols = [
    # 归母净利润   归属于母公司所有者权益合计
    'R_np_atoopc@xbx', 'B_total_equity_atoopc@xbx', 'R_basic_eps@xbx'
]

# 指定流量字段flow_fin_cols和截面字段cross_fin_cols。flow_fin_cols、cross_fin_cols必须是raw_fin_cols的子集
flow_fin_cols = [
    # 归母净利润
    'R_np_atoopc@xbx', 'R_basic_eps@xbx'
]

# raw_fin_cols财务数据中所需要计算截面数据的原生字段
cross_fin_cols = [
    # 归属于母公司所有者权益合计
    'B_total_equity_atoopc@xbx'
]

# 下面是处理财务数据之后需要的ttm，同比等一些字段
derived_fin_cols = [
    # 归母净利润_单季  归母净利润_单季同比
    'R_np_atoopc@xbx_单季', 'R_np_atoopc@xbx_单季同比'
]
