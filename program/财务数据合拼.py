# 此脚本用于购买网站数据后合并数据

from Functions import *
from Config import *

import shutil
import platform
from datetime import datetime
from multiprocessing import Pool, freeze_support, cpu_count

current_path = os.path.dirname(__file__)
os.chdir(current_path)

pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 5000)  # 最多显示数据的行数


# 先读取用于更新的文件，然后找到对应的主csv文件，然后把新文件拼接到旧文件后边，然后保存
def update_stock_fin(code):
    new_path = root_path + '/data/fin_data/fin_xbx_new/%s/' % code
    old_path = root_path + '/data/fin_data/fin_xbx/%s/' % code
    df_new = pd.DataFrame()
    df_all = pd.DataFrame()
    # 因为有可能是新股，所以先判断一下是否存在old_path，如果是新股，直接复制到stock目录内就可以，否则要继续进行数据拼接
    if not os.path.exists(old_path):
        for dirpath, dirnames, filenames in os.walk(new_path):
            new_path = new_path + filenames[0]
            os.makedirs(old_path)
            shutil.copyfile(new_path, old_path + filenames[0])
    else:
        for dirpath, dirnames, filenames in os.walk(new_path):
            new_path = new_path + filenames[0]
            df_new = pd.read_csv(new_path, encoding='gbk', skiprows=1)

        for dirpath, dirnames, filenames in os.walk(old_path):
            old_path = old_path + filenames[0]
            df_all = pd.read_csv(old_path, encoding='gbk', skiprows=1)

        # 合并、去掉重复行
        df_all = pd.concat([df_all, df_new], axis=0, ignore_index=True)
        df_all.drop_duplicates(subset=['report_date', 'publish_date'], keep='last', inplace=True)
        df_all.sort_values(by=['report_date', 'publish_date'], axis=0, ascending=[True, True], inplace=True)
        # 数据头必须写回来~
        pd.DataFrame(columns=['数据由邢不行整理']).to_csv(old_path, index=False, encoding='gbk')
        # 覆盖保存
        df_all.to_csv(old_path, mode='a', encoding='gbk', index=False)


if __name__ == '__main__':
    # 读取所有股票代码的列表 最新的数据需要放到指定的目录
    stock_code_list = get_stock_code_list_in_one_dir(root_path + '/data/fin_data/fin_xbx_new')  # 获取目录内的所有股票的文件的路径
    stock_code_list = [code for code in stock_code_list]  # 排除北证版块的股票
    print('股票数量：', len(stock_code_list))

    # 测试用
    # update_stock('sh600000')
    # exit()

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
            df_list = pool.map(update_stock_fin, sorted(stock_code_list))
    else:
        exit()

    # 看一下花了多久
    print('完成任务，所需时间： ' + str(datetime.now() - start_time))
