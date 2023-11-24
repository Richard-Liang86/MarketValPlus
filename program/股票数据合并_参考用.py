import os
from datetime import datetime
from multiprocessing import Pool, cpu_count
import pandas as pd

# 获取操作系统的目录分隔符备用
sep = os.sep
local_data_dir = r'你的本地存量数据路径，必须为csv文件的上一层，不然查不到已有数据'
increment_data_dir = r'下载的更新数据路径'
# 1（股票合并，以股票为维度，需要进行日期的合并）、2（日期数据迁移，如资金流数据，每个文件都是单独的某一天的数据，则直接复制写入本地即可）
mode = 1


def merge_two_data(fpath):
    fname = fpath.split(sep)[-1]
    stock_code = str(fname[:8])
    print(stock_code)

    current_data_path = local_data_dir + sep + fname
    increment_df = pd.read_csv(fpath, encoding='gbk', skiprows=1)

    if mode == 2:
        increment_df.to_csv(current_data_path, encoding='gbk', index=False)
        return

    # 本地存在存量数据，则进行读取并合并
    if os.path.exists(current_data_path):
        local_df = pd.read_csv(current_data_path, encoding='gbk', skiprows=1)

        # 在本地数据的基础上，追加新数据
        df = pd.concat([local_df, increment_df], axis=0, ignore_index=True)
        df.sort_values('交易日期', inplace=True)
        # 去除两个数据中重复的日期，优先保留后者，倾向于认为新数据更准确
        df.drop_duplicates(subset=['交易日期'], keep='last', inplace=True)
    else:
        # 本地未找到，说明可能是新股，直接复制
        df = increment_df
    # 数据头必须写回来~
    pd.DataFrame(columns=['数据由邢不行整理']).to_csv(current_data_path, index=False, encoding='gbk')
    df.to_csv(current_data_path, mode='a', encoding='gbk', index=False)


def merge():
    """
    合并本地的存量股票数据，与下载的更新股票数据
    """
    # 遍历所有增量股票数据
    file_name_list = []
    for root, dirs, files in os.walk(increment_data_dir):
        if files:  # 当files不为空的时候
            for f in files:
                if f.endswith('.csv'):
                    file_name_list.append(root + sep + f)

    start_time = datetime.now()
    print(start_time)

    multiply_process = True
    if multiply_process:
        # 开始并行
        with Pool(max(cpu_count() - 1, 1)) as pool:
            pool.map(merge_two_data, file_name_list)
    else:
        for file_name in file_name_list:
            merge_two_data(file_name)
    print('耗时:', datetime.now() - start_time)


if __name__ == '__main__':
    merge()