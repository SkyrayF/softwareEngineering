import pickle
from collections import Counter


# 加载pickle文件的函数
def load_pickle(filename):
    with open(filename, 'rb') as f:  # 以二进制模式打开文件
        data = pickle.load(f, encoding='iso-8859-1')  # 使用特定编码读取数据
    return data  # 返回读取的数据


# 将数据根据qids分割成单条和多条
def split_data(total_data, qids):
    result = Counter(qids)  # 统计每个qid出现的次数
    total_data_single = []  # 存储单条数据
    total_data_multiple = []  # 存储多条数据
    for data in total_data:
        if result[data[0][0]] == 1:  # 如果该qid只出现一次
            total_data_single.append(data)
        else:  # 如果该qid出现多次
            total_data_multiple.append(data)
    return total_data_single, total_data_multiple  # 返回分割后的数据


# 处理staqc数据的函数
def data_staqc_processing(filepath, save_single_path, save_multiple_path):
    with open(filepath, 'r') as f:  # 打开文件
        total_data = eval(f.read())  # 读取并评估文件内容
    qids = [data[0][0] for data in total_data]  # 提取所有的qid
    total_data_single, total_data_multiple = split_data(total_data, qids)  # 分割数据

    with open(save_single_path, "w") as f:  # 保存单条数据
        f.write(str(total_data_single))
    with open(save_multiple_path, "w") as f:  # 保存多条数据
        f.write(str(total_data_multiple))


# 处理大规模数据的函数
def data_large_processing(filepath, save_single_path, save_multiple_path):
    total_data = load_pickle(filepath)  # 加载pickle文件数据
    qids = [data[0][0] for data in total_data]  # 提取所有的qid
    total_data_single, total_data_multiple = split_data(total_data, qids)  # 分割数据

    with open(save_single_path, 'wb') as f:  # 保存单条数据
        pickle.dump(total_data_single, f)
    with open(save_multiple_path, 'wb') as f:  # 保存多条数据
        pickle.dump(total_data_multiple, f)


# 将单条未标记数据转为标记数据的函数
def single_unlabeled_to_labeled(input_path, output_path):
    total_data = load_pickle(input_path)  # 加载pickle文件数据
    labels = [[data[0], 1] for data in total_data]  # 给每条数据添加标签
    total_data_sort = sorted(labels, key=lambda x: (x[0], x[1]))  # 对数据进行排序
    with open(output_path, "w") as f:  # 保存排序后的数据
        f.write(str(total_data_sort))


if __name__ == "__main__":
    # 定义Python和SQL的staqc数据集路径
    staqc_python_path = './ulabel_data/python_staqc_qid2index_blocks_unlabeled.txt'
    staqc_python_single_save = './ulabel_data/staqc/single/python_staqc_single.txt'
    staqc_python_multiple_save = './ulabel_data/staqc/multiple/python_staqc_multiple.txt'
    data_staqc_processing(staqc_python_path, staqc_python_single_save, staqc_python_multiple_save)

    staqc_sql_path = './ulabel_data/sql_staqc_qid2index_blocks_unlabeled.txt'
    staqc_sql_single_save = './ulabel_data/staqc/single/sql_staqc_single.txt'
    staqc_sql_multiple_save = './ulabel_data/staqc/multiple/sql_staqc_multiple.txt'
    data_staqc_processing(staqc_sql_path, staqc_sql_single_save, staqc_sql_multiple_save)

    # 定义大规模的Python和SQL数据集路径
    large_python_path = './ulabel_data/python_codedb_qid2index_blocks_unlabeled.pickle'
    large_python_single_save = './ulabel_data/large_corpus/single/python_large_single.pickle'
    large_python_multiple_save = './ulabel_data/large_corpus/multiple/python_large_multiple.pickle'
    data_large_processing(large_python_path, large_python_single_save, large_python_multiple_save)

    large_sql_path = './ulabel_data/sql_codedb_qid2index_blocks_unlabeled.pickle'
    large_sql_single_save = './ulabel_data/large_corpus/single/sql_large_single.pickle'
    large_sql_multiple_save = './ulabel_data/large_corpus/multiple/sql_large_multiple.pickle'
    data_large_processing(large_sql_path, large_sql_single_save, large_sql_multiple_save)

    # 将单条未标记数据转换为标记数据并保存
    large_sql_single_label_save = './ulabel_data/large_corpus/single/sql_large_single_label.txt'
    large_python_single_label_save = './ulabel_data/large_corpus/single/python_large_single_label.txt'
    single_unlabeled_to_labeled(large_sql_single_save, large_sql_single_label_save)
    single_unlabeled_to_labeled(large_python_single_save, large_python_single_label_save)
