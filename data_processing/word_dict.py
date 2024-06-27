import pickle

# 获取词汇表
def get_vocab(corpus1, corpus2):
    word_vocab = set()  # 初始化词汇表集合
    for corpus in [corpus1, corpus2]:
        for i in range(len(corpus)):
            # 更新词汇表集合，包含代码上下文和查询中的词汇
            word_vocab.update(corpus[i][1][0])
            word_vocab.update(corpus[i][1][1])
            word_vocab.update(corpus[i][2][0])
            word_vocab.update(corpus[i][3])
    print(len(word_vocab))  # 打印词汇表的大小
    return word_vocab  # 返回词汇表集合

# 加载pickle文件
def load_pickle(filename):
    with open(filename, 'rb') as f:  # 以二进制读取模式打开文件
        data = pickle.load(f)  # 反序列化文件内容
    return data  # 返回反序列化的数据

# 处理词汇表
def vocab_processing(filepath1, filepath2, save_path):
    with open(filepath1, 'r') as f:
        total_data1 = set(eval(f.read()))  # 读取并评估文件内容为集合
    with open(filepath2, 'r') as f:
        total_data2 = eval(f.read())  # 读取并评估文件内容

    word_set = get_vocab(total_data2, total_data2)  # 获取词汇表

    excluded_words = total_data1.intersection(word_set)  # 获取两个集合的交集
    word_set = word_set - excluded_words  # 从词汇表中去除交集部分

    print(len(total_data1))  # 打印初始数据1的大小
    print(len(word_set))  # 打印处理后的词汇表大小

    with open(save_path, 'w') as f:  # 以写入模式打开保存路径
        f.write(str(word_set))  # 将词汇表写入文件

if __name__ == "__main__":
    # 定义各种文件路径
    python_hnn = './data/python_hnn_data_teacher.txt'
    python_staqc = './data/staqc/python_staqc_data.txt'
    python_word_dict = './data/word_dict/python_word_vocab_dict.txt'

    sql_hnn = './data/sql_hnn_data_teacher.txt'
    sql_staqc = './data/staqc/sql_staqc_data.txt'
    sql_word_dict = './data/word_dict/sql_word_vocab_dict.txt'

    new_sql_staqc = './ulabel_data/staqc/sql_staqc_unlabled_data.txt'
    new_sql_large = './ulabel_data/large_corpus/multiple/sql_large_multiple_unlable.txt'
    large_word_dict_sql = './ulabel_data/sql_word_dict.txt'

    # 调用处理词汇表函数
    final_vocab_processing(sql_word_dict, new_sql_large, large_word_dict_sql)