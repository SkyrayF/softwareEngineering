import pickle
import multiprocessing
from python_structured import *
from sqlang_structured import *

# 解析Python查询的函数，接受数据列表作为参数，返回解析后的结果
def multipro_python_query(data_list):
    return [python_query_parse(line) for line in data_list]

# 解析Python代码的函数，接受数据列表作为参数，返回解析后的结果
def multipro_python_code(data_list):
    return [python_code_parse(line) for line in data_list]

# 解析Python上下文的函数，接受数据列表作为参数，根据特定条件进行解析
def multipro_python_context(data_list):
    result = []
    for line in data_list:
        if line == '-10000':
            result.append(['-10000'])
        else:
            result.append(python_context_parse(line))
    return result

# 解析SQL查询的函数，接受数据列表作为参数，返回解析后的结果
def multipro_sqlang_query(data_list):
    return [sqlang_query_parse(line) for line in data_list]

# 解析SQL代码的函数，接受数据列表作为参数，返回解析后的结果
def multipro_sqlang_code(data_list):
    return [sqlang_code_parse(line) for line in data_list]

# 解析SQL上下文的函数，接受数据列表作为参数，根据特定条件进行解析
def multipro_sqlang_context(data_list):
    result = []
    for line in data_list:
        if line == '-10000':
            result.append(['-10000'])
        else:
            result.append(sqlang_context_parse(line))
    return result

# 解析数据的通用函数
# 参数：数据列表、分割数量、上下文解析函数、查询解析函数、代码解析函数
def parse(data_list, split_num, context_func, query_func, code_func):
    pool = multiprocessing.Pool()  # 创建一个进程池
    split_list = [data_list[i:i + split_num] for i in range(0, len(data_list), split_num)]  # 将数据列表按指定数量分割
    results = pool.map(context_func, split_list)  # 使用进程池并行执行上下文解析函数
    context_data = [item for sublist in results for item in sublist]  # 合并结果
    print(f'context条数：{len(context_data)}')

    results = pool.map(query_func, split_list)  # 使用进程池并行执行查询解析函数
    query_data = [item for sublist in results for item in sublist]  # 合并结果
    print(f'query条数：{len(query_data)}')

    results = pool.map(code_func, split_list)  # 使用进程池并行执行代码解析函数
    code_data = [item for sublist in results for item in sublist]  # 合并结果
    print(f'code条数：{len(code_data)}')

    pool.close()  # 关闭进程池
    pool.join()  # 等待所有进程完成

    return context_data, query_data, code_data  # 返回解析后的上下文、查询和代码数据

# 主函数
# 参数：语言类型、分割数量、源路径、保存路径、上下文解析函数、查询解析函数、代码解析函数
def main(lang_type, split_num, source_path, save_path, context_func, query_func, code_func):
    with open(source_path, 'rb') as f:  # 打开源数据文件
        corpus_lis = pickle.load(f)  # 读取并反序列化数据

    # 解析数据
    context_data, query_data, code_data = parse(corpus_lis, split_num, context_func, query_func, code_func)
    qids = [item[0] for item in corpus_lis]  # 提取问题ID

    # 组合所有数据
    total_data = [[qids[i], context_data[i], code_data[i], query_data[i]] for i in range(len(qids))]

    with open(save_path, 'wb') as f:  # 打开保存路径文件
        pickle.dump(total_data, f)  # 序列化并保存数据

if __name__ == '__main__':
    # 定义不同语言和数据集的路径
    staqc_python_path = './ulabel_data/python_staqc_qid2index_blocks_unlabeled.txt'
    staqc_python_save = '../hnn_process/ulabel_data/staqc/python_staqc_unlabled_data.pkl'

    staqc_sql_path = './ulabel_data/sql_staqc_qid2index_blocks_unlabeled.txt'
    staqc_sql_save = './ulabel_data/staqc/sql_staqc_unlabled_data.pkl'

    # 处理Python和SQL的staqc数据集
    main(python_type, split_num, staqc_python_path, staqc_python_save, multipro_python_context, multipro_python_query, multipro_python_code)
    main(sqlang_type, split_num, staqc_sql_path, staqc_sql_save, multipro_sqlang_context, multipro_sqlang_query, multipro_sqlang_code)

    # 定义大规模数据集的路径
    large_python_path = './ulabel_data/large_corpus/multiple/python_large_multiple.pickle'
    large_python_save = '../hnn_process/ulabel_data/large_corpus/multiple/python_large_multiple_unlable.pkl'

    large_sql_path = './ulabel_data/large_corpus/multiple/sql_large_multiple.pickle'
    large_sql_save = './ulabel_data/large_corpus/multiple/sql_large_multiple_unlable.pkl'

    # 处理大规模的Python和SQL数据集
    main(python_type, split_num, large_python_path, large_python_save, multipro_python_context, multipro_python_query, multipro_python_code)
    main(sqlang_type, split_num, large_sql_path, large_sql_save, multipro_sqlang_context, multipro_sqlang_query, multipro_sqlang_code)
