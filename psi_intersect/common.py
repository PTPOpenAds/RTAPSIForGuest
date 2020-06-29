#!/bin/python3
import os
import sys

# 号码匹配预期的错误率阈值
ERROR_RATIO = 0.001

# 是否为调试模式：调试模式会打印大量的中间日志，生成每个step的中间结果数据,影响整体执行速度
IS_DEBUG = False

# 最大并发线程数
MAX_THREADS_NUM = 30

# 创建空文件,仅适用于目录参数,不适用于创建新文件
def create_path_not_exist(path):
    try:
        cmd_line = "mkdir -p %s;" %(path)
        os.popen(cmd_line)
    except Exception as e:
        print("Exception: [{0}:{1}] {2}".format(os.path.basename(__file__), 
                                                sys._getframe().f_lineno, 
                                                str(e)))
                                                
####### Part_2 执行流程相关的宏定义 ############

# 平台侧提供的加密用大数N
HOST_BIG_NUM_N_FILE_NAME = "stage0_host_big_num_N"

# 客户侧加密使用的私钥D
GUEST_PRIVATE_KEY_D_FILE_NAME = "stage0_guest_private_key_D"

# Guest的手机号原始数据文件目录
GUEST_PHONE_NUM_FILE_PATH = "./psi_intersect/transfer_files/phone_num_guest.csv"

# 当数据量太大时,会对原始数据进行分片
# 0. 每个分片文件的记录数
FILE_SPLIT_LINE_NUM = 3000000
# 1. 分片数据的保存目录
GUEST_SPLIT_FILE_PATH = "./psi_intersect/transfer_files/guest_split_file"
create_path_not_exist(GUEST_SPLIT_FILE_PATH)

# 2. 分片数据对应的散列函数处理后的中间数据，用于Debug模式下调试
GUEST_SPLIT_FILTER_VALUE_FILE_DIR = "./psi_intersect/transfer_files/guest_filter_value_file"
create_path_not_exist(GUEST_SPLIT_FILTER_VALUE_FILE_DIR)

# 3. 分片数据对应的位图数据
GUEST_SPLIT_BITS_FILE_DIR = "./psi_intersect/transfer_files/guest_bits_file"
create_path_not_exist(GUEST_SPLIT_BITS_FILE_DIR)

# Guest侧最终输出的位图结果文件(包含分片数据结果的聚合,可直接上传到平台侧)
GUEST_ALL_BITS_FILE_PATH = "./psi_intersect/transfer_files/guest_all_bits"
