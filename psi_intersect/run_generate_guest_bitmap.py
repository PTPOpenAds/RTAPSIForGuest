#
#  Copyright 2020, Tencent Inc.
#  Author: joyesjiang@tencent.com
#

#!/usr/bin/python3

import os
import sys
import json
import time
import gmpy2
import hashlib
import argparse
import datetime
import numpy as np
from psi_intersect.common import *
from psi_intersect.base_file_opt import BaseFileOpreator
from psi_intersect.intersect_guest import RsaIntersectionGuest
from psi_intersect.log.logging_conf import LOG_PATH_FILE, LOGGER
 
class RsaIntersectScheduler(object):
    def __init__(self):
        self.total_data_num:int = 1000000

    def check_ret_status_OK_or_Exit(self, is_succ):
        if not is_succ:
            LOGGER.error("check_ret_status is_succ == False, "\
                "caller_func: {0}, caller_line: {1}".format(sys._getframe().f_back.f_code.co_name, 
                                                            sys._getframe().f_back.f_lineno))
            print("check_ret_status is_succ == False, "\
                "caller_func: {0}, caller_line: {1} \n"\
                "program shutdown!!! ".format(sys._getframe().f_back.f_code.co_name, 
                                              sys._getframe().f_back.f_lineno))
            exit(1)

    def generate_bool_filter_bits(self, rsa_intersect_guest:RsaIntersectionGuest):
        if rsa_intersect_guest.is_data_empty():
            LOGGER.error("guest data empty! ")
            return list()

        is_succ = True
        LOGGER.error("Start generate_bool_filter_bits")
        print("Start generate_bool_filter_bits, Log_file_path: {0}".format(LOG_PATH_FILE))
        start_all = datetime.datetime.now()

        # Step1: [guest] - run for start and set hist_rsa_n
        # 1. Guest侧初始化己方私钥self.d和Host侧大数self.host_rsa_n
        start = datetime.datetime.now()
        LOGGER.error("Step1/3: Start Guest-run_for_start.")
        is_succ = rsa_intersect_guest.init_rsa_param()
        time_used = (datetime.datetime.now() - start).seconds
        LOGGER.error("Step1/3: Guest-run_for_start, time_used: %.d seconds" %(time_used)) # seconds
        print("Step1/3: Guest-run_for_start, time_used: %.d seconds" %(time_used))
        self.check_ret_status_OK_or_Exit(is_succ)

        # Step2: [host] - guest_gen_bloom_filter_param
        # 1. Guest侧初始化BloomFilter相关的参数:函数个数K,位图空间长度m
        start = datetime.datetime.now()
        LOGGER.error("Step2/3: Start guest-BloomFilter param init.")
        is_succ = rsa_intersect_guest.gen_bloom_filter_param(data_len = self.total_data_num)
        time_used = (datetime.datetime.now() - start).seconds
        LOGGER.error("Step2/3: guest-BloomFilter param init, time_used: %.d seconds" %(time_used)) # seconds
        print("Step2/3: guest-BloomFilter param init, time_used: %.d seconds" %(time_used))
        self.check_ret_status_OK_or_Exit(is_succ)

        # Step3: [guest] - process guest_ids to bloomfilter_bits
        # Guest侧对本地数据进行处理,生成最终的脱敏BloomFilter二部图特征数据:
        # 1. 对本地原始号码包数据进行一次Hash加密,生成H_g(x)
        # 2. 对加密数据应用K个散列函数集合Hash_set(K)进行散列并填充到BloomFilter二进制数组中.
        start = datetime.datetime.now()
        LOGGER.error("Step3/3: Start Guest-process guest ids with hash+BloomFilter, gen bits_A.")
        is_succ = rsa_intersect_guest.generate_guest_ids_bloom_filter_bits()
        self.check_ret_status_OK_or_Exit(is_succ)
        time_used = (datetime.datetime.now() - start).seconds
        LOGGER.error("Step3/3: Guest-process generate bits_A, time_used: %.d seconds" %(time_used)) # seconds
        print("Step3/3: Guest-process generate bits_A, time_used: %.d seconds" %(time_used))

        # Step4 [GuestTimeCostAnaly] do time-cost analysis
        # 分析整体的执行耗时情况.
        #The End time
        time_used_all = (datetime.datetime.now() - start_all).seconds
        LOGGER.error("The function total run time is : %.d seconds" %(time_used_all)) # seconds
        print("The function total run time is : %.d seconds" %(time_used_all)) # seconds
        return True        

    def run(self):
        # 初始化Guest的数据Instance
        rsa_intersect_guest = RsaIntersectionGuest()
        if len(GUEST_PHONE_NUM_FILE_PATH) == 0:
            LOGGER.error("Got_Error: GUEST_PHONE_NUM_FILE_PATH is empty")
            exit(1)
        rsa_intersect_guest.set_data_path(GUEST_PHONE_NUM_FILE_PATH)
        guest_data_len = rsa_intersect_guest.get_data_size(GUEST_SPLIT_FILE_PATH)
        # exit(0)

        parser = argparse.ArgumentParser()
        parser.add_argument('-v',
                            '--log_level',
                            type=str,
                            required=False,
                            help="Specify the log_level(INFO Default): [INFO|ERROR]!)")
        parser.add_argument('-e',
                            '--error_rate',
                            type=float,
                            required=False,
                            help="Specify the error_rate threshold(default: 0.009): 0.00001~0.2)")
        args = parser.parse_args()
        if args.log_level is not None and len(args.log_level) > 0:
            LOGGER.setLevel(args.log_level)
            LOGGER.error("ReSet LOGGER.setLevel: {0}".format(args.log_level))
            print("ReSet LOGGER.setLevel: {0}".format(args.log_level))
        if args.error_rate is not None and args.error_rate > 0 and args.error_rate < 0.2:
            ERROR_RATIO = args.error_rate
            LOGGER.error("ReSet LOGGER.error_rate: {0}".format(args.error_rate))
            print("ReSet LOGGER.error_rate: {0}".format(args.error_rate))

        if guest_data_len is None or guest_data_len <= 0:
            LOGGER.error("guest_data_len out of rnage: {0}".format(guest_data_len))
            exit(1)
        LOGGER.info("guest_data_len: {0}".format(guest_data_len))
        self.total_data_num = guest_data_len * 1.1 # 预留10%的值空间

        # Guest侧生成Bits位图文件
        is_succ = self.generate_bool_filter_bits(rsa_intersect_guest)


if __name__ == "__main__":
    LOGGER.info("\n" * 10 + "-" * 100)
    LOGGER.info("[StartRun]: {0}, cur_time: {1}".format(os.path.basename(__file__),
                                                        time.strftime("%Y-%m-%d %H:%M:%S")))
    rsa_scheduler = RsaIntersectScheduler().run()




