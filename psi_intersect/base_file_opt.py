#
#  Copyright 2020, Tencent Inc.
#  Author: joyesjiang@tencent.com
#

import os
import time
import random
import datetime
import multiprocessing
from psi_intersect.log.logging_conf import LOGGER
BASE_PATH = "./psi_intersect/transfer_files/"

class BaseFileOpreator(object):
    @staticmethod
    def gen_full_path(file_name):
        fulle_name = BASE_PATH + file_name
        LOGGER.info("gen_full_path: {}".format(fulle_name))
        return fulle_name

    @staticmethod
    def get_file_cont_to_str_list(file_name, seperator=',', wait_loop = 1):
        full_path = BaseFileOpreator.gen_full_path(file_name)
        wait_sec = 0
        while wait_sec < wait_loop:
            if os.path.exists(full_path):
                f_in = open(full_path, 'r')
                line_list = f_in.readline().split(seperator)
                LOGGER.info("get_file_cont_to_str_list: {}".format(line_list))
                f_in.close()
                return line_list

            wait_sec = wait_sec + 1
            time.sleep(1)
        if wait_sec >= wait_loop:
            LOGGER.info("file_not_exist: {0}, wait_sec: {1}".format(full_path, wait_sec))
            return list()

    @staticmethod
    def write_list_to_file_one_line(file_name, data_list, seperator=',', wait_loop = 1):
        full_path = BaseFileOpreator.gen_full_path(file_name)
        data_list = map(lambda x: str(x), data_list)
        data_cont = seperator.join(data_list)
        LOGGER.info("full_path: {0}, data_cont: {1}".format(full_path, data_cont))
        with open(full_path, 'wt') as f:
            f.write(data_cont)
            f.close()

    @staticmethod
    def write_list_to_file_multi_line(file_name, data_list, seperator=',', wait_loop = 1):
        start = datetime.datetime.now()
        full_path = BaseFileOpreator.gen_full_path(file_name)
        print("write_list_to_file_multi_line: {0}, data_cont: {1}".format(full_path, len(data_list)))
        with open(full_path, 'wt') as f:
            for line in data_list:
                f.write(str(line)+"\n")
            f.close()
        end = datetime.datetime.now()
        print("write_list_to_file_multi_line, time_used: %.d sec" %((end - start).seconds))

    @staticmethod
    def write_list_to_file_multi_line_abs_path(file_name, data_list, seperator=',', wait_loop = 1):
        start = datetime.datetime.now()
        full_path = file_name
        print("write_list_to_file_multi_line: {0}, data_cont: {1}".format(full_path, len(data_list)))
        with open(full_path, 'wt') as f:
            for line in data_list:
                f.write(str(line)+"\n")
            f.close()
        end = datetime.datetime.now()
        print("write_list_to_file_multi_line, time_used: %.d sec" %((end - start).seconds))

    @staticmethod
    def generate_random_id_phone_age_data(file_name, data_num):
        start = datetime.datetime.now()
        full_path = BaseFileOpreator.gen_full_path(file_name)
        idx = 0
        # phone_num_set = set()
        with open(full_path, 'wt') as f_write:
            f_write.write("#phone_id\n")
            line_str = ""
            while idx < data_num:
                phone_id = "13" + str(random.randrange(5, 10)) + str(random.randrange(10000000, 
                                                                                      99999999))
                # if phone_id in phone_num_set:
                #     continue
                # phone_num_set.add(phone_id)
                f_write.write(phone_id + "\n")
                idx = idx + 1
            f_write.close()
        end = datetime.datetime.now()
        print("generate_random_data, file_path: {0}, data_num: {1}".format(full_path, idx))
        print("File_path: %s, generate_data_num: %s, time_used: %.d sec" %(full_path, 
                                                                           data_num, 
                                                                           (end - start).seconds))

    @staticmethod
    def load_file_to_string_set(file_path):
        if not os.path.exists(file_path):
            LOGGER.error("load_file_to_string_set failed: %s".format(file_path))
            exit(0)
        result_set = set()
        with open(file_path) as fin:
            for line in fin.readlines():
                result_set.add(line.strip())
        return result_set
