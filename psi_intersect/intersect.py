#
#  Copyright 2020, Tencent Inc.
#  Author: joyesjiang@tencent.com
#

import os
import sys
import hashlib
import logging
import datetime
import numpy as np
import gmpy2, math
from Cryptodome import Random
from psi_intersect.common import *
from Cryptodome.PublicKey import RSA
from psi_intersect.log.logging_conf import LOGGER
POWMOD_GMP_SIZE = pow(2, 64)


class RsaIntersect(object):
    def __init__(self, random_bit = 128):
        self.random_bit = random_bit
        self.data_file_path = None
        self.data_size = None
        self.split_data_dir = None

    @staticmethod
    def hash(value):
        return hashlib.md5(bytes(str(value), encoding='utf-8')).hexdigest()
        # return hashlib.sha256(bytes(str(value), encoding='utf-8')).hexdigest()

    @staticmethod
    def get_hash_num_k():
        return math.ceil(abs(gmpy2.log2(ERROR_RATIO * 0.9)))

    @staticmethod
    def generate_rsa_param(rsa_bit = 1024):
        random_generator = Random.new().read
        rsa = RSA.generate(rsa_bit, random_generator)
        return rsa.e, rsa.d, rsa.n

    @staticmethod
    def powmod(a, b, c):
        """
        return int: (a ** b) % c
        """
        if a == 1:
            return 1
        if max(a, b, c) < POWMOD_GMP_SIZE:
            return pow(a, b, c)
        else:
            return int(gmpy2.powmod(a, b, c)) 

    def gen_data_instance_iterator(self):
        with open(self.data_file_path) as f:
            for line in f:
                yield line

    # Is data empty
    def is_data_empty(self):
        if self.data_size == 0:
            LOGGER.error("{0} is_data_empty.".format(type(self).__name__))
            return True
        else:
            return False

    def set_data_path(self, data_file_path):
        self.data_file_path = data_file_path

    def get_data_size(self, split_file_dir):
        try:
            cmd_line = "cat %s | wc -l;" %(self.data_file_path)
            result_pipe = os.popen(cmd_line)
            self.data_size = int(result_pipe.read())
        except Exception as e:
            err_str = "Exception: [{0}:{1}] {2}".format(os.path.basename(__file__), 
                                                        sys._getframe().f_lineno, 
                                                        str(e))
            print(err_str)
            LOGGER.error(err_str)
        self.split_data_dir = split_file_dir
        cmd_line = " rm %s/* > /dev/null 2>&1 " %(split_file_dir)
        result_pipe = os.system(cmd_line)
        try:
            cmd_line = "split -l %s -d %s %s/; " %(FILE_SPLIT_LINE_NUM, 
                                                   self.data_file_path, 
                                                   split_file_dir)
            result_pipe = os.popen(cmd_line)
            cmd_line = " ls %s/* | wc -l; " %(split_file_dir)
            result_pipe = int(os.popen(cmd_line).read())
            self.split_data_dir = split_file_dir
        except Exception as e:
            err_str = "Exception: [{0}:{1}] {2}".format(os.path.basename(__file__), 
                                                        sys._getframe().f_lineno, 
                                                        str(e))
            print(err_str)
            LOGGER.error(err_str)
        return self.data_size

