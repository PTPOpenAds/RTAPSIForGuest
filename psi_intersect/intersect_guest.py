#
#  Copyright 2020, Tencent Inc.
#  Author: joyesjiang@tencent.com
#

import os
import math
import gmpy2
import random
import logging
import bitarray
from multiprocessing import *
from psi_intersect.common import *
from psi_intersect import RsaIntersect
from psi_intersect import IS_DEBUG
from psi_intersect.hasher import hash_set
from psi_intersect import FILE_SPLIT_LINE_NUM
from psi_intersect.log.logging_conf import LOGGER
from psi_intersect.base_file_opt import BaseFileOpreator


class RsaIntersectionGuest(RsaIntersect):
    def __init__(self):
        super().__init__()
        # RSA param
        self.d = None
        self.host_rsa_n = None
        
        # for debug -- desensitization bits
        self.guest_bits_a_detail = list() 

        # BloomFiter param
        self.hash_set_list = list()
        self.k:int = 11
        self.m:int = 8000000
        
        # Desensitization bits data
        self.guest_bits_a_data = list()
        
    # 初始化己方私钥self.d 和 Host侧大数self.host_rsa_n
    def init_rsa_param(self):
        # self.e, self.d, self.n = RsaIntersectionGuest.generate_rsa_param(rsa_bit = 1024)
        # print("get_guest_rsa_key, n: {0}, e: {1}, d: {2}".format(self.n, self.e, self.d))
        host_N = BaseFileOpreator.get_file_cont_to_str_list(HOST_BIG_NUM_N_FILE_NAME)
        guest_D = BaseFileOpreator.get_file_cont_to_str_list(GUEST_PRIVATE_KEY_D_FILE_NAME)
        if len(host_N) == 0 or len(guest_D) == 0:
            LOGGER.fatal("Failed init_rsa_param host_N or guest_D empty: %s, %s" %(len(host_N), len(guest_D)))
            exit(1)        
        else:
            self.host_rsa_n, self.d = int(host_N[0]), int(guest_D[0])
            LOGGER.info("init_rsa_param self.host_rsa_n: %s, self.d: %s" %(self.host_rsa_n, self.d))
        return True

    # Init Local BloomFilter param
    def gen_bloom_filter_param(self, data_len:int = 100000):
        # data_len = 100000000
        LOGGER.error("Start-Guest-gen_bloom_filter_param:")
        LOGGER.error("m = {0} \n n = {1} \n k = {2} \n e = {3}% \n " )
        instance_len = self.data_size
        if data_len < instance_len:
            data_len = instance_len
            LOGGER.warning("Adjust_bloom_filter_m_to_3_times_instance: {}".format(data_len))
            print("Adjust_bloom_filter_m_to_3*instance: {}".format(data_len))
        if data_len <= 0:
            LOGGER.error("gen_bloom_filter_param failed, data_instance is empty.")
            return False
        data_len = int(data_len)
        k = RsaIntersectionGuest.get_hash_num_k()
        m = math.ceil((float(k) * data_len) / gmpy2.log(2)) + 10000
        error = pow((1 - pow(math.e, -1 * k * data_len / m)), k)
        LOGGER.error("m = {0} \n n = {1} \n k = {2} \n e = {3}% \n " \
            "expect_precision = {4}% \n data_size = {5}M".format(int(m), 
                                                                 data_len, 
                                                                 k, 
                                                                 int(100000 * error) / 1000, 
                                                                 int(100000 * (1 - error)) / 1000, 
                                                                 int(100 * m/8000000) / 100))
        self.k, self.m = int(k), int(m)
        self.hash_set_list = hash_set.gen_hash_set(k)
        self.bit_array = bitarray.bitarray('0') * self.m
        LOGGER.info("hash_set_list deatil: {0}".format(self.hash_set_list))
        # open to print bitarray detail 0-1 bits
        # LOGGER.info("guest_init_param_detail: {0}".format(
        #     "\n".join(['%s:%s' % item for item in self.__dict__.items()])))
        return True

    # Process one guest_id to hash_bits
    def guest_id_process(self, sid):
        if self.host_rsa_n is None:
            LOGGER.error("guest_id_process failed, self.host_rsa_n is None.");
            return list()
        cipher_hash = RsaIntersect.powmod(int(RsaIntersectionGuest.hash(sid), 16), 
                                          self.d, 
                                          self.host_rsa_n)
        return list(map(lambda f: f.do_hash(cipher_hash) % self.m,  self.hash_set_list))

    # Process All guest_ids to BloomFilter bit_array
    # [return] bits_A: BitArray( Hash_set(H(u_i)^d_1) )
    def generate_bloom_bits_by_file_split_id(self, 
                                             split_id, 
                                             file_path, 
                                             result_bits_map, 
                                             split_bit_file_path):
        # For Debug use
        # self.guest_bits_a_detail = list()
        f_guest_filter_value = open(GUEST_SPLIT_FILTER_VALUE_FILE_DIR + "/" + split_id, 'wt')
        idx = 0
        tmp_bit_array = bitarray.bitarray('0') * self.m 
        LOGGER.error("Cur_idx: {0}, file_path: {1}".format(split_id, file_path))
        with open(file_path) as f:
            for line in f:
                key = line.strip()
                # LOGGER.info("idx: {0}, key: {1}".format(idx, key))
                sub_list = self.guest_id_process(key)
                if IS_DEBUG:
                    f_guest_filter_value.write(str((key, str(sub_list))) + "\n")
                # LOGGER.info("BloomFilter_map_detail: {0} --> {1}".format(key, sub_list))
                for sub in sub_list:
                    tmp_bit_array[sub] = '1'
                idx = idx + 1
                if idx % 10000 == 0:
                    LOGGER.error("BloomFilter_process_split_path: {0} - {1} / {2}"\
                        .format(file_path, idx, FILE_SPLIT_LINE_NUM))  
            result_bits_map[split_id] = tmp_bit_array
            with open(split_bit_file_path, 'wt') as fout_bits:
                if IS_DEBUG:
                    fout_bits.write(tmp_bit_array.to01())
                LOGGER.error("write_bits_to_file, split_id: {0}, "\
                    " data_file_path: {1}"\
                    " bit_result_path: {2}"\
                    " bit_count_1_num: {3}"\
                    " bloom_filter_m: {4}".format(split_id, 
                                                   file_path, 
                                                   split_bit_file_path, 
                                                   tmp_bit_array.count(), 
                                                   self.m) )
            LOGGER.error("generate_bloom_bits_by_file_split_id succ: {0}".format(split_id))
            print ("generate_bloom_bits_by_file_split_id succ: {0}".format(split_id))
            return True
        LOGGER.error("open file_path failed: {0}".format(file_path))
        return False

    def generate_guest_ids_bloom_filter_bits(self):
        # For Debug use
        # self.guest_bits_a_detail = list()
        # exit(0)
    
        LOGGER.info("Start generate_guest_ids_bloom_filter_bits")
        # if os.path.exists(GUEST_ALL_BITS_FILE_PATH):
        #     with open(GUEST_ALL_BITS_FILE_PATH) as fin_bits:
        #         self.bit_array = bitarray.bitarray(fin_bits.readline())
        #         LOGGER.info("Succ_got_bits_from_file, "\
        #             " bits_data_file_path: {0}"\
        #             " count_1_num: {1}".format(GUEST_ALL_BITS_FILE_PATH, self.bit_array.count()) )
        #         LOGGER.info("Succ_got_bits_from_file, "\
        #             " bits_data_file_path: {0}"\
        #             " count_1_num: {1}".format(GUEST_ALL_BITS_FILE_PATH, self.bit_array.count()))
        #     fin_bits.close()
        #     return True

        file_list = os.listdir(self.split_data_dir)
        if len(file_list) == 0:
            LOGGER.info("split_data_dir: {0} files_list empty".format(self.split_data_dir))
            file_list.append(self.data_file_path)

        LOGGER.info("generate_guest_ids_bloom_filter_bits, file_list: {}".format(str(file_list)))
        processed_id = 0
        result_bits_map = Manager().dict()
        total_split_file_size = len(file_list)
        print ("generate_bloom_bits_by_file_split_id total_job_size: {0}".format(total_split_file_size))
        while processed_id < total_split_file_size:
            p_map = {}
            rool = 0
            while rool < MAX_THREADS_NUM and processed_id < total_split_file_size:
                split_id = file_list[processed_id]
                file_path = self.split_data_dir + "/" + file_list[processed_id]
                split_bit_file_path = GUEST_SPLIT_BITS_FILE_DIR + "/" + file_list[processed_id]
                p_map[processed_id] = Process(target = self.generate_bloom_bits_by_file_split_id,
                                              args = [split_id,
                                                      file_path,
                                                      result_bits_map,
                                                      split_bit_file_path])
                LOGGER.info("MultiProcess, total_split_file_size: {0}"\
                            ", cur_split_file_idx: {1} cur_rool: {2}"\
                            ", cur_file_path: {3}".format(total_split_file_size, 
                                                          processed_id, 
                                                          rool, 
                                                          file_path))
                p_map[processed_id].start()
                processed_id += 1
                rool += 1
                LOGGER.info("running_rool_size: {0}".format(len(p_map)))
            for proc in p_map:
                p_map[proc].join()

        LOGGER.info("process_succ, result_bits_map size: {0}".format(len(result_bits_map)))
        print ("generate_bloom_bits_by_file_split_id process_succ: {0} / {1}".format(total_split_file_size, 
                                                                                     total_split_file_size))
        for processed_id in result_bits_map.keys():
            LOGGER.error("processed_id: {0}, bit_array_1_size: {1}".format(
                processed_id,
                result_bits_map[processed_id].count()))
            self.bit_array = self.bit_array | result_bits_map[processed_id]
        
        if self.bit_array.count() > 0:
            # with open(GUEST_ALL_BITS_FILE_PATH, 'wt') as fout_bits:
            #     # fout_bits.write(self.bit_array.to01())
            #     LOGGER.error("write_all_bits_to_file, "\
            #             " data_file_path: {0}"\
            #             " bit_result_path: {1}"\
            #             " bit_count_1_num: {2}".format(self.data_file_path, 
            #                                            GUEST_ALL_BITS_FILE_PATH, 
            #                                            self.bit_array.count()) )
            #     fout_bits.close()
            with open(GUEST_ALL_BITS_FILE_PATH, 'wb') as fout_bits:
                fout_bits.write(b"%d\n" %(self.m))
                fout_bits.write(self.bit_array.tobytes())
                LOGGER.error("write_all_bits_to_file, "\
                        " data_file_path: {0}"\
                        " bit_result_path: {1}"\
                        " bit_count_1_num: {2}".format(self.data_file_path, 
                                                       GUEST_ALL_BITS_FILE_PATH, 
                                                       self.bit_array.count()) )
                fout_bits.close()
        # exit(0)
        return True

    # 在线服务阶段:Guest侧对Host实时请求的信息进行处理并返回给Host(托管类客户不关心此过程)
    # [Input] host_processed_online_id_ZB: cipher-1024bit (Hash(u_i)^e_B % n)
    # [Input] host_rsa_n : rsa_param-1024bit, For version check
    # [output] guest_process_final_online_id_DB: cipher-1024bit (hash(Z_B)^d_A % n)
    def guest_process_online_id_to_DA(self, host_online_id_ZB, host_rsa_n):
        if host_rsa_n != self.host_rsa_n:
            LOGGER.error("Got wrong host_rsa_n: {0} != {1}".format(host_rsa_n, self.host_rsa_n))
            return False
        # LOGGER.info("host_online_id_ZB: {}".format(host_online_id_ZB))
        guest_process_final_online_id_DB = RsaIntersect.powmod(host_online_id_ZB, 
                                                               self.d, 
                                                               self.host_rsa_n)
        # LOGGER.info("ZB -> DB: {0} -> {1}".format(host_online_id_ZB, 
        #                                           guest_process_final_online_id_DB))
        return guest_process_final_online_id_DB

