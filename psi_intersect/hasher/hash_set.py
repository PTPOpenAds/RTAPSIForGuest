#
#  Copyright 2020, Tencent Inc.
#  Author: joyesjiang@tencent.com
#

import sys
import copy
import mmh3
import gmpy2
import logging
import hashlib
import cityhash
import murmurhash
from psi_intersect.common import *
from psi_intersect.log.logging_conf import LOGGER


def RSHash(key):  
    seed_b = 378551
    seed_a = 63689
    hash = 0
    input = list(key)
    for idx in range(len(input)):  
        hash = hash * seed_a + ord(input[idx]) 
        seed_a = seed_a * seed_b;  
    return hash;  


def JSHash(key):  
    hash = 1315423911;
    input = list(key)  
    for idx in range(len(input)):
        hash = hash ^ ((hash << 5) + ord(input[idx]) + (hash >> 2))  
    return hash;  


def PJWHash(key):  
    input = list(key)
    bits_size = 32
    bits_size_3_over_4 = int(bits_size * 3) / 4
    one_eighth = int(bits_size / 8)
    high_bits = (0xFFFFFFFF) << (bits_size - one_eighth)
    hash = 0
    tmp = 0
    for idx in range(len(input)):
        hash = (hash << one_eighth) + ord(input[idx]) 
        tmp = hash & high_bits
        if tmp:
            hash = (( hash ^ (tmp >> bits_size_3_over_4)) & (~high_bits))
    return hash


def ELFHash(key):
    hash = 0
    tmp = 0
    input = list(key)
    for idx in range(len(input)):
        hash = (hash << 4) + ord(input[idx])
        tmp = hash & 0xF0000000
        if tmp:
            hash ^= (tmp >> 24)
        hash = hash & (~tmp)
    return hash


def SDBMHash(key):
    input = list(key)
    hash = 0
    for idx in range(len(input)):
        hash = ord(input[idx]) + (hash << 6) + (hash << 16) - hash
    return hash

class HashFunctor(object):
    def __init__(self, hash_type = murmurhash.hash, seed = 0):
        super().__init__()
        self.hash_type = hash_type
        self.seed = seed
    
    def set_seed(self, seed):
        self.seed = seed

    def do_hash(self, key):
        input = str(key)
        return self.hash_type(input, self.seed)


def gen_hash_set_v1(hash_size = 11):
    hash_set_result = []
    base_set = [HashFunctor(murmurhash.hash), HashFunctor(cityhash.CityHash128WithSeed)]
    if hash_size <= len(base_set):
        hash_set_result = base_set[:hash_size]
    elif hash_size > 20:
        LOGGER.error("gen_hash_set failed, hash_size out range: {} > 20".format(hash_size))
        return []
    else:
        seed = 1
        hash_set_result = base_set
        # print("hash_size - len(base_set): {}".format(hash_size - len(base_set)))
        for idx in range(int(hash_size - len(base_set))):
            hash_functor = base_set[idx % 2]
            hash_functor.set_seed(pow(2, idx))
            hash_set_result.append(hash_functor)

    return hash_set_result

class HashFunctor_V2(object):
    def __init__(self, seed = 0):
        super().__init__()
        self.hash_type_1 = murmurhash.hash
        self.hash_type_2 = cityhash.CityHash128WithSeed
        self.seed = seed
    
    def set_seed(self, seed):
        self.seed = seed

    def do_hash(self, key):
        input = str(key)
        return self.hash_type_1(input, self.seed) * self.hash_type_2(input, self.seed)

def gen_hash_set_v2(hash_size = 11):
    hash_set_result = []
    base_list = [HashFunctor_V2(100), HashFunctor_V2(200)]
    if hash_size <= len(base_list):
        hash_set_result = base_list[:hash_size]
    elif hash_size > 20:
        LOGGER.error("gen_hash_set failed, hash_size out range: {} > 20".format(hash_size))
        return []
    else:
        seed = 1
        hash_set_result = base_list
        remain_num = int(hash_size - len(base_list))
        LOGGER.error("hash_size - len(base_set): {}".format(remain_num))
        for idx in range(remain_num):
            hash_functor = copy.copy(base_list[idx % 2])
            hash_functor.set_seed(pow(2, idx))
            hash_set_result.append(hash_functor)

    LOGGER.error("{0}-hash_set_result: {1}".format(sys._getframe().f_back.f_code.co_name, 
                                                   hash_set_result))
    return hash_set_result

class HashFunctor_V3(object):
    def __init__(self, seed = 0):
        super().__init__()
        self.hash_type_1 = mmh3.hash
        self.hash_type_2 = hashlib.sha1
        self.seed = seed
    
    def set_seed(self, seed):
        self.seed = seed

    def do_hash(self, key):
        input = str(key)
        return self.hash_type_1(input, self.seed) \
            + (self.seed * int(self.hash_type_2(input.encode('utf-8')).hexdigest(), 16))

def gen_hash_set_v3(hash_size = 11):
    hash_set_result = []
    base_list = [HashFunctor_V3(100), HashFunctor_V3(200)]
    if hash_size <= len(base_list):
        hash_set_result = base_list[:hash_size]
    elif hash_size > 20:
        LOGGER.error("gen_hash_set failed, hash_size out range: {} > 20".format(hash_size))
        return []
    else:
        hash_set_result = base_list
        remain_num = int(hash_size - len(base_list))
        LOGGER.error("hash_size - len(base_set): {}".format(remain_num))
        for idx in range(remain_num):
            hash_functor = copy.copy(base_list[idx % 2])
            hash_functor.set_seed(pow(2, idx))
            hash_set_result.append(hash_functor)

    LOGGER.error("{0}-hash_set_result: {1}".format(sys._getframe().f_back.f_code.co_name, 
                                                   hash_set_result))
    return hash_set_result

def gen_hash_set(hash_size = 11):
    # hash_set_result = gen_hash_set_v1(hash_size)
    # hash_set_result = gen_hash_set_v2(hash_size)
    hash_set_result = gen_hash_set_v3(hash_size)
    return hash_set_result


if __name__ == "__main__":
    hash_set = gen_hash_set(6)
    print("hash_set: {0}, size: {1}".format(hash_set, len(hash_set)))
    key = "123456"
    if len(hash_set) == 0:
        print("Do gen_hash_set failed, got empty list.")
        exit(0) 

    (v1, v2, v3, v4) = (hash_set[3].do_hash(key), 
                        murmurhash.hash(key), 
                        cityhash.CityHash64WithSeed(key, 200),
                        murmurhash.hash(key, 999999))
    print("v1: {0}, v2: {1}, v3: {2}, v4: {3}".format(v1, v2, v3, v4))
    print(PJWHash("123456"))
    print(ELFHash("123456"))
    print("ln2: {}".format(gmpy2.log(2)))
