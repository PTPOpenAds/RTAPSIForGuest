
<!-- Copyright 2020, Tencent Inc. -->
<!-- Author: joyesjiang@tencent.com -->


rta_psi_intersect_for_guest说明文档

1. 相关的模块依赖
rta_psi_intersect_for_guest/requirements.txt


2. 执行流程：

(1) 进入 rta_psi_intersect_for_guest 目录

(2) 执行命令：PYTHONPATH=. python3 ./psi_intersect/run_generate_guest_bitmap.py
    run_generate_guest_bitmap.py # 执行脚本入口文件
    （b）可选参数：
        （1）log_level: --log_level=INFO # 执行过程中的日志级别,可选INFO|ERROR，默认为INFO
        （2）error_rate: --error_rate=0.01 # 号码匹配能接受的错误率，默认0.01即99.9%的准确率

(3) 主要执行过程：
    (3.1) run_generate_guest_bitmap.run
        1. 初始化客户方Guest对象以及各自的数据源
            (1) guest侧数据源：psi_intersect/transfer_files/phone_num_guest.csv

        2. 当数据量比较大时按照记录条数对数据源进行拆分,拆分后的子文件在后续的操作中并行处理
            --- 可以指定每个分片的数据条数  psi_intersect_demo/common.py.FILE_SPLIT_LINE_NUM
            --- 这个参数影响最终的每片文件的记录行数，并影响最终每个并发进程的执行时间(1w条约7s)

    (3.2) run_rsa_intersect.generate_bool_filter_bits
        初始化加载Guest端的参数信息:
        (3.2.1) # Step1: [guest] - run for start and set hist_rsa_n
            1. 初始化Host和Guest端的RSA加密参数:Host-(n,e,d), Guest-(n,e,d)
            2. 此阶段需要Host和Guest进行数据交互:Guest从Host获取参数 Host.n
            --- Host侧Rsa加密大数N: psi_intersect_demo/transfer_files/stage0_host_big_num_N
            --- Host侧Rsa加密私钥D: psi_intersect_demo/transfer_files/stage0_host_big_num_N

        (3.2.2) # Step2: [host] - guest_gen_bloom_filter_param
            1. Guest侧初始化BloomFilter相关的参数:函数个数K,位图空间长度m
        
        (3.2.3) # Step3: [guest] - process guest_ids to bloomfilter_bits
            Guest侧对本地数据进行处理,生成最终的脱敏BloomFilter二部图特征数据:
            1. 对本地原始号码包数据进行一次Md5加密,生成H_g(x)
            2. 对加密数据应用K个散列函数集合Hash_set(K)进行散列并填充到BloomFilter二进制数组中.
            --- Guest侧的分片数据：psi_intersect_demo/transfer_files/guest_split_file
            --- Guest侧分片中每条数据的BloomFilter值：intersect_demo/transfer_files/guest_filter_value_file --- 当psi_intersect_demo/intersect.py.IS_DEBUG==True时写入
            --- Guest侧二进制数据文件-分片: psi_intersect_demo/transfer_files/guest_bits_file/
            --- Guest二进制数据文件-聚合: psi_intersect_demo/transfer_files/guest_all_bits

【相关说明】
(1) 根据Guest侧设置的精度需求(ERROR_RATIO = 0.001)，不同的精度会推导出不同的bloomFilter位图长度，对应不同的输出位图的结果文件
(2) 通过生成同格式的数据替换guest/host侧的数据文件(psi_intersect_demo/transfer_files/phone_num_guest.csv/psi_intersect_demo/transfer_files/phone_num_host.csv)，即可实现不同规模的数据场景的测试
