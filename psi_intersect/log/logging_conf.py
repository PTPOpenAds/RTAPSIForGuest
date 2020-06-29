#
#  Copyright 2020, Tencent Inc.
#  Author: joyesjiang@tencent.com
#

#!/bin/python3
#!coding:utf-8

import logging
from logging.handlers import RotatingFileHandler

## log settings
LOG_PATH_FILE = "./log/intersect_job.log"
LOG_MODE = 'a'
LOG_MAX_SIZE = 20*1024*1024 # 20M
LOG_MAX_FILES = 100 # 100 Files: print.log.1, print.log.2, print.log.3, print.log.4
LOG_LEVEL = logging.DEBUG
LOG_LEVEL = logging.INFO

LOG_FORMAT = "%(asctime)s %(levelname)-10s[%(filename)s:%(lineno)d(%(funcName)s)] %(message)s"

handler = RotatingFileHandler(LOG_PATH_FILE, LOG_MODE, LOG_MAX_SIZE, LOG_MAX_FILES)
formatter = logging.Formatter(LOG_FORMAT)
handler.setFormatter(formatter)

# handler.setLevel(LOG_LEVEL)
LOGGER = logging.getLogger('rta_psi_intersect')
LOGGER.setLevel(LOG_LEVEL)
LOGGER.addHandler(handler)
