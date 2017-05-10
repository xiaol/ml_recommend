# -*- coding: utf-8 -*-
# @Time    : 17/3/10 下午12:08
# @Author  : liulei
# @Brief   : 
# @File    : logger.py
# @Software: PyCharm Community Edition
import logging

class Logger:
    def __init__(self, logName = '', logFile='', level=logging.INFO):
        self._logger = logging.getLogger(logName)
        handler = logging.FileHandler(logFile)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self._logger.addHandler(handler)
        self._logger.setLevel(level)

    def info(self, msg):
        if self._logger is not None:
            self._logger.info(msg)
    def error(self, msg):
        if self._logger is not None:
            self._logger.error(msg)
    def exception(self, msg):
        if self._logger is not None:
            self._logger.exception(msg)


if __name__ == "__main__":
    log1 = Logger("log1", "/hefu/merge/t1.log")
    log1.log("hello")
    log2 = Logger("log2", "/hefu/merge/t2.log")
    log2.log("hdfaad,fdas")