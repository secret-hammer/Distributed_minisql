# author: Spring
import os
import sys
import time

from APIManager import api
from CatalogManager import catalog
from IndexManager import index
from RecordManager import record
from utiles import dbinfo


class SQL(object):
    def __init__(self):
        super(SQL, self).__init__()
        self.__initialize__()

    def __initialize__(self):
        pwd = os.getcwd()
        catalog.__initialize__(pwd)
        index.__initialize__()
        record.__initialize__()

    def __finalize__(self):
        pwd = os.getcwd()
        catalog.__finalize__(pwd)
        index.__finalize__()
        record.__finalize__()

    def __call__(self, command):
        self.excute(command)

    def excute(self, command):
        command = command.replace(';', '').strip()
        command_ind = command.find(' ')
        command_name = command[:command_ind].strip()
        command = command[command_ind:].strip()
        # 执行具体的命令
        try:
            executor = getattr(api, command_name)
            executor(command)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    sql = SQL()
    sql.excute("select * from student;")
