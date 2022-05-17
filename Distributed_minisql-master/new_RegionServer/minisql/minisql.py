# author: Spring
from operator import imod
import os
import sys
import time

from minisql.APIManager import api
from minisql.CatalogManager import catalog
from minisql.IndexManager import index
from minisql.RecordManager import record
from minisql.utiles import dbinfo


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
        self.execute(command)

    def execute(self, command):
        command = command.replace(';', '').strip()
        command_ind = command.find(' ')
        command_name = command[:command_ind].strip()
        command = command[command_ind:].strip()
        # 执行具体的命令
        try:
            executor = getattr(api, command_name)
            if command_name == 'select':
                return executor(command)
            else:
                executor(command)
                return 0
        except Exception as e:
            return e


if __name__ == '__main__':
    sql = SQL()
    sql.execute("select * from student;")
