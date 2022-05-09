# author: Spring
import cmd
import os
import sys
import time

import APIManager.api
from CatalogManager import catalog
from IndexManager import index
from RecordManager import record
from utiles import dbinfo

class miniSQL(cmd.Cmd):
    intro = '''
MiniSQL database server, version 1.0.0-release, (x86_64-apple)
Copyright 2022 @ Spring from ZJU. Course final project for DDBS.
These shell commands are defined internally.  Type `help' to see this list.
Type `help name' to find out more about the function `name'.
    '''
    def do_select(self, args):
        try:
            APIManager.api.select(args.replace(';', ''))
        except Exception as e:
            print(e)

    def do_create(self, args):
        try:
            APIManager.api.create(args.replace(';', ''))
        except Exception as e:
            print(e)

    def do_drop(self, args):
        try:
            APIManager.api.drop(args.replace(';', ''))
        except Exception as e:
            print(e)

    def do_insert(self, args):
        try:
            APIManager.api.insert(args.replace(';', ''))
        except Exception as e:
            print(e)

    def do_delete(self, args):
        try:
            APIManager.api.delete(args.replace(';', ''))
        except Exception as e:
            print(e)

    def do_commit(self, args):
        time_start = time.perf_counter()
        __finalize__()
        time_end = time.perf_counter()
        print('Modifications has been commited to local files,', end='')
        print(" time elapsed : %fs." % (time_end - time_start))

    def do_quit(self, args):
        __finalize__()
        print("bye")
        sys.exit()

    def do_test(self, args):
        index.print_B_plus_tree(dbinfo.table_index[args])

    def do_truncate(self, args):
        try:
            APIManager.api.truncate(args.replace(';', ''))
        except Exception as e:
            print(e)

    def emptyline(self):
        pass

    def default(self, line: str):
        print("Unable to recognize the command %s" % line)
        print("Type `help' to see this list")

    def help_commit(self):
        text = "To reduce file transfer's time, this SQL server is designed to \n"+\
        "'lasy' write changes to local files, which means it will not store changes \n"+\
        "until you type 'quit' to normally exit the server. if this server exit \n"+\
        "unnormally, all changes will be lost. If you want to write changes to \n"+\
        "local files immediately, please use 'commit' command.\n"
        print(text)

    def help_quit(self):
        print('退出并保存修改')

    def help_select(self):
        print("********  查询记录（支持复合条件查询）  ********\n")
        print("SELECT 列名称 FROM 表名称 WHERE 列名称 =(>, <, >=, <=) 值 and(or) ...")

    def help_create(self):
        print("********  创建索引  ********\n")
        print("CREATE INDEX index_name ON table_name (column_name)")
        print()
        print("********   创建表   ********\n")
        print('''CREATE TABLE 表名称
(
    列名称1 数据类型(支持int, float, char),
    列名称2 数据类型,
    列名称3 数据类型,
    ....
    primary key(),
    unique()
)
        ''')

    def help_drop(self):
        print("********  删除索引  ********\n")
        print("DROP INDEX index_name ON table_name")
        print()
        print("********   删除表   ********\n")
        print("DROP TABLE table_name")

    def help_insert(self):
        print("********  插入记录  ********\n")
        print("INSERT INTO 表名称 VALUES (值1, 值2,....)")

    def help_delete(self):
        print("********  删除记录  ********\n")
        print("DELETE FROM 表名称 WHERE 列名称 =(>, <, >=, <=) 值 and(or) ...")

def __initialize__():
    pwd = os.getcwd()
    catalog.__initialize__(pwd)
    index.__initialize__()
    record.__initialize__()


def __finalize__():
    pwd = os.getcwd()
    catalog.__finalize__(pwd)
    index.__finalize__()
    record.__finalize__()


if __name__ == '__main__':
    try:
        __initialize__()
        miniSQL.prompt = '>>>'
        miniSQL().cmdloop()
    except Exception as e:
        print(e)





