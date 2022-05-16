# -*- coding: utf-8 -*-
import os

import yaml
import time
from multiprocessing import Process
from argparse import ArgumentParser
from control import Controller

from minisql.minisql import SQL
from Logger import Logger

from engine import MasterServer_start, ClientServer_start

from ZookeeperManager import ZookeeperManager


class RegionServer(object):
    def __init__(self, master_server_cfg, client_server_cfg, zookeeper_cfg, name):
        super(RegionServer, self).__init__()
        # 命令执行模块
        self.SQL = SQL()

        # 日志模块
        self.logger = Logger(self.SQL)
        self.logger.redo_log()

        # MasterServer Socket模块，启动直接运行
        self.MasterServer_process = Process(target=MasterServer_start, args=(
            master_server_cfg['IP'], master_server_cfg['Port'], self.SQL, self.logger
        ))
        self.MasterServer_process.start()

        # zookeeper模块，用于操作zookeeper
        self.zookeeper_manager = ZookeeperManager(
            ip=zookeeper_cfg['IP'],
            port=zookeeper_cfg['Port'],
            name=name
        )

        # RegionServer准备完毕，等待Client连接
        # ClientServer Socket模块， 启动直接运行
        self.ClientServer_process = Process(target=ClientServer_start, args=(
            client_server_cfg['IP'], client_server_cfg['Port'], self.SQL, self.logger
        ))
        self.ClientServer_process.start()

        time.sleep(2)

        # 启动日志回收
        self.logger.start_timer()
        print("日志回收程序启动！")

    def quit(self):
        # 关闭日志回收程序
        self.logger.close_timer()
        print("日志回收程序关闭")
        # 关闭MasterSocketServer
        self.MasterServer_process.terminate()
        print("MasterServerManager关闭成功")
        self.ClientServer_process.terminate()
        print("ClientServerManager关闭成功")
        self.zookeeper_manager.quit()
        print("Zookeeper连接关闭成功")


def main():
    argparser = ArgumentParser()
    argparser.add_argument('--id', type=int, help='RegionServer Id')
    args = argparser.parse_args()

    cfg = yaml.load(open('./config.yml', 'r'), Loader=yaml.FullLoader)
    region_server_name = 'RegionServer' + str(args.id)
    socket_cfg = cfg[region_server_name]
    master_server_cfg = socket_cfg['MasterSocketServer']
    client_server_cfg = socket_cfg['ClientSocketServer']
    zookeeper_cfg = cfg['zookeeper']

    region_server = RegionServer(
        master_server_cfg=master_server_cfg,
        client_server_cfg=client_server_cfg,
        zookeeper_cfg=zookeeper_cfg,
        name=region_server_name
    )

    Controller.prompt = '>>>'
    Controller(region_server).cmdloop()


if __name__ == '__main__':
    main()
