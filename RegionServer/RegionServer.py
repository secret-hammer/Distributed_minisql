import os
import kazoo
import socketserver
import yaml
from argparse import ArgumentParser

from minisql.minisql import SQL
from Logger import Logger
from SocketManager.MasterSocketServer import MasterSocketHandler, MasterServer
from SocketManager.ClientSocketServer import ClientSocketHandler, ClientServer
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
        self.master_server = MasterServer(
            server_address=(master_server_cfg['IP'], master_server_cfg['Port']),
            request_handler=MasterSocketHandler,
            sql=self.SQL
        )
        self.master_server.serve_forever()

        # zookeeper模块，用于操作zookeeper
        self.zookeeper_manager = ZookeeperManager(
            ip=zookeeper_cfg['IP'],
            port=zookeeper_cfg['Port'],
            name=name
        )

        # RegionServer准备完毕，等待Client连接
        # ClientServer Socket模块， 启动直接运行
        self.client_server = ClientServer(
            server_address=(client_server_cfg['IP'], client_server_cfg['Port']),
            request_handler=ClientSocketHandler,
            sql=self.SQL
        )
        self.master_server.serve_forever()


def main():
    argparser = ArgumentParser()
    argparser.add_argument('--id', type=int, help='RegionServer Id')
    args = argparser.parse_args()

    cfg = yaml.load(open('./config.yml', 'r'), Loader=yaml.FullLoader)
    region_server_name = 'RegionServer' + str(args.id)
    socket_cfg = cfg[region_server_name]
    master_server_cfg = socket_cfg['MasterSocketServer']
    client_server_cfg = socket_cfg['ClientSocketServer']
    zookeeper_cfg = socket_cfg['zookeeper']

    region_server = RegionServer(
        master_server_cfg=master_server_cfg,
        client_server_cfg=client_server_cfg,
        zookeeper_cfg=zookeeper_cfg,
        name=region_server_name
    )


if __name__ == '__main__':
    main()
