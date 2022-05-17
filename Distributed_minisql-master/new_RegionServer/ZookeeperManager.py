# -*- coding: utf-8 -*-
from kazoo.client import KazooClient


class ZookeeperManager(object):
    def __init__(self, ip: str, port: int, master_server_cfg: dict, client_server_cfg: dict, name: str):
        self.host = ip + ':' + str(port)
        self.master_host = master_server_cfg['IP'] + ':' + str(master_server_cfg["Port"])
        self.client_host = client_server_cfg['IP'] + ':' + str(client_server_cfg["Port"])

        # 创建zookeeper客户端
        self.client = KazooClient(hosts=self.host)
        self.client.start()
        self.zookeeper_root_path = '/RegionServer'
        self.region_node_path = self.zookeeper_root_path + f'/{name}'

        self.register()

    def register(self):
        self.client.ensure_path(self.zookeeper_root_path)

        node_value = (self.master_host + ';' + self.client_host).encode('utf-8')
        # 注册Region节点,节点内容为host二进制字节流
        self.client.create(self.region_node_path, value=node_value, ephemeral=True, sequence=False)
        print(f"RegionServer成功注册zk节点{self.region_node_path}！")

    def quit(self):
        self.client.stop()
        self.client.close()

