from kazoo.client import KazooClient


class ZookeeperManager(object):
    def __init__(self, ip: str, port: int, name: str):
        self.host = ip + ':' + str(port)

        # 创建zookeeper客户端
        self.client = KazooClient(hosts=self.host)
        self.client.start()
        self.zookeeper_root_path = '/RegionServer'
        self.region_node_path = self.zookeeper_root_path + '/name'

        self.register()

    def register(self):
        self.client.ensure_path(self.zookeeper_root_path)

        # 注册Region节点,节点内容为host二进制字节流
        self.client.create(self.region_node_path, value=bytes(self.host), ephemeral=True, sequence=False)

