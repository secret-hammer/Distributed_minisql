from SocketManager import SocketManager

import os
import psutil
import logging
from kazoo.client import KazooClient, KazooState


class MasterServer(object):
    def __init__(self, ip: str = "127.0.0.1", port: int = 2181):
        # zookeeper部分所需变量
        super(MasterServer, self).__init__()
        self.host = ip + ':' + str(port)
        self.zk = KazooClient(hosts=self.host)
        self.zk.start()
        self.zk.ensure_path('/MasterServer/Lock')
        self.pid = os.getpid()
        self.p = psutil.Process(self.pid)
        logging.basicConfig()  # 解决建立连接异常因没有传入log日志对象而产生的报错

        # 记录每一个socket的list
        self.masterserver = []
        # 记录每一个相连过的regionserver字典（包括已经断开的和没有断开的）
        self.RegionServer = {}
        # 每一个新开socket的端口号
        self.number = 0

        if not os.path.exists('./log.txt'):
            with open('./log.txt', 'w') as f:
                f.close()

        with open('./log.txt', 'r') as f:
            self.content = f.readlines()
            f.close()

        def state_listener(state):
            if state == KazooState.LOST:
                print("Zookeeper connection has been dropped!")
            elif state == KazooState.CONNECTED:
                print("Zookeeper connection has been established!")
            else:
                print("Zookeeper connection has been lost!")

        self.zk.add_listener(state_listener)

        print("Zookeeper Manager has successfully initialed")

    # 暂时不确定能不能正常运行
    def register(self):
        def register_watcher(event):
            current_master_server = self.zk.get_children('/MasterServer', watch=register_watcher)
            if len(current_master_server) == 0:
                self.zk.create('/MasterServer/master', sequence=False, ephemeral=True, value=bytes(self.host))
                print("Successfully register as current Master Server")
                self.p.resume()
            else:
                print("Other Server has registered, process is still blocked!")

        current_master_server = self.zk.get_children('/MasterServer')
        if len(current_master_server) == 1:
            self.zk.create('/MasterServer/master', sequence=False, ephemeral=True, value=self.host.encode())
            print("Successfully register as current Master Server")
        else:
            self.zk.get_children('/MasterServer', watch=register_watcher)
            print("Other Server has registered, process is blocked!")
            # block当前进程
            self.p.suspend()

    # 注册RegionServer节点下的监听器
    def register_region_watcher(self):
        @self.zk.ChildrenWatch('/RegionServer')
        def Region_watcher(children):
            host_list = list()
            # 有regionserver新增节点
            for name in children:
                host = self.zk.get(f"/RegionServer/{name}")
                host = host[0].decode('utf-8')
                master_region_host, client_region_host = host.split(';')
                host_list.append(host)

                master_server = SocketManager(self.number, master_region_host, self.content)
                self.number = self.number + 1

                if host not in list(self.RegionServer.keys()):
                    # 新增策略
                    master_server.NewAdd()
                # 1 表示之前连过后断开，0表示正常运行中
                elif host in list(self.RegionServer.keys()) and self.RegionServer[host] == 1:
                    # 恢复策略
                    master_server.Restore()

                self.RegionServer[host] = 0
                self.masterserver.append(master_server)
            # 有reginonserver断开
            for host, state in self.RegionServer.items():
                if state == 0 and host not in host_list:
                    # 代表有RegionServer断连了，就标记当前节点断连
                    self.RegionServer[host] = 1
                    print("Host: {host}, Region server has disconnected")

    def execute(self, message):
        for var in self.masterserver:
            state = var.Write(message)
            if state == 0:
                return 1
        return 0

    def register_lock_watcher(self):
        @self.zk.ChildrenWatch('/Master/Lock')
        def Lock_watcher(command_nodes):
            node_name = command_nodes[-1]
            # 初次读
            if "read" in node_name:
                host = self.find_best_region_server()
                self.zk.set(f'/Master/Lock/{node_name}', value=bytes(host))
                print("The host of region server is sent")
            # 写指令
            if "write" in node_name:
                command_bytes = self.zk.get(f'/Master/Lock/{node_name}')
                # 将字节码转为字符串，这里要和客户端的编码方式一致
                command = str(command_bytes)
                state = self.execute(command)
                # 成功执行
                if state == 0:
                    self.zk.set(f'/Master/Lock/{node_name}', value=bytes("Success"))
                    print(f"Command \"{command}\" is successfully executed!")
                else:
                    self.zk.set(f'/Master/Lock/{node_name}', value=bytes("Fail"))
                    print(f"Command \"{command}\" is failed!")


def main():
    master = MasterServer()
    master.register()
    master.register_region_watcher()
    master.register_region_watcher()

    print("MasterServer启动成功")
    while True:
        pass


if __name__ == '__main__':
    main()
