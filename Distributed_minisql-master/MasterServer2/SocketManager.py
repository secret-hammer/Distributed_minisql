import re
import socket
import threading
import time
import os


class SocketManager(object):
    def __init__(self, port_aug, region_server_address, log):
        self.region_ip, self.region_port = region_server_address.split(':')
        self.serversocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.serversocket.bind(('127.0.0.1', 25000 + port_aug))

        # 连接相应的region server
        self.serversocket.connect((self.region_ip, int(self.region_port)))
        self.content = log

    # 恢复策略
    def Restore(self):
        # Mastersocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.serversocket.connect(client_address)
        while True:
            # 第一步：询问Region Server是否可以restore
            message = '[master][0]'  # 'master' + str(0)
            self.serversocket.send(message.encode())
            # 第二步：等待Region Server的回应
            data = self.serversocket.recv(4096)
            data = data.decode()
            datalist = re.findall("\[([^[]*)\]", data)
            # 若是region 0 -1则不行
            if datalist[2] == "-1":
                time.sleep(3)
                continue
            # 可以则开始进行restore
            else:
                sequence_number = (int)(datalist[2])
                last_content = self.content[len(self.content)-1]
                last_content_number = (int)last_content[0]
                while (sequence_number < last_content_number):
                    send_content_length = last_content_number - sequence_number
                    send_log_message = '[master][1]'
                    for i in range(len(self.content)-send_content_length, len(self.content)):
                        send_log_message = send_log_message + self.content[i] + ';' + '\n'
                    self.serversocket.send(send_log_message)
                    data2 = self.serversocket.recv(4096)
                    datalist2 = re.findall("\[([^[]*)\]", data2)
                    sequence_number = (int)(datalist2[2])
                message = '[master][2]'
                self.serversocket.send(message)
                data3 = self.serversocket.recv(4096)
                datalist3 = re.findall("\[([^[]*)\]", data3)
            break

        # self.serversocket.close()

    # 新增策略
    def NewAdd(self):
        # clientaddress = client_host, client_port
        # Mastersocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # self.serversocket.connect(client_address)
        while True:
            # 第一步：询问Region Server是否可以新增
            message = '[master][0]'
            self.serversocket.send(message.encode())
            # 第二步：等待Region Server的回应
            data = self.serversocket.recv(4096).decode('utf-8')
            datalist = re.findall("\[([^[]*)\]", data)
            # 若是region 0 -1则不行
            if datalist[2] == "-1":
                time.sleep(3)
                continue
            # 可以则开始进行restore
            else:
                sequence_number = (int)(datalist[2])
                last_content = self.content[len(self.content) - 1]
                last_content_number = (int)
                last_content[0]
                while (sequence_number < last_content_number):
                    send_content_length = last_content_number - sequence_number
                    send_log_message = '[master][1]'
                    for i in range(len(self.content) - send_content_length, len(self.content)):
                        send_log_message = send_log_message + self.content[i] + ';' + '\n'
                    self.serversocket.send(send_log_message)
                    data2 = self.serversocket.recv(4096)
                    datalist2 = re.findall("\[([^[]*)\]", data2)
                    sequence_number = (int)(datalist2[2])
                message = '[master][2]'  # 'master' + str(2)
                self.serversocket.send(message)
                data3 = self.serversocket.recv(4096)
                datalist3 = re.findall("\[([^[]*)\]", data3)
            break
        # self.serversocket.close()

    # 对单个region server进行写指令
    def Write(self, message):
        flag = 0
        while True:
            # 第一步：向对应的region sever发送相应的信息
            Send_Message = '[master][3]' + f"[{message}]"  # 'master' + str(3) + message
            self.serversocket.send(Send_Message.encode())
            # 第二步：等待Region Server的回应
            data = self.serversocket.recv(4096)
            data = data.decode()
            datalist = re.findall("\[([^[]*)\]", data)
            # 若是region 3 0则说明成功
            if ((datalist[0] == "region") and (datalist[1] == "3") and (
                    datalist[2] == "0")):  # if datalist == 'region30':
                print("write success!\n")
                # 将该指令写入日志文件
                f = open("log.txt", "a")
                message = message + '\n'
                f.write(message)
                f.close()
                flag = 1
                break
            # 若未成功则循环执行
            else:
                flag = 0
                break
        return flag
        # self.serversocket.close()

# 对全部的连接的region server都需要发送相关的指令，当所有的region server都全部成功收到后，返回0


# host= "127.0.0.1"
# port= 1234
# s=socket.socket()
#
