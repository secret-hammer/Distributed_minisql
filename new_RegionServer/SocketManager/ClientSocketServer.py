# -*- coding: utf-8 -*-
import re
from socketserver import TCPServer, BaseRequestHandler


# 自定义TCPServer方便参数传递
class ClientServer(TCPServer):
    def __init__(self, server_address, request_handler, sql, logger):
        self.sql = sql  # sql命令执行器
        self.logger = logger  # 日志控制
        TCPServer.__init__(self, server_address, request_handler)

        print("ClientServerManager启动成功！等待Client连接")


class ClientSocketHandler(BaseRequestHandler):
    # 使用self.server.sql调用命令执行器
    def handle(self):
        try:
            while True:
                data = self.request.recv(4096)
                datalist = re.findall("\[([^[]*)\]", data)
                assert datalist.pop(0) == 'client'

                ret_message = self.execute(datalist)
                self.request.send(ret_message)

        except Exception as e:
            print(e)
            print(f"Client连接断开: {self.client_address}")
        finally:
            self.request.close()

    def setup(self):
        print(f"Client连接成功: {self.client_address}")

    def finish(self):
        print(f"Client连接断开: {self.client_address}")

    def execute(self, req: list):
        command_number = int(req.pop(0))
        ret_message = f'[region][{command_number}]'
        sql = self.server.sql
        # 读指令
        if command_number == 0:
            command = req[0]
            data = sql.execute(command)
            # 成功执行[table_name, cols, values]
            if isinstance(data, list):
                col_string = ",".join(data[1])
                value_string_list = list()
                for v in data[2]:
                    for ind, item in enumerate(v[:]):
                        v[ind] = str(item)
                    v_string = ",".join(v)
                    value_string_list.append(v_string)
                value_string_list.insert(0, col_string)
                record_message = ";".join(value_string_list)
                ret_message = ret_message + '[0]' + f"[{record_message}]"
            elif isinstance(data, Exception):
                ret_message = ret_message + '[1]' + f"[{repr(data)}]"
        else:
            ret_message = ret_message + '[1]' + "[Unknown command]"
            print("ClientServer: Unknown message")
        return ret_message
