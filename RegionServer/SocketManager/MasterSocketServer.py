import re
from socketserver import TCPServer, BaseRequestHandler


# 自定义TCPServer方便参数传递
class MasterServer(TCPServer):
    def __init__(self, server_address, request_handler, sql, logger):
        self.sql = sql  # sql命令执行器
        self.logger = logger # 日志控制
        TCPServer.__init__(self, server_address, request_handler)


class MasterSocketHandler(BaseRequestHandler):
    # 使用self.server.sql调用命令执行器
    # command1: 恢复策略
    # command2: 新增策略
    # command3: 写命令
    def handle(self):
        try:
            while True:
                data = self.request.recv(4096)
                datalist = re.findall("\[([^[]*)\]", data)
                assert datalist.pop(0) == 'master'

                ret_message = self.execute(datalist)
                self.request.send(ret_message)

        except Exception as e:
            print(e)
            print(f"MasterServer连接断开: {self.client_address}")
        finally:
            self.request.close()

    def setup(self):
        print(f"MasterServer连接成功: {self.client_address}")

    def finish(self):
        print(f"MasterServer连接断开: {self.client_address}")

    # 接受request，执行并构造返回消息
    def execute(self, req: list):
        command_number = int(req.pop(0))
        ret_message = f'[region][{command_number}]'
        logger = self.server.logger
        sql = self.server.sql
        # 询问是否可以执行恢复/新增策略
        if command_number == 0:
            ret_message = ret_message + f'[{logger.last_log_sequence_number}]'
        if command_number == 1:
            log_records = req[0].split(';')
            for record in log_records:
                split_ind = record.find(' ')
                sequence_number = int(record[:split_ind].strip())
                if sequence_number != logger.last_log_sequence_number + 1:
                    print("Restore: Wrong sequence number, not continuous")
                    break
                sql_record = record[split_ind:].strip()
                logger.add_log(sql_record)
                sql.excute(sql_record)
            ret_message = ret_message + f'[{logger.last_log_sequence_number}]'
        if command_number == 2:
            print("The Region Server is synchronous with cluster!")
        else:
            print("Unknown message")
        return ret_message


