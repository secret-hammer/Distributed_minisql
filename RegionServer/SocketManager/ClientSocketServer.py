from socketserver import TCPServer, BaseRequestHandler


# 自定义TCPServer方便参数传递
class ClientServer(TCPServer):
    def __init__(self, server_address, request_handler, sql):
        self.sql = sql  # sql命令执行器
        TCPServer.__init__(self, server_address, request_handler)


class ClientSocketHandler(BaseRequestHandler):
    # 使用self.server.sql调用命令执行器
    def handle(self):
        try:
            pass
        except Exception as e:
            print(self.client_address, "连接断开")
        finally:
            self.request.close()

    def setup(self):
        print("连接建立：", self.client_address)

    def finish(self):
        pass
