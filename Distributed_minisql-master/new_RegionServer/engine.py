from SocketManager.MasterSocketServer import MasterServer, MasterSocketHandler
from SocketManager.ClientSocketServer import ClientSocketHandler, ClientServer


def MasterServer_start(ip, port, SQL, logger):
    master_server = MasterServer(
        server_address=(ip, port),
        request_handler=MasterSocketHandler,
        sql=SQL,
        logger=logger
    )
    master_server.serve_forever()


def ClientServer_start(ip, port, SQL, logger):
    client_server = ClientServer(
        server_address=(ip, port),
        request_handler=ClientSocketHandler,
        sql=SQL,
        logger=logger
    )
    client_server.serve_forever()
