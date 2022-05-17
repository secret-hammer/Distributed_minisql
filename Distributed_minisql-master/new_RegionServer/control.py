# -*- coding: utf-8 -*-
import cmd
import sys

class Controller(cmd.Cmd):
    intro = '''
cmd Controller启动，可以从控制台输入指令，控制RegionServer进程
    '''

    def __init__(self, region_server):
        super(Controller, self).__init__()
        self.region_server = region_server

    def do_quit(self, args):
        self.region_server.quit()
        print("Region_server成功关闭，程序退出")
        sys.exit()

    def emptyline(self):
        pass

