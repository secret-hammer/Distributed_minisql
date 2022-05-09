import os
import time
import threading

from minisql.minisql import SQL

log_root = './logfile'
log_file_name = 'sql.log'


class Logger(object):
    def __init__(self, sql: SQL):
        # 加载日志文件到内存
        if not os.path.exists(log_root):
            os.mkdir(log_root)
        if not os.path.exists(os.path.join(log_root, log_file_name)):
            f = open(os.path.join(log_root, log_file_name), 'w')
            # 日志第一行表示起始日志项序号
            f.write(str(0) + '\n')
            f.close()

        self.logs = list()
        self.sql = sql
        self.log_file_path = os.path.join(log_root, log_file_name)
        with open(self.log_file_path, 'r', newline='\n') as f:
            self.first_log_sequence_number = int(f.readline())
            for log_record in f.readlines():
                sequence_number, command = log_record.strip().split(':')
                sequence_number = int(sequence_number.strip())
                command = command.strip()
                if self.first_log_sequence_number < 1:
                    self.first_log_sequence_number = sequence_number
                self.logs.append(command)
        self.last_log_sequence_number = self.first_log_sequence_number + len(self.logs)

        print("Log file is loaded into memory, the first log sequence number is %d" % self.first_log_sequence_number)

    def redo_log(self, start_sequence_number=0):
        try:
            if len(self.logs) == 0:
                print("Logfile is empty, Redo complete!")
                return
            start_ind = start_sequence_number - self.first_log_sequence_number
            for i in range(start_ind, len(self.logs)):
                self.sql.execute(command=self.logs[i])
            print("Redo complete!")
        except Exception as e:
            print(e)

    def add_log(self, command: str):

        # 直接追加到文件
        self.last_log_sequence_number += 1
        log_record = str(self.last_log_sequence_number) + ':' + command
        with open(self.log_file_path, 'a') as f:
            f.write(log_record + '\n')

        # 写入内存
        self.logs.append(command)
        print(f"Successfully Add log record \'{log_record}\'!")

    # 这里日志回收期间RegionServer不会意外退出
    def auto_commit(self):
        time_start = time.perf_counter()

        self.sql.__finalize__()
        with open(os.path.join(log_root, log_file_name), 'w') as f:
            f.write(str(self.last_log_sequence_number) + '\n')
            f.close()
        self.first_log_sequence_number = self.last_log_sequence_number
        self.logs = list()

        time_end = time.perf_counter()
        print('Log file has been commited to local files,', end='')
        print(" time elapsed : %fs." % (time_end - time_start))

        # 重新启动日志回收
        interval_time = 300  # seconds
        timer = threading.Timer(interval_time, function=self.auto_commit)
        timer.start()

