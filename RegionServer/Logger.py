import os

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
            f.close()

        self.logs = list()
        self.first_log_sequence_number = 0
        self.sql = sql
        self.log_file_path = os.path.join(log_root, log_file_name)
        if os.path.getsize(self.log_file_path) > 0:
            with open(self.log_file_path, 'r') as f:
                for log_record in f.readlines():
                    sequence_number, command = log_record.strip().split(':')
                    sequence_number = int(sequence_number.strip())
                    command = command.strip()
                    if self.first_log_sequence_number < 1:
                        self.first_log_sequence_number = sequence_number
                    self.logs.append(command)
        self.first_log_sequence_number = max(1, self.first_log_sequence_number)
        print("Log file is loaded into memory, the first log sequence number is %d" % self.first_log_sequence_number)

    def redo_log(self, start_sequence_number=1):
        try:
            if len(self.logs) == 0:
                print("Logfile is empty, Redo complete!")
                return
            start_ind = start_sequence_number - self.first_log_sequence_number
            for i in range(start_ind, len(self.logs)):
                self.sql.excute(command=self.logs[i])
            print("Redo complete!")
        except Exception as e:
            print(e)

    def add_log(self, command: str):
        # 直接追加到文件
        log_sequence_number = self.first_log_sequence_number + len(self.logs)
        log_record = str(log_sequence_number) + ':' + command
        with open(self.log_file_path, 'a') as f:
            f.write(log_record + '\n')

        # 写入内存
        self.logs.append(command)
        print(f"Successfully Add log record \'{log_record}\'!")

    def auto_commit(self):
        pass




