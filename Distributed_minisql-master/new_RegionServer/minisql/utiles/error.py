class Argument_num_error(Exception):
    def __init__(self, value_type='Somewhere'):
        err = 'The argument number of ' + value_type + ' is not correct'
        Exception.__init__(self, err)


class Semantic_error(Exception):
    def __init__(self):
        Exception.__init__(self, 'Semantic Error')


class Table_not_exist(Exception):
    def __init__(self, table_name):
        err = 'The table named ' + table_name + ' doesn\'t exist'
        Exception.__init__(self, err)


class Column_not_exist(Exception):
    def __init__(self, table_name, column_name):
        err = 'The table named ' + table_name + ' doesn\'t have column ' + column_name
        Exception.__init__(self, err)


class Table_exist(Exception):
    def __init__(self, table_name):
        err = 'The table named ' + table_name + ' has existed'
        Exception.__init__(self, err)


class Column_type_error(Exception):
    def __init__(self, value, __type):
        err = value + ' is not the value of the type of ' + '\'' + __type + '\''
        Exception.__init__(self, err)


class Primary_key_exist(Exception):
    def __init__(self):
        Exception.__init__(self, 'Primary_key already exists.')


class String_over_len(Exception):
    def __init__(self, s):
        Exception.__init__(self, 'The length of string \'' + s + '\' is out of limit')


class Command_error(Exception):
    def __init__(self, command):
        err = 'Unrecognized command ' + command + '!'
        Exception.__init__(self, err)


class Column_not_unique(Exception):
    def __init__(self, table_name, column_name):
        err = 'The table ' + table_name + ' \'s column ' + column_name + ' is not unique'
        Exception.__init__(self, err)


class Index_has_exist(Exception):
    def __init__(self, table_name, column_name):
        err = 'The table ' + table_name + ' has had index on ' + column_name
        Exception.__init__(self, err)


class Index_not_exist(Exception):
    def __init__(self, table_name, index_name):
        err = 'The table ' + table_name + ' doesn\'t have index named ' + index_name
        Exception.__init__(self, err)