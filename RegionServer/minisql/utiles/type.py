class table(object):
    def __init__(self, table_name, columns, primary_key=0):
        self.table_name = table_name
        self.primary_key = primary_key
        self.columns = columns


class column(object):
    def __init__(self, column_name, type, unique=False, length=16):
        self.column_name = column_name
        self.type = type
        self.is_unique = unique
        self.length = length

    def __iter__(self):
        return next(self)

    def __next__(self):
        yield 'column_name', self.column_name
        yield 'type', self.type
        yield 'is_unique', self.is_unique
        yield 'length', self.length


# class index(object):
#     def __init__(self, index_name, root):
#         self.index_name = index_name
#         self.root = root


class node():
    def __init__(self, is_leaf, keys, pointers, parent=''):
        self.is_leaf = is_leaf
        self.keys = keys
        self.pointers = pointers
        self.parent = parent
