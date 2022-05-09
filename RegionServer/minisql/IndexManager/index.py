import math
import json
import os
import copy

from utiles import dbinfo, error
from utiles.type import node
from CatalogManager import catalog
from RecordManager import record

N = 4


def __initialize__():
    if not os.path.exists(os.path.join(dbinfo.path, 'dbfiles/index_files')):
        os.mkdir(os.path.join(dbinfo.path, 'dbfiles/index_files'))
    if not os.path.exists(os.path.join(dbinfo.path, 'dbfiles/index_files/record_index.msql')):
        f = open(os.path.join(dbinfo.path, 'dbfiles/index_files/record_index.msql'), 'w')
        f.close()
    if not os.path.exists(os.path.join(dbinfo.path, 'dbfiles/index_files/other_index.msql')):
        f = open(os.path.join(dbinfo.path, 'dbfiles/index_files/other_index.msql'), 'w')
        f.close()
    __load__()


def __finalize__():
    __store__()


def recursive_store_node(__node):
    cur_node = {}
    cur_node['is_leaf'] = __node.is_leaf
    cur_node['keys'] = __node.keys
    if __node.is_leaf is True and __node.pointers[-1] != '':
        cur_node['pointers'] = __node.pointers[:-1]
    elif __node.is_leaf is True and __node.pointers[-1] == '':
        cur_node['pointers'] = __node.pointers
    else:
        cur_node['pointers'] = []
        for p in __node.pointers:
            cur_node['pointers'].append(recursive_store_node(p))
    return cur_node


def __tuple2str(t):
    s = ''
    for item in t[:-1]:
        s = s + str(item) + ' '
    s += str(t[-1])
    return s


def __str2tuple(s):
    return tuple(s.split(' '))


def __store__():
    tmp_table = {}
    for table in dbinfo.table_index.items():
        tmp_table[table[0]] = recursive_store_node(table[1])
    f = open(os.path.join(dbinfo.path, 'dbfiles/index_files/record_index.msql'), 'w')
    f.write(json.dumps(tmp_table))
    f.close()

    tmp_table = {}
    for __key, __root in dbinfo.indexs.items():
        tmp_table[__tuple2str(__key)] = recursive_store_node(__root)
    f = open(os.path.join(dbinfo.path, 'dbfiles/index_files/other_index.msql'), 'w')
    f.write(json.dumps(tmp_table))
    f.close()


def recursive_load_node(root, parent):
    cur_node = node(root['is_leaf'], root['keys'], [], parent)
    if cur_node.is_leaf is True:
        cur_node.pointers = root['pointers']
        if dbinfo.pre_leaf != '':
            dbinfo.pre_leaf.pointers.append(cur_node)
        dbinfo.pre_leaf = cur_node
    else:
        for p in root['pointers']:
            cur_node.pointers.append(recursive_load_node(p, cur_node))
    return cur_node


def __load__():
    if os.path.getsize(os.path.join(dbinfo.path, 'dbfiles/index_files/record_index.msql')) != 0:
        f = open(os.path.join(dbinfo.path, 'dbfiles/index_files/record_index.msql'), 'r')
        json_index_dict = f.read()
        index_dict = json.loads(json_index_dict)
        dbinfo.table_index = {}
        for table_name, root_dict in index_dict.items():
            dbinfo.table_index[table_name] = recursive_load_node(root_dict, '')
        f.close()
    dbinfo.pre_leaf = ''
    if os.path.getsize(os.path.join(dbinfo.path, 'dbfiles/index_files/other_index.msql')) != 0:
        f = open(os.path.join(dbinfo.path, 'dbfiles/index_files/other_index.msql'), 'r')
        json_index_dict = f.read()
        index_dict = json.loads(json_index_dict)
        dbinfo.indexs = {}
        for __key, root_dict in index_dict.items():
            dbinfo.indexs[__str2tuple(__key)] = recursive_load_node(root_dict, '')
        f.close()


def create_table(table_name):
    dbinfo.table_index[table_name] = node(True, [], [])


def create_index(table_name, index_name, column_name):
    dbinfo.indexs[(table_name, column_name, index_name)] = node(True, [], [])
    records = dbinfo.table_record[table_name]
    for ind, c in enumerate(dbinfo.tables[table_name].columns):
        if column_name == c.column_name:
            __index_key = ind
            break
    for r in records:
        __insert_into_table(table_name, copy.deepcopy(r), dbinfo.indexs[(table_name, column_name, index_name)],
                            __index_key)
        # 如果产生了新根
        if dbinfo.new_root != '':
            dbinfo.indexs[(table_name, column_name, index_name)] = dbinfo.new_root
            dbinfo.new_root = ''


def __insert_into_table(table_name, __values, root, __index_key):
    # 新树（第一次插入）
    if len(root.keys) == 0:
        root.keys.append(__values[__index_key])
        root.pointers.append(__values)
        root.pointers.append('')
        return

    cur_node = find_leaf_place(root, __values[__index_key])
    if len(cur_node.keys) < N - 1:
        insert_into_leaf(cur_node, __values[__index_key], __values)

    else:
        insert_into_leaf(cur_node, __values[__index_key], __values)
        new_node = node(True, [], [])
        tmp_keys = cur_node.keys
        tmp_pointers = cur_node.pointers
        cur_node.keys = []
        cur_node.pointers = []
        for i in range(math.ceil(N / 2)):
            cur_node.keys.append(tmp_keys.pop(0))
            cur_node.pointers.append(tmp_pointers.pop(0))
        for i in range(N - math.ceil(N / 2)):
            new_node.keys.append(tmp_keys.pop(0))
            new_node.pointers.append(tmp_pointers.pop(0))
        cur_node.pointers.append(new_node)
        new_node.pointers.append(tmp_pointers.pop(0))
        new_node.parent = cur_node.parent
        insert_into_parent(table_name, cur_node, new_node.keys[0], new_node)


def insert_into_table(table_name, __values):
    # 插入主键索引
    cur_node = dbinfo.table_index[table_name]
    __index_key = dbinfo.tables[table_name].primary_key
    __dc_values = copy.deepcopy(__values)
    __insert_into_table(table_name, __dc_values, cur_node, __index_key)
    # 如果产生了新根
    if dbinfo.new_root != '':
        dbinfo.table_index[table_name] = dbinfo.new_root
        dbinfo.new_root = ''

    # 插入其他索引
    for i, root in dbinfo.indexs.items():
        if i[0] == table_name:
            for ind, c in enumerate(dbinfo.tables[table_name].columns):
                if i[1] == c.column_name:
                    __index_key = ind
                    break
            __dc_values = copy.deepcopy(__values)
            __insert_into_table(table_name, __dc_values, root, __index_key)
            # 如果产生了新根
            if dbinfo.new_root != '':
                dbinfo.indexs[i] = dbinfo.new_root
                dbinfo.new_root = ''
    # print_B_plus_tree(dbinfo.table_index[table_name])


def maintain_B_plus_tree_after_delete(table_name, cur_node, root):
    global N
    if cur_node.parent == '' and len(cur_node.pointers) == 1:
        root = root.pointers[0]
    elif ((len(cur_node.pointers) < math.ceil(N / 2) and cur_node.is_leaf is False) or
          (len(cur_node.keys) < math.ceil((N - 1) / 2) and cur_node.is_leaf is True)) and \
            cur_node.parent != '':
        previous = False
        other_node = node(True, [], [])
        K = ''
        __index = 0
        for index, i in enumerate(cur_node.parent.pointers):
            if i == cur_node:
                if index == len(cur_node.parent.pointers) - 1:
                    other_node = cur_node.parent.pointers[-2]
                    previous = True
                    K = cur_node.parent.keys[index - 1]
                else:
                    K = cur_node.parent.keys[index]
                    other_node = cur_node.parent.pointers[index + 1]
                    __index = index + 1
                break
        if (other_node.is_leaf is True and len(other_node.keys) + len(cur_node.keys) < N) or \
                (other_node.is_leaf is False and len(other_node.pointers) + len(cur_node.pointers) <= N):
            # 合并兄弟节点
            if previous is True:
                if other_node.is_leaf is False:
                    other_node.pointers = other_node.pointers + cur_node.pointers
                    other_node.keys = other_node.keys + [K] + cur_node.keys
                    for n in cur_node.pointers:
                        n.parent = other_node
                else:
                    other_node.pointers = other_node.pointers[:-1] + cur_node.pointers
                    other_node.keys = other_node.keys + cur_node.keys
                cur_node.parent.pointers = cur_node.parent.pointers[:-1]
                cur_node.parent.keys = cur_node.parent.key[:-1]
                maintain_B_plus_tree_after_delete(table_name, cur_node.parent, root)
            else:
                if other_node.is_leaf is False:
                    cur_node.pointers = cur_node.pointers + other_node.pointers
                    cur_node.keys = cur_node.keys + [K] + other_node.keys
                    for n in other_node.pointers:
                        n.parent = cur_node
                else:
                    cur_node.pointers = cur_node.pointers[:-1] + other_node.pointers
                    cur_node.keys = cur_node.keys + other_node.keys
                cur_node.parent.pointers.pop(__index)
                cur_node.parent.keys.pop(__index - 1)
                maintain_B_plus_tree_after_delete(table_name, cur_node.parent, root)
        else:
            # 借用兄弟节点
            if previous is True:
                if other_node.is_leaf is True:
                    cur_node.keys.insert(0, other_node.keys.pop(-1))
                    cur_node.pointers.insert(0, other_node.keys.pop(-2))
                    cur_node.parent.keys[-1] = cur_node.keys[0]
                else:
                    __tmp = other_node.pointers.pop(-1)
                    __tmp.parent = cur_node
                    cur_node.pointers.insert(0, __tmp)
                    cur_node.keys.insert(0, cur_node.parent.keys[-1])
                    cur_node.parent.keys[-1] = other_node.keys.pop(-1)
            else:
                if other_node.is_leaf is True:
                    cur_node.keys.insert(-1, other_node.keys.pop(0))
                    cur_node.pointers.insert(-2, other_node.keys.pop(0))
                    cur_node.parent.keys[__index - 1] = other_node.keys[0]
                else:
                    __tmp = other_node.pointers.pop(-1)
                    __tmp.parent = cur_node
                    cur_node.pointers.insert(-1, __tmp)
                    cur_node.keys.insert(-1, cur_node.parent.keys[__index - 1])
                    cur_node.parent.keys[__index - 1] = other_node.keys.pop(0)


def delete_from_table(table_name, deleted_records):
    while True:
        root = dbinfo.table_index[table_name]
        cur_node = find_first_leaf_place(root)
        once = False
        while cur_node != '':
            for i, p in enumerate(cur_node.pointers[:-1]):
                if p in deleted_records:
                    cur_node.pointers.pop(i)
                    cur_node.keys.pop(i)
                    maintain_B_plus_tree_after_delete(table_name, cur_node, root)
                    once = True
                    break
            if once is True:
                break
            cur_node = cur_node.pointers[-1]
        if once is False:
            break

    for i, root in dbinfo.indexs.items():
        if i[0] == table_name:
            while True:
                cur_node = find_first_leaf_place(root)
                once = False
                while cur_node != '':
                    for i, p in enumerate(cur_node.pointers[:-1]):
                        if p in deleted_records:
                            cur_node.pointers.pop(i)
                            cur_node.keys.pop(i)
                            maintain_B_plus_tree_after_delete(table_name, cur_node, root)
                            once = True
                            break
                    if once is True:
                        break
                    cur_node = cur_node.pointers[-1]
                if once is False:
                    break


def select_from_table(table_name, __index_column_name, __conditions, __is_primary_key):
    rt = []
    root = ''
    if __is_primary_key:
        root = dbinfo.table_index[table_name]
    else:
        for i in dbinfo.indexs.keys():
            if i[0] == table_name and i[1] == __index_column_name:
                root = dbinfo.indexs[i]
    index_key = ''
    index_opt = ''
    for condition in __conditions:
        if condition[0] == __index_column_name:
            index_key = condition[2]
            index_opt = condition[1]
            break
    leaf_node = find_leaf_place(root, index_key)
    if index_opt == '<' or index_opt == '<=':
        cur_node = find_first_leaf_place(root)
        while cur_node != leaf_node.pointers[-1]:
            for r in cur_node.pointers[:-1]:
                if record.__check_condition_on_record(table_name, r, __conditions):
                    rt.append(r)
            cur_node = cur_node.pointers[-1]
    elif index_opt == '>' or index_opt == '>=':
        cur_node = leaf_node
        while cur_node != '':
            for r in cur_node.pointers[:-1]:
                if record.__check_condition_on_record(table_name, r, __conditions):
                    rt.append(r)
            cur_node = cur_node.pointers[-1]
    elif index_opt == '=':
        if index_key in leaf_node.keys:
            record_place = leaf_node.keys.index(index_key)
            rt.append(leaf_node.pointers[record_place])
    return rt


def find_first_leaf_place(root):
    while not root.is_leaf:
        root = root.pointers[0]
    return root


def find_leaf_place(root, key):
    cur_node = root
    while not cur_node.is_leaf:
        seed = False
        for index, node_key in enumerate(cur_node.keys):
            if key < node_key:
                seed = True
                cur_node = cur_node.pointers[index]
                break
        if not seed:
            cur_node = cur_node.pointers[-1]
    return cur_node


def split_parent(__node):
    if len(__node.pointers) <= N:
        raise Exception("should not split")
    new_node = node(False, [], [])
    tem_pointers = __node.pointers
    tem_keys = __node.keys
    __node.pointers = []
    __node.keys = []
    __node.pointers.append(tem_pointers.pop(0))
    for index in range(math.ceil(N / 2)):
        __node.pointers.append(tem_pointers.pop(0))
        __node.keys.append(tem_keys.pop(0))
    new_node.pointers.append(tem_pointers.pop(0))
    split_key = tem_keys.pop(0)
    new_node.pointers += tem_pointers
    new_node.keys += tem_keys
    return __node, new_node, split_key


def insert_into_parent(table_name, __node, __key, new_node):
    if __node.parent == '':
        cur_node = node(False, [], [])
        cur_node.keys.append(__key)
        cur_node.pointers.append(__node)
        cur_node.pointers.append(new_node)
        __node.parent = cur_node
        new_node.parent = cur_node
        # dbinfo.table_index[table_name] = cur_node
        dbinfo.new_root = cur_node
    else:
        parent_node = __node.parent
        for index, p in enumerate(parent_node.pointers):
            if p == __node:
                parent_node.keys.insert(index, new_node.keys[0])
                parent_node.pointers.insert(index + 1, new_node)
                break
        if len(parent_node.pointers) > N:
            cur_parent, new_parent, parent_split_key = split_parent(parent_node)
            insert_into_parent(table_name, cur_parent, parent_split_key, new_parent)


def insert_into_leaf(cur_node, key, __values):
    for index, node_key in enumerate(cur_node.keys):
        if node_key == key:
            raise error.Primary_key_exist()
        if key < node_key:
            cur_node.pointers.insert(index, __values)
            cur_node.keys.insert(index, key)
            return
    cur_node.pointers.insert(len(cur_node.keys), __values)
    cur_node.keys.insert(len(cur_node.keys), key)


def print_B_plus_tree(tree):
    tem = [tree]
    leafs = []
    while len(tem) > 0:
        num = len(tem)
        for i in range(num):
            __node = tem.pop(0)
            print(__node.keys, end=' ')
            if __node.is_leaf:
                for p in __node.pointers[:-1]:
                    leafs.append(p)
            else:
                for p in __node.pointers:
                    tem.append(p)
        print('\n')
    for leaf in leafs:
        print(leaf, end=' ')
    print('\n')


def check_index_not_exist(table_name, column_name):
    for i in dbinfo.indexs.keys():
        if i[0] == table_name and i[1] == column_name:
            raise error.Index_has_exist(table_name, column_name)

def check_index_exist(table_name, index_name):
    for i in dbinfo.indexs.keys():
        if i[0] == table_name and i[2] == index_name:
            return
    raise error.Index_not_exist(table_name, index_name)



def drop_table(table_name):
    del dbinfo.table_index[table_name]
    for k in dbinfo.indexs.keys:
        if k[0] == table_name:
            del dbinfo.indexs[k]


def drop_index(table_name, index_name):
    for k in dbinfo.indexs.keys():
        if k[0] == table_name and k[2] == index_name:
            del dbinfo.indexs[k]
            break


def truncate_table(table_name):
    dbinfo.table_index[table_name] = node(True, [], [])
    for k in dbinfo.indexs.keys():
        if k[0] == table_name:
            del dbinfo.indexs[k]


if __name__ == '__main__':
    dbinfo.table_index['student'] = node(True, [], [])
    insert_into_table('student', [2, 'we'])
    insert_into_table('student', [3, 'ke'])
    insert_into_table('student', [5, 'ww'])
    insert_into_table('student', [7, 'ww'])
    insert_into_table('student', [11, 'wl'])
    insert_into_table('student', [17, 'wl'])
    insert_into_table('student', [19, 'wl'])
    insert_into_table('student', [23, 'wl'])
    insert_into_table('student', [29, 'wl'])
    insert_into_table('student', [31, 'wl'])
    insert_into_table('student', [9, 'wl'])
    insert_into_table('student', [10, 'wl'])
    insert_into_table('student', [8, 'wl'])
