import os
import json
import prettytable as pt

from utiles import dbinfo
from IndexManager import index


def __initialize__():
    if not os.path.exists(os.path.join(dbinfo.path, 'dbfiles/record_files')):
        os.mkdir(os.path.join(dbinfo.path, 'dbfiles/record_files'))
    __load__()


def __finalize__():
    __store__()


def __load__():
    for table_name in dbinfo.tables.keys():
        with open(os.path.join(dbinfo.path, 'dbfiles/record_files/' + table_name + '.msql'), 'r') as f:
            dbinfo.table_record[table_name] = json.loads(f.read())


def __store__():
    for table_name, records in dbinfo.table_record.items():
        with open(os.path.join(dbinfo.path, 'dbfiles/record_files/' + table_name + '.msql'), 'w') as f:
            f.write(json.dumps(records))


def create_table(table_name):
    dbinfo.table_record[table_name] = []


def insert_into_table(table_name, __values):
    dbinfo.table_record[table_name].append(__values)


def __check_condition_on_record(table_name, __record, __conditions):
    all_column_names = []
    satisfied = True
    for c in dbinfo.tables[table_name].columns:
        all_column_names.append(c.column_name)
    for condition in __conditions:
        record_value = __record[all_column_names.index(condition[0])]
        condition_value = condition[2]
        opt = condition[1]
        if opt == '>=' and record_value >= condition_value or \
                opt == '<=' and record_value <= condition_value or \
                opt == '>' and record_value > condition_value or \
                opt == '<' and record_value < condition_value or \
                opt == '=' and record_value == condition_value:
            continue
        else:
            satisfied = False
            break
    return satisfied


def __select_from_table(table_name, __conditions):
    select_records = []
    for r in dbinfo.table_record[table_name]:
        if __check_condition_on_record(table_name, r, __conditions):
            select_records.append(r)
    return select_records


def print_select_records(table_name, __column_names, __records):
    tb = pt.PrettyTable()
    all_column_names = []
    if __column_names[0] == '*':
        for c in dbinfo.tables[table_name].columns:
            all_column_names.append(c.column_name)
    else:
        all_column_names = __column_names
    tb.field_names = all_column_names
    for r in __records:
        tb.add_row(r)
    print(tb)


def select_from_table(table_name, __column_names, __conditions, return_value=True):
    values = []
    select_column_index = []
    all_column_names = []
    for c in dbinfo.tables[table_name].columns:
        all_column_names.append(c.column_name)
    if __column_names[0] != '*':
        for select_column_name in __column_names:
            select_column_index.append(all_column_names.index(select_column_name))
    else:
        select_column_index = list(range(len(dbinfo.tables[table_name].columns)))
    print(select_column_index)

    if len(__conditions) == 0:
        rt = dbinfo.table_record[table_name]
        for i in range(len(rt)):
            r_tmp = rt[i]
            rt[i] = []
            for select_i in select_column_index:
                rt[i].append(r_tmp[select_i])
        print_select_records(table_name, __column_names, rt)
        return

    for condition_and in __conditions:
        has_index = False
        primary_key_name = dbinfo.tables[table_name].columns[dbinfo.tables[table_name].primary_key].column_name
        index_column_name = ''
        is_index_primary_key = False
        for condition in condition_and:
            if condition[0] == primary_key_name:
                has_index = True
                index_column_name = primary_key_name
                is_index_primary_key = True
                break
            for i in dbinfo.indexs.keys():
                if i[0] == table_name and i[1] == condition[0]:
                    has_index = True
                    index_column_name = condition[0]
                    break
        if has_index:
            value = index.select_from_table(table_name, index_column_name, condition_and, is_index_primary_key)
        else:
            value = __select_from_table(table_name, condition_and)
        # 选择对应项
        for i in range(len(value)):
            r_tmp = value[i]
            value[i] = []
            for select_i in select_column_index:
                value[i].append(r_tmp[select_i])
        values.append(value)
    # 所有values取并集
    rt = values[0]
    for vs in values[1:]:
        for v in vs:
            if v not in rt:
                rt.append(v)

    if return_value:
        return rt
    else:
        print_select_records(table_name, __column_names, rt)


def delete_from_table(table_name, conditions):
    # 在records里删除匹配项
    records = dbinfo.table_record[table_name]
    deleted_records = []
    for r in records[:]:
        for condition_and in conditions:
            if __check_condition_on_record(table_name, r, condition_and):
                deleted_records.append(r)
                records.remove(r)
    # 删除索引中的匹配项 (选择直接用records进行记录）
    if len(deleted_records) > 0:
        index.delete_from_table(table_name, deleted_records)

    return len(deleted_records)


def drop_table(table_name):
    del dbinfo.table_record[table_name]


def truncate_table(table_name):
    dbinfo.table_record[table_name] = []