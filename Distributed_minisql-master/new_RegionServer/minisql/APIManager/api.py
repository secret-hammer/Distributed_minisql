import re
from minisql.utiles import error, dbinfo

from minisql.RecordManager import record
from minisql.CatalogManager import catalog
from minisql.IndexManager import index

__root = True


def select(args):
    try:
        args = re.sub('=', ' = ', args)
        args = re.sub('>', ' > ', args)
        args = re.sub('<', ' < ', args)
        args = re.sub(' +', ' ', args).strip()
        args = re.sub('> +=', '>=', args)
        args = re.sub('< +=', '<=', args)
        args = re.sub(r' +', ' ', args).strip().replace(';', '')
        arglist = args.split(' ')
        pos_from = arglist.index('from')
        cols = arglist[0:pos_from]
        table_name = arglist[pos_from + 1]
        catalog.check_table_exist(table_name)
        conditions = []
        if len(arglist) > pos_from + 2:
            if arglist[pos_from + 2] != 'where':
                raise error.Semantic_error()
            conditions = __access_condition_list(table_name, arglist[pos_from + 3:])

        rt = record.select_from_table(table_name, cols, conditions)
        return [table_name, cols, rt]
    except Exception as e:
        raise e


def create(args):
    try:
        if re.search('table', args) is not None:
            args = re.sub(' +', ' ', args).strip().replace(';', '')
            pos_left_bracket = args.find('(')
            pos_right_bracket = args.rfind(')')
            primary_key = re.findall("primary key *\( *(\w*) *\)", args)
            unique_key = re.findall("unique *\( *(\w*) *\)", args)
            if len(primary_key) > 1:
                raise error.Argument_num_error('primary key')
            table_name = (args[:pos_left_bracket]).split(' ')[1]
            catalog.check_table_not_exist(table_name)
            cols_list = args[pos_left_bracket + 1:pos_right_bracket].split(',')
            columns = {}
            for col in cols_list:
                if re.search("primary key|unique", col) is None:
                    col_info = col.strip(' ').split(' ')
                    columns[col_info[0]] = col_info[1]
            catalog.create_table(table_name, primary_key, unique_key, columns)
            index.create_table(table_name)
            record.create_table(table_name)
            print("Successfully create table " + table_name)
        elif re.search('index', args) is not None:
            args = re.sub(' +', ' ', args)
            args = re.sub('\(', r' (', args).strip().replace(';', '')
            arglist = args.split(' ')
            if arglist[0] != 'index' or arglist[2] != 'on':
                raise error.Command_error('create ' + args)
            index_name = arglist[1]
            table_name = arglist[3]
            catalog.check_table_exist(table_name)
            column_name = re.findall('\( *(\w+) *\)', args)[0]
            catalog.check_column_exist(table_name, column_name)
            catalog.check_column_unique(table_name, column_name)
            index.check_index_not_exist(table_name, column_name)
            index.create_index(table_name, index_name, column_name)
            print('Successfully create index %s on table %s!' % (index_name, table_name))
    except Exception as e:
        raise e


def insert(args):
    try:
        args = re.sub(' +', ' ', args).strip().replace(';', '')
        arglist = args.split(' ')
        if arglist[0] != 'into':
            raise Exception("API Module : Unrecoginze symbol for command 'insert',it should be 'into'.")
        if re.search('values', arglist[2]) is None:
            raise Exception("API Module : Unrecoginze symbol for command 'insert',it should be 'values'.")
        table_name = arglist[1]
        values = re.findall("values *\( *(.*) *\)", args)
        values = values[0].replace(' ', '').split(',')
        catalog.check_table_exist(table_name)

        for i, col in enumerate(dbinfo.tables[table_name].columns):
            if col.type == 'int':
                values[i] = int(values[i])
            elif col.type == 'float':
                values[i] = float(values[i])

        catalog.check_types_of_table(table_name, values)
        index.insert_into_table(table_name, values)
        record.insert_into_table(table_name, values)
        print('Successfully insert into table %s!' % table_name)
    except Exception as e:
        raise e


def delete(args):
    try:
        args = re.sub('=', ' = ', args)
        args = re.sub('>', ' > ', args)
        args = re.sub('<', ' < ', args)
        args = re.sub(' +', ' ', args).strip()
        args = re.sub('> +=', '>=', args)
        args = re.sub('< +=', '<=', args)
        arglist = args.split(' ')
        if arglist[0] != 'from' or arglist[2] != 'where':
            raise error.Command_error('delete ' + args)
        table_name = arglist[1]
        catalog.check_table_exist(table_name)
        conditions = __access_condition_list(table_name, arglist[3:])
        #print(conditions)
        count = record.delete_from_table(table_name, conditions)
        print("Successfully delete %d entry(s) from table '%s'," % (count, table_name))
    except Exception as e:
        raise e


def drop(args):
    try:
        args = re.sub(' +', ' ', args).strip().replace(';', '')
        arglist = args.split(' ')
        if arglist[0] == 'table':
            table_name = arglist[1]
            catalog.check_table_exist(table_name)
            record.drop_table(table_name)
            catalog.drop_table(table_name)
            index.drop_table(table_name)
            print("Successfully drop table '%s'." % table_name)
        elif arglist[0] == 'index':
            if arglist[2] != 'on':
                raise error.Command_error('drop ' + args)
            index_name = arglist[1]
            table_name = arglist[3]
            catalog.check_table_exist(table_name)
            index.check_index_exist(table_name, index_name)
            index.drop_index(table_name, index_name)
            print("Successfully drop index '%s' on table '%s'." % (index_name, table_name))
        else:
            raise error.Command_error('drop ' + args)
    except Exception as e:
        raise e


def truncate(args):
    try:
        table_name = args.strip()
        catalog.check_table_exist(table_name)
        record.truncate_table(table_name)
        index.truncate_table(table_name)
        print("Successfully truncate table '%s'." % table_name)
    except Exception as e:
        raise e

# 处理条件表达式到指定格式（检查条件正确性）
def __access_condition_list(table_name, conditions):
    condition_list = []
    condition_and = []
    condition = []
    pos = 1
    for item in conditions:
        if item == 'and':
            condition_and.append(condition)
            condition = []
            pos = 1
            continue
        if item == 'or':
            condition_and.append(condition)
            condition_list.append(condition_and)
            condition_and = []
            condition = []
            pos = 1
            continue
        if pos == 2 or pos == 1 and catalog.check_column_exist(table_name, item):
            condition.append(item)
        elif pos == 3:
            col_index = 0
            for i, c in enumerate(dbinfo.tables[table_name].columns):
                if c.column_name == condition[0]:
                    col_index = i
                    break
            condition.append(__str_to_realtype(item, dbinfo.tables[table_name].columns[col_index].type))
        pos += 1
    if len(condition) > 0:
        condition_and.append(condition)
    condition_list.append(condition_and)
    print(condition_list)
    return condition_list


def __str_to_realtype(item, __type):
    if __type == 'int':
        return int(item)
    elif __type == 'float':
        return float(item)
    elif __type == 'char':
        return item
