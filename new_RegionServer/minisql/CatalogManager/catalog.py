import os
import re
import json
from minisql.utiles import error, dbinfo
from minisql.utiles.type import table, column, node
from minisql.IndexManager import index


def check_table_exist(table_name):
    if table_name not in dbinfo.tables.keys():
        raise error.Table_not_exist(table_name)


def check_table_not_exist(table_name):
    if table_name in dbinfo.tables.keys():
        raise error.Table_exist(table_name)


def check_column_exist(table_name, column_name):
    for c in dbinfo.tables[table_name].columns:
        if c.column_name == column_name:
            return True
    raise error.Column_not_exist(table_name, column_name)

def check_column_unique(table_name, column_name):
    for c in dbinfo.tables[table_name].columns:
        if c.column_name == column_name and not c.is_unique:
            raise error.Column_not_unique(table_name, column_name)

def __initialize__(__path):
    print(__path)
    dbinfo.path = __path
    dbfilePath = os.path.join(dbinfo.path, 'dbfiles')
    if not os.path.exists(os.path.join(dbinfo.path, 'dbfiles/catalog_files')):
        os.makedirs(os.path.join(dbfilePath, 'catalog_files'))
    if not os.path.exists(os.path.join(dbfilePath, 'catalog_files/tables_catalog.msql')):
        f = open(os.path.join(dbfilePath, 'catalog_files/tables_catalog.msql'), 'w')
        f.close()
    if not os.path.exists(os.path.join(dbfilePath, 'catalog_files/index_catalog.msql')):
        f = open(os.path.join(dbfilePath, 'catalog_files/index_catalog.msql'), 'w')
        f.close()
    __load__()


def __finalize__(pwd):
    __store__()


def __store__():
    __tables = {}
    for table_name, table_instance in dbinfo.tables.items():
        table_dict = {'primary_key': table_instance.primary_key}
        __columns = {}
        for c in table_instance.columns:
            __columns[c.column_name] = c.__dict__
        table_dict['columns'] = __columns
        __tables[table_name] = table_dict
    json_tables = json.dumps(__tables)
    dbfilePath = os.path.join(dbinfo.path, 'dbfiles')
    f = open(os.path.join(dbfilePath, 'catalog_files/tables_catalog.msql'), 'w')
    f.write(json_tables)
    f.close()


def __load__():
    tableFilePath = os.path.join(dbinfo.path, 'dbfiles/catalog_files/tables_catalog.msql')
    indexFilePath = os.path.join(dbinfo.path, 'dbfiles/catalog_files/index_catalog.msql')
    if os.path.getsize(tableFilePath) != 0:
        f = open(tableFilePath, 'r')
        json_tables = json.loads(f.read())
        dbinfo.tables = {}
        for table_name, table_instance in json_tables.items():
            primary_key = table_instance['primary_key']
            columns = []
            for __column in table_instance['columns'].values():
                columns.append(column(__column['column_name'], __column['type'],
                                      bool(__column['is_unique']), int(__column['length'])))
            dbinfo.tables[table_name] = table(table_name, columns, primary_key)
            index.create_table(table_name)
        f.close()
    if os.path.getsize(indexFilePath) != 0:
        f = open(indexFilePath, 'r')
        json_indexs = json.loads(f.read())
        for i in json_indexs.items():
            dbinfo.indexs[i[0]] = i[1]
        f.close()


def create_table(table_name, primary_key, unique_key, column_dict):
    columns = []
    for column_name, typeis in column_dict.items():
        length = 0
        is_unique = False
        if re.search('char', typeis) is not None:
            length = int(re.findall('char\((\d+)\)', typeis)[0])
            typeis = 'char'
        if column_name in primary_key or column_name in unique_key:
            is_unique = True
        columns.append(column(column_name, typeis, is_unique, length))
    if len(primary_key) == 1:
        primary_key = primary_key[0]
        count = 0
        for c in columns:
            if c.column_name == primary_key:
                primary_key = count
                break
            count += 1
    else:
        primary_key = 0
    dbinfo.tables[table_name] = table(table_name, columns, primary_key)


def check_types_of_table(table_name, values):
    for i, value in enumerate(values):
        __type = dbinfo.tables[table_name].columns[i].type
        if isinstance(value, int) and __type != 'int' or \
                isinstance(value, float) and __type != 'float' or \
                isinstance(value, str) and __type != 'char':
            raise error.Column_type_error(value, __type)

        if type(value) == str and len(value) > dbinfo.tables[table_name].columns[i].length:
            raise error.String_over_len(value)


def drop_table(table_name):
    del dbinfo.tables[table_name]