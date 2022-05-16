data = [['student'], ['id', 'name', 'age'], [[1, 'bob', 12], [2, 'alice', 13], [3, 'jim', 14]]]

col_string = ",".join(data[1])
value_string_list = list()
for v in data[2]:
    for ind, item in enumerate(v[:]):
        v[ind] = str(item)
    v_string = ",".join(v)
    value_string_list.append(v_string)
value_string_list.insert(0, col_string)
rt_message = ";".join(value_string_list)
print(rt_message)