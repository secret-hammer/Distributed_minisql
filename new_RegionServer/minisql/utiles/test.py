import json
import sys
from utiles.type import column, table

if __name__ == '__main__':
    a = [1, 2 ,1, 3]
    for i, p in enumerate(a):
        if p == 2:
            a.pop(i)
    print(a)



