from parsons import Table
import json
import petl


tbl = Table([[1,2,3],['a','b','c']])
x = open(tbl.to_json()).read()
print (x)