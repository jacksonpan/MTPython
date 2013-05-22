from database import *
from datetime import *
import types
from copy import deepcopy

config = { 
    'user': 'root', 
    'password': '123456', 
    'host': '127.0.0.1', 
    'charset': 'utf8',
    'db' : 'mtdb',
}

database = database.current(config)

def checkDataType(obj):
    attr_s = "%s,"
    attr_d = "%d,"
    attr_f = "%f,"
    if isinstance(obj, int):
        return attr_d
    elif isinstance(obj, str):
        return attr_s
    elif isinstance(obj, float):
        return attr_f
    elif isinstance(obj, datetime.date):
        return attr_s
    else:
        return None

def generateValuesReplaceStr(values):
    result = ""
    for item in values:
        #print type(item)
        result += checkDataType(item)
    result = result[:-1]
    return result

def generateValue(item):
    i = 0
    values = deepcopy(item.values())
    for value in values:
        if value == None:
            values[i] = 'NULL'
        i += 1
    ret = str(tuple(values))
    ret = ret.replace("'NULL'", 'NULL')
    return ret

def generateValues(data):
    result = ""
    for item in data:
        result += generateValue(item) + ","
    return result[:-1]

class dbHelper():
    
    @staticmethod
    def __init__(config):
        database.current(config)
        
    @staticmethod
    def setAutoCommit(autoCommit = False):
        database.setAutoCommit(autoCommit)

    @staticmethod
    def insert(table=None, data=None):
        "single table insert, data like [{'a':1, 'b':'abc'}] or {'a':1, 'b':'abc'}"
        if table == None or data == None:
            return None
        if isinstance(data, dict):
        #if str(type(data)) == "<type 'dict'>":
            item0 = data
            lenKeys = len(item0.keys()) 
            keys = ",".join(item0.keys())
            ss = lenKeys*"%s,"
            ss = ss[:-1]
            statement = "INSERT INTO " + table + " (" + keys + ") VALUES (" + ss + ")"
            dd = tuple(item0.values())
            ret = database.execute(statement, dd)
            return ret
        else:
            item0 = data[0]
            lenKeys = len(item0.keys()) 
            keys = ",".join(item0.keys())
            ss = lenKeys*"%s,"
            ss = ss[:-1]
            statement = "INSERT INTO " + table + " (" + keys + ") VALUES (" + ss + ")"
            dd = []
            for item in data:
                dd.append(tuple(item.values()))
            ret = database.executeMany(statement, dd)
            return ret
        
    @staticmethod
    def update(table=None, data={}, condition=None):
        "single table update, data like {'a':1, 'b':'abc'}, condition like 'where id = 1"
        if table == None or not data or condition == None:
            return None
        st = ''
        for key in data.keys():
            st += (key + '=' + '%s' + ',')
        st = st[:-1]

        statement = "UPDATE " + table + " SET " + st + " " + condition
        ret = database.execute(statement, data.values())
        return ret
    
    @staticmethod
    def delete(table=None, condition=None):
        "single table delete"
        if table == None:
            return None
        statement = ''
        if condition == None:
            statement = "DELETE FROM "+ table
        else:
            statement = "DELETE FROM "+ table + " " + condition
        ret = database.execute(statement)
        return ret

    @staticmethod
    def select(table=None, elements=None, condition=None):
        "single table select, elements like ['a', 'b'] and None is '*', condition like 'where id = 1"
        if table == None:
            return None
        em = '*'
        if elements:
            em = ", ".join(elements)
        statement = "SELECT " + em + " FROM " + table
        if condition:
            statement += " " + condition
        ret = database.query(statement)
        return ret
    
    @staticmethod
    def execute(query, params=None):
        return database.execute(query, params)
    
    @staticmethod
    def executeMany(query, params=None):
        return database.executeMany(query, params)
    
    @staticmethod
    def query(query, params=None):
        return database.execute(query, params)
    
    @staticmethod
    def rollback():
        database.rollback()

    @staticmethod
    def commit():
        database.commit()

dbHelper(config)
#dbHelper = dbHelper(config)


#data = [
#    {'first_name':'Jane', 'hire_date':str(date(2005, 2, 12)), 'sex':0},
#    {'first_name':'Joe', 'hire_date':str(date(2006, 5, 23)), 'sex':0}, 
#    {'first_name':'John', 'hire_date':str(date(2010, 10, 3)), 'sex':1},
#]

#data = {'first_name':'jacksonpan', 'hire_date':date(2013, 2, 12), 'sex':1}
#where = 'where id = 2'

#elements = ['id', 'first_name']
#where = 'where id = 31'

#dbHelper(autoCommit=True)
#dbHelper.insert('employees', data)
#dbHelper.commit()
#dbHelper.update('employees', data, where)
#dbHelper.delete('employees', None)
#print dbHelper.select('employees', elements, where)
#print dbHelper.select('employees', None, None)

#data = [
#  ('Jane', date(2005, 2, 12), 1),
#  ('Joe', date(2006, 5, 23), 1),
#  ('John', date(2010, 10, 3), 0),
#]
#stmt = "INSERT INTO employees (first_name, hire_date, sex) VALUES (%s, %s, %s)"
#dbHelper.executeMany(stmt, data)

#insert = "INSERT INTO employees (first_name, hire_date, sex) VALUES (%s, %s, %s)"
#data = ('Jane', date(2005, 2, 12), 1)
#dbHelper.execute(insert, data)

