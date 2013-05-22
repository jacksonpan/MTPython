# -*- coding:utf-8 -*-
from mysql import connector
import os
import threading
import itertools
from datetime import * 

class database():
    instance = None
    mutex = threading.Lock()
    db = None
    autoCommit = False
    config = None
    
    def __init__(self, config):
        self.setConfig(config)
        self.init()
        
    def setConfig(self, config):
        self.config = config
        self.reconnect()

    @staticmethod
    def current(config):
        if(database.instance == None):
            database.mutex.acquire()
            if(database.instance == None):
                # print 'init class object'
                database.instance = database(config)
            else:
                # print 'object exist'
                database.instance.setConfig(config)
                pass
            database.mutex.release()
        else:
            # print 'object exist'
            database.instance.setConfig(config)
            pass
        return database.instance
    
    def init(self):
        try:
            self.reconnect()
        except Exception, e:
            print e
    
    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        if self.db == None:
            pass
        else:
            self.db.close()
            self.db = None
        self.db = connector.connect(**self.config)
        self.db.autocommit = self.autoCommit
        
    def commit(self):
        self.db.commit()
        
    def rollback(self):
        self.db.rollback()
        
    def setAutoCommit(self, value=False):
        self.checkIsNeedReconnect()
        self.autoCommit = value
        self.db.autocommit = self.autoCommit
        
    def checkIsNeedReconnect(self):
        if self.db == None:
            print 'no connect with mysqldatabse'
            self.reconnect()
        else:
            if self.db.is_connected() == True:
                return;
            else:
                print 'disconnect with mysqldatabse'
                self.reconnect()
        
    
    def close(self):
        if self.db != None:
            self.db.close()
            self.db = None
            
    def execute(self, query, params=None):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, params)
            if cursor.with_rows:
                return cursor.fetchall()
            else:
                #print cursor.statement
                return None
        finally:
            cursor.close()
    
    def executeMany(self, query, seq_params):
        """Executes the given query, returning the lastrowid from the query. suggest insert"""
        cursor = self._cursor()
        try:
            self._executeMany(cursor, query, seq_params)
            if cursor.with_rows:
                return cursor.fetchall()
            else:
                #print cursor.statement
                return None
        finally:
            cursor.close()
            
    def _cursor(self):
        self.checkIsNeedReconnect()
        return self.db.cursor()

    def _execute(self, cursor, query, params):
        try:
            return cursor.execute(query, params)
        except Exception, e:
            print e
            print "Error connecting to MySQL"
            self.close()
            raise
        
    def _executeMany(self, cursor, query, seq_params):
        try:
            return cursor.executemany(query, seq_params)
        except Exception, e:
            print e
            print "Error connecting to MySQL"
            self.close()
            raise
    
    def query(self, query, params=None):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, params)
            column_names = [d[0] for d in cursor.description]
            return [Row(itertools.izip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except Exception, e:
            print e
            #raise AttributeError(name)

#alldata = database.execute('select * from user')
#print alldata

#ret = database.query('select * from user limit 0,1')
#print ret

#ret = database.query('select * from user limit 0,1')
#print ret

#ret = database.query('select * from user limit 0,1')
#print ret

#data = [
#  ('Jane', date(2005, 2, 12)),
#  ('Joe', date(2006, 5, 23)),
#  ('John', date(2010, 10, 3)),
#]
#data = ('John', date(2010, 10, 3))
#stmt = "INSERT INTO employees (first_name, hire_date, sex) VALUES (%s, %s, %s)"
#ret = database.executeMany(stmt, data)
#database.commit()

#insert = (
#"INSERT INTO employees (first_name, hire_date, sex) "
#"VALUES (%s, %s, %s)")
#print insert
#data = ('John', date(2010, 10, 3), 1)
#cursor = database._cursor()
#cursor.execute(insert, data)