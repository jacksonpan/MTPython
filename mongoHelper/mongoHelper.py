'''
Created on 2013-5-21

@author: jacksonpan
'''
import pymongo
import threading
from bson.objectid import ObjectId

SORT_ASC = pymongo.ASCENDING
SORT_DESC = pymongo.DESCENDING

strObjectId = "_id"
strLessThan = "$lt"
strLessThanOrEqual = "$lte"
strGreaterThan = "$gt"
strGreaterThanOrEqual = "$gte"
strIsNotEqualTo = "$ne"
strSet = "$set"

def helpObjectId(strId):
    return ObjectId(strId)

class mongoHelper(object):
    instance = None
    mutex = threading.Lock()
    
    @staticmethod
    def current(host='localhost', port=27017):
        if(mongoHelper.instance == None):
            mongoHelper.mutex.acquire()
            if(mongoHelper.instance == None):
                # print 'init class object'
                mongoHelper.instance = mongoHelper(host, port)
            else:
                # print 'object exist'
                pass
            mongoHelper.mutex.release()
        else:
            # print 'object exist'
            pass
        return mongoHelper.instance

    def __init__(self, host='localhost', port=27017):
        self.connect(host, port)
    
    def connect(self, host='localhost', port=27017):
        self.conn = pymongo.Connection(host, port)
    
    def getConn(self, **data):
        return self.conn
    
    def getDatabase(self, db_name):
        self.db_name = db_name
        return self.conn[self.db_name]
    
    def getCollection(self, ct_name):
        self.ct_name = ct_name
        return self.conn[self.db_name][self.ct_name]
    
    def dropCollection(self, ct_name):
        self.ct_name = ct_name
        ct = self.conn[self.db_name][self.ct_name]
        ct.drop()
    
mongoHelper = mongoHelper()
#db = dbHelper.getDatabase('local')
#ct = dbHelper.getCollection('c1')
#print ct
#print dbHelper.local.collection_names()
#for item in ct.find():
#    print item