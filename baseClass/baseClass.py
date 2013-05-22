# -*- coding: utf-8 -*-

from tool.tuTool import dict
#import types
from mongoHelper import mongoHelper

class baseClass():
    def __init__(self, **data):
        for k, v in data.iteritems():
            setattr(self, k, v)
            
    def getDict(self):
        return dict(self)
    
    @classmethod
    def getClassName(cls):
        return cls.__name__

    def insert(self):
        ct = mongoHelper.getCollection(self.getClassName())
        data = self.getDict()
        return ct.insert(data)
    
    def insertWithData(self, data):
        ct = mongoHelper.getCollection(self.getClassName())
        return ct.insert(data)
    
    def _find(self, *args, **kwargs):
        ct = mongoHelper.getCollection(self.getClassName())
        return ct.find(*args, **kwargs)
    
    def find(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        return self._find(spec=spec, fields=fields, skip=skip, limit=limit, sort=sort)
    
    def _find_one(self, spec_or_id=None, *args, **kwargs):
        ct = mongoHelper.getCollection(self.getClassName())
        return ct.find_one(spec_or_id, *args, **kwargs)
    
    def find_one(self, spec=None, fields=None, skip=0, limit=0, sort=None):
        return self._find_one(spec_or_id=spec, fields=fields, skip=skip, limit=limit, sort=sort)
    
    def update(self, spec, document):
        ct = mongoHelper.getCollection(self.getClassName())
        return ct.update(spec, document)
    
    def drop(self):
        ct = mongoHelper.getCollection(self.getClassName())
        ct.drop()
        
    def remove(self, spec_or_id=None):
        ct = mongoHelper.getCollection(self.getClassName())
        return ct.remove(spec_or_id)
    
#baseClass().update({},{})