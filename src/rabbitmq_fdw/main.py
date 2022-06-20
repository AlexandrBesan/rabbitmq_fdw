from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log 

from logging import ERROR
from logging import INFO
import uuid 
import json 
from json import JSONDecodeError

from .RabbitmqConnector import RabbitmqConnector

class RabbitmqFDW(ForeignDataWrapper):
    
    def __init__(self, options, columns):
        super(RabbitmqFDW, self).__init__(options, columns)
        self.columns = columns
        log('columns: %s' % columns)
        log('options: %s' % options) 
        self.rowid = options.get('rowid_column','id')
        self.column = options.get('column',None) 
        if      self.column in list(self.columns.keys()): 
            self.isTable = False
        elif    self.column is  None:
            self.isTable = True
        elif    self.column not in list(self.columns.keys()): 
            log("There are no such a column in table %s " % self.column , ERROR) 
            self.isTable = True
        self.bulk_size = int(options.get('bulk_size', 1000)) 
        self.rabbitmq = RabbitmqConnector(options)
        
    @property
    def rowid_column(self):
        log('rowid')
        return self.rowid ;

    def execute(self, quals, columns):   
        limit = self.bulk_size
        queue_length = self.rabbitmq.get_message_count()
        for i in range(min(queue_length, limit )):
            body =  self.rabbitmq.get_message()
            line = {} 
            if self.isTable: 
                for col in self.columns:
                    try: 
                        data = json.loads( body.decode() )
                    except JSONDecodeError: 
                        log("Wrong format json"  , ERROR)  
                    line[col] = data[col] 
            else: 
                line[self.column] = body.decode() 
            yield line 
 

    def insert(self, new_values): 
        log('new_values: %s' % new_values) 
        if self.isTable: 
            content = json.dumps(new_values)
        else: 
            content = new_values[self.column]
        self.rabbitmq.publish(content)
    
