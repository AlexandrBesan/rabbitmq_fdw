from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log 

from logging import ERROR
import pika
import uuid 
import json 

class RabbitmqFDW(ForeignDataWrapper):
    
    def __init__(self, options, columns):
        super(RabbitmqFDW, self).__init__(options, columns)
        self.columns = columns
        log('columns: %s' % columns)
        log('options: %s' % options)
        username = options.get('username', 'guest')
        password = options.get('password', 'guest')
        self.credentials =  pika.PlainCredentials(username, password)

        self.host = options.get('host', 'localhost')
        self.port = int(options.get('port', 5672))

        self.virtual_host  = options.get('virtual_host', '/')
        self.queue  = options.get('queue', 'main')
        self.exchange = options.get('exchange', 'indexing') 
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
        self.connectionParameters = pika.ConnectionParameters(self.host,
                                    self.port,
                                    self.virtual_host,
                                    self.credentials)
        self.data = list()
        self.connection = pika.BlockingConnection(self.connectionParameters)
        
        self.channel = self.connection.channel()




    def __del__(self):
            self.channel.close()
            self.connection.close() 

    @property
    def rowid_column(self):
        log('rowid')
        return self.rowid ;

    # def execute(self, quals, columns): 
    #     pass
    #     log('quals: %s' % quals)
    #     log('columns: %s' % columns) 
    #     limit = self.bulk_size

    #     for method_frame, properties, body in self.channel.basic_get(queue=self.queue , auto_ack=True): 
    #         print(method_frame)
    #         print(properties)
    #         print(body)      
    #         line = {}
    #         log('method_frame: %s' % method_frame)        
    #         log('properties: %s' % properties)        
    #         log('body: %s' % body)  
    #         line['body'] = body.decode()
    #         self.channel.basic_ack(method_frame.delivery_tag) 
    #         if method_frame.delivery_tag == limit:
    #             break 
    #         yield line
    #     self.channel.cancel()

    def insert(self, new_values): 
        log('new_values: %s' % new_values) 
        if self.isTable: 
            content = json.dumps(new_values)
        else: 
            content = new_values[self.column]
        self.channel.basic_publish(exchange=self.exchange,
                        routing_key=self.queue,
                        body=content) 