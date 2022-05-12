from multicorn import ForeignDataWrapper
from multicorn.utils import log_to_postgres as log 

from logging import ERROR
from logging import INFO
import pika
import uuid 
import json 
import pika.exceptions


connect_exceptions = (
    pika.exceptions.ConnectionClosed,
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.StreamLostError,
    pika.exceptions.ChannelWrongStateError,
    pika.exceptions.ChannelClosed,
) 

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
        self.connection =  pika.BlockingConnection(self.connectionParameters)  
        self.channel =  self.connection.channel() 

    def _connect(self):  
        if not self.connection or self.connection.is_closed: 
            self.connection = pika.BlockingConnection(self.connectionParameters)
            self.channel = self.connection.channel()

 

    def __del__(self):
            self.channel.close()
            self.connection.close() 

    def _publish(self, msg):
        try:
            self.channel.basic_publish(exchange=self.exchange,
                            routing_key=self.queue,
                            body=msg) 
        except connect_exceptions as e: 
            self._connect()
            self.channel.basic_publish(exchange=self.exchange,
                            routing_key=self.queue,
                            body=msg) 
    def _getMessage_count(self): 
        try: 
            queue = self.channel.queue_declare(self.queue , durable = True)
        except connect_exceptions as e: 
            self._connect()             
            queue = self.channel.queue_declare(self.queue , durable = True)       
        queue_length = queue.method.message_count 
        return queue_length

    def _getMessage(self): 
        try: 
            (method_frame, properties, body)=self.channel.basic_get(queue=self.queue , auto_ack=True)
        except connect_exceptions as e: 
            self._connect()       
            (method_frame, properties, body)=self.channel.basic_get(queue=self.queue , auto_ack=True)
        return body 
        
    @property
    def rowid_column(self):
        log('rowid')
        return self.rowid ;

    def execute(self, quals, columns):   
        limit = self.bulk_size
        queue_length = self._getMessage_count()
        for i in range(min(queue_length, limit )):
            body = self._getMessage()
            line = {} 
            line['body'] = body.decode() 
            yield line 
 

    def insert(self, new_values): 
        log('new_values: %s' % new_values) 
        if self.isTable: 
            content = json.dumps(new_values)
        else: 
            content = new_values[self.column]
        self._publish(content)
    
