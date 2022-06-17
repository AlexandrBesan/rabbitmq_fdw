
import pika
import pika.exceptions


connect_exceptions = (
    pika.exceptions.ConnectionClosed,
    pika.exceptions.AMQPConnectionError,
    pika.exceptions.StreamLostError,
    pika.exceptions.ChannelWrongStateError,
    pika.exceptions.ChannelClosed,
) 

class RabbitmqConnector(): 
    def __init__(self, options): 
        username = options.get('username', 'guest')
        password = options.get('password', 'guest')
        self.host = options.get('host', 'localhost')
        self.port = int(options.get('port', 5672))
        self.credentials =  pika.PlainCredentials(username, password)
        self.virtual_host  = options.get('virtual_host', '/')
        self.queue  = options.get('queue', 'main')
        self.exchange = options.get('exchange', 'indexing') 
        self.connectionParameters = pika.ConnectionParameters(self.host,
                                    self.port,
                                    self.virtual_host,
                                    self.credentials)
        self.connection =  pika.BlockingConnection(self.connectionParameters)  
        self.channel =  self.connection.channel() 

    def connect(self):  
        if not self.connection or self.connection.is_closed: 
            self.connection = pika.BlockingConnection(self.connectionParameters)
            self.channel = self.connection.channel()


    def disconnect(self):  
        self.channel.close()
        self.connection.close() 

    def publish(self, msg):
        try:
            self.channel.basic_publish(exchange=self.exchange,
                            routing_key=self.queue,
                            body=msg) 
        except connect_exceptions as e: 
            self.connect()
            self.channel.basic_publish(exchange=self.exchange,
                            routing_key=self.queue,
                            body=msg) 

    def get_message_count(self): 
        try: 
            queue = self.channel.queue_declare(self.queue , durable = True)
        except connect_exceptions as e: 
            self.connect()             
            queue = self.channel.queue_declare(self.queue , durable = True)       
        queue_length = queue.method.message_count 
        return queue_length


    def get_message(self):
        try: 
            (method_frame, properties, body)=self.channel.basic_get(queue=self.queue , auto_ack=True)
        except connect_exceptions as e: 
            self.connect()       
            (method_frame, properties, body)=self.channel.basic_get(queue=self.queue , auto_ack=True)
        return body 

    def __del__(self):
        self.disconnect()