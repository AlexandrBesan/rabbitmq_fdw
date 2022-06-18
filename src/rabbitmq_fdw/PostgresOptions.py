
from .RabbitmqConnector import RabbitmqConnector

class RabbitmqConnectorPLPY(): 

    def __init__(self, plpy, servername, queue,  bulk_size): 
        import json
        self.plpy = plpy
        self.bulk_size = bulk_size
        self.queue = queue
        def getDictFromOptions(opt):
            options = {}
            for i in opt:
                (key , value) = i.split('=')
                options[key]= value
            return options 
        serveroptions = plpy.execute("""
            select fs.srvoptions
            from pg_foreign_server fs
            join pg_foreign_data_wrapper fdw on fdw.oid=fs.srvfdw
            where fdw.fdwname='multicorn' and fs.srvname='%s'
        """ % servername)
        if serveroptions.nrows()==0: 
            raise Exception("Empty options for provided server name")
        serveroptions = getDictFromOptions(serveroptions[0]['srvoptions'])
        credentials =  plpy.execute("""
            select um.umoptions
            from pg_foreign_server fs
                        join pg_foreign_data_wrapper fdw on fdw.oid = fs.srvfdw
                        join pg_user_mappings um on um.srvname = fs.srvname
            where fs.srvname = '%s'
            and fdw.fdwname = 'multicorn'
            limit 1 
        """ % servername)
        if credentials.nrows()==0: 
            raise Exception("Empty credentials for provided server name")
        credentials = getDictFromOptions(credentials[0]['umoptions'])
        self.options = {}
        self.options.update(serveroptions)
        self.options.update(credentials) 
        self.options['bulk_size'] = self.bulk_size
        self.options['queue'] = self.queue

    def push(self, body): 
        rmq = RabbitmqConnector(self.options)
        rmq.publish(body)
 
    def get_bulk_message(self):
        rmq = RabbitmqConnector(self.options)
        limit = self.bulk_size
        queue_length = rmq.get_message_count()
        result = []
        for i in range(min(queue_length, limit )):
            body =  rmq.get_message()
            line = {}
            line['body'] = body.decode()
            result.append(line)
        return result