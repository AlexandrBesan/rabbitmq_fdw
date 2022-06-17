----------------------------------------------------------
--use 
----------------------------------------------------------
CREATE FOREIGN DATA WRAPPER rabbitmq_fdw;
 
CREATE SERVER rabbitmq FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'rabbitmq_fdw.RabbitmqFDW',
  host 'rabbitmq',
  virtual_host '/',
  port '5672',
  exchange ''
);

CREATE USER MAPPING FOR public
SERVER rabbitmq
OPTIONS (username 'dev', password '12345')
;

----------------------------------------------------------
----------------------------------------------------------


CREATE OR REPLACE FUNCTION rabbitmq_push(
    body json,
    bulk_size varchar(2) = '10',
    queue  varchar(30) = 'externally_configured_queue'
)
RETURNS bool
    LANGUAGE 'plpython3u'
AS $BODY$
    import rabbitmq_fdw
    options = rabbitmq_fdw.getPostgresOptions(plpy, servername ='rabbitmq')
    options['bulk_size'] = bulk_size
    options['queue'] = queue
    rmq = rabbitmq_fdw.RabbitmqConnector(options)
    rmq.publish(body)
    return True
$BODY$;


CREATE OR REPLACE FUNCTION rabbitmq_get_message(
    bulk_size int = 10,
    queue  varchar(30) = 'externally_configured_queue'
)
RETURNS table ( body text)
    LANGUAGE 'plpython3u'
AS $BODY$
    import rabbitmq_fdw
    options = rabbitmq_fdw.getPostgresOptions(plpy, servername = 'rabbitmq')
    options['bulk_size'] = bulk_size
    options['queue'] = queue
    rmq = rabbitmq_fdw.RabbitmqConnector(options)
    limit = bulk_size
    queue_length = rmq.get_message_count()
    result = []
    for i in range(min(queue_length, limit )):
        body =  rmq.get_message()
        line = {}
        line['body'] = body.decode()
        result.append(line)
    return result
$BODY$;
