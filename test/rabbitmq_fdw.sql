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
    servername varchar(30) ='rabbitmq' ,
    queue  varchar(30) = 'externally_configured_queue',
    bulk_size int = 10
)
RETURNS bool
    LANGUAGE 'plpython3u'
AS $BODY$
    import rabbitmq_fdw
    rmq = rabbitmq_fdw.RabbitmqConnectorPLPY(plpy, servername  , queue , bulk_size)
    rmq.push(body)
    return True
$BODY$;

CREATE OR REPLACE FUNCTION rabbitmq_get_message(
    servername varchar(30) ='rabbitmq' ,
    queue  varchar(30) = 'externally_configured_queue',
    bulk_size int = 10
)
RETURNS table ( body text)
    LANGUAGE 'plpython3u'
AS $BODY$
    import rabbitmq_fdw
    rmq = rabbitmq_fdw.RabbitmqConnectorPLPY(plpy, servername  , queue , bulk_size)
    result = rmq.get_bulk_message()
    return result
$BODY$;
