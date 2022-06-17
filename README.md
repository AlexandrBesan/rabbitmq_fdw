# PostgreSQL RabbitMQ Foreign Data Wrapper

Main goal of the rabbitmq Foreign Data Wrapper is to 
1. Fast and convinience publish data from Postgresql 
2. To read data from RabbitMQ to Postgresql DB (by approach of bunch messages to read - the amount of the moment of call with a limit)

## Requirements

- [Multicorn](http://multicorn.org/) 
- Python 3 
- Python package pika 
- RabbitMQ server configured   
- plpython and multicorn extensions on PostgreSQL server
- PostgreSQL server configured
## Installation

```bash 
wget https://github.com/AlexandrBesan/rabbitmq_fdw/archive/refs/heads/main.zip
sudo su
python3 setup.py install
```
## Usage
First thing is to create server with wrapper to rabbitmq_fdw
```sql 
CREATE SERVER rabbitmq FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'rabbitmq_fdw.RabbitmqFDW',
  host 'rabbitmq',
  virtual_host '/',
  port '5672',
  exchange ''
);

```
for the server you should create user mapping to the user/password on rabbitmq accordinly to your roles in DB.
```sql 
CREATE USER MAPPING FOR public
SERVER rabbitmq
OPTIONS (username 'quest', password 'quest')
;
```

The next is to creade foreign table with all option needed 
There is option column, if there is value in that column - with insert to the rabbitmq server to the queue will be published only value from that column. 
```sql 
CREATE FOREIGN TABLE test  (
    body
)
SERVER rabbitmq
OPTIONS ( 
    bulk_size '10',
    queue 'externally_configured_queue', 
    column 'body'
);
```
, if there is value in that column is ommited  - with insert to the rabbitmq server to the queue will be published  the whole row in the table transformed to the json (key - column name). For now all types in json - string. 
```sql 
CREATE FOREIGN TABLE test  (
    body
)
SERVER rabbitmq
OPTIONS ( 
    bulk_size '10',
    queue 'externally_configured_queue'
);
```

## Alternative usage by functions 

The same there is need to create objects FOREIGN DATA WRAPPER , SERVER and USER MAPPING. 

Function to push to the queue by using rabbitmq_fdw library 
```sql 

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
```

Function to get from the queue (by bulk limit) by using rabbitmq_fdw library 
```sql 
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
```
## To DO 

1. Type of the json fields mapped to the type of the table fields. 


