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
python3 setup.py install
```
## Usage
First thing is to create server with wrapper to rabbitmq_fdw
```sql 
CREATE SERVER rabbitmq FOREIGN DATA WRAPPER multicorn
OPTIONS (
  wrapper 'rabbitmq_fdw.RabbitmqFDW'
);

```

The next is to creade foreign table with all option needed 
There is option column, if there is value in that column - with insert to the rabbitmq server to the queue will be published only value from that column. 
```sql 
CREATE FOREIGN TABLE test  (
    body
)
SERVER testrb
OPTIONS (
    host 'rabbitmq',
    virtual_host '/',
    bulk_size '10',
    queue 'externally_configured_queue',
    port '5672',
    username 'quest',
    password 'quest',
    exchange '',
    column 'body'
);
```
, if there is value in that column is ommited  - with insert to the rabbitmq server to the queue will be published  the whole row in the table transformed to the json (key - column name). For now all types in json - string. 
```sql 
CREATE FOREIGN TABLE test  (
    body
)
SERVER testrb
OPTIONS (
    host 'rabbitmq',
    virtual_host '/',
    bulk_size '10',
    queue 'externally_configured_queue',
    port '5672',
    username 'quest',
    password 'quest',
    exchange '',
    column 'body'
);
```
## To DO 
1. Implementation of excecute method. 
2. Type of the json fields mapped to the type of the table fields. 


how to basic setup 
https://github.com/kennethreitz/setup.py/tree/master/mypackage
https://packaging.python.org/en/latest/tutorials/packaging-projects/

https://github.com/pypa/sampleproject/blob/main/setup.py


