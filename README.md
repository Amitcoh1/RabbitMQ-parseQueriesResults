# RabbitMQ - parse queries results 

In this project I will recieve a json message using a message queue (rabbitMQ),
the message will contain the db path(chinook db) and a format of output.
I will run queries on the db and the results will be in the format which we have recieved from the user.

The formats are : JSON , XML , CSV , DB-TABLE

## Getting Started

In order to test this code , please open a new project and import all the .py files.
In addition, replace the DB-PATH to your own.

### Prerequisites

In order to use this project and test it on your own machine please do the following:

### Installing

A step by step series of examples that tell you how to get a development env running

First, download and install dicttoxml - this is for the conversion to XML format

```
pip install dicttoxml
```
Pika:
Pika is available for download via PyPI and may be installed using easy_install or pip:

```
pip install pika 
```
or
```
easy_install pika 
```
After you have installed the following , all the imports will be availabe.


### NOTE

All the files will be saved in the project folder which you have opened and import all the .py files.

The option of create a db-table creates a new db (chinookQueriesDB.db) and inside all the tables will be created.


## Built With

* [chinookDB](http://www.sqlitetutorial.net/sqlite-sample-database/) - The database
* [Erlang](https://www.erlang.org/downloads) - The framework (OTP 21.2)
* [SQLite](https://www.sqlite.org/download.html) - Database managment Ver. 3
* [RabbitMQ](https://rometools.github.io/rome/) - The message queue

* Python Ver. 3.6 

## Authors

* **Amit Cohen** 
