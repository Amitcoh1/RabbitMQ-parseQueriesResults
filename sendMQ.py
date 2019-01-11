import pika
import json

# THIS .PY CLASS SENDS A JSON MSG TO THE MQ WITH THE DB PATH AND WITH THE QUERIES RESULT OPTION
##YOU CAN SELECT FROM THE FOLLOEING OPTIONS : 1) JSON 2) XML 3) CSV 4)DB TABLE

# db path : user have to send the full path and give the database name.db
#NOTE: if the path will look like that  : /user/dbFolder/dbname , then it will create a file with the 'dbname' and won't be able to connect your existing database file.
#So, please send the path that way :      /user/dbFolder/dbname.db in order to connect the existing database
DB_PATH = 'C:/Users/AmitCoh/Desktop/home_test_PMO/db_relatives/sqlite-tools-win32-x86-3260000/chinookDB.db'
# array of queries output options
options = ['json', 'xml', 'csv', 'table']
# build the message as string in json structure using the array and the path const
stringJsonMsg = {'db-path': DB_PATH, 'output-format': options[0]}
# convert the string into json valid format
convertStringToJsonData = json.dumps(stringJsonMsg)

                                       # # # MQ # # #

# connect to localhost using pika : Pika is a pure-Python implementation of the AMQP 0-9-1 protocol
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
#declare on the queue name we are going to listen to on the recieve.py class
channel.queue_declare(queue='db_conn')
#public the channel with the given info/params
#exchange    == (str or unicode) – The exchange to publish to
#routing_key == (str or unicode) – The routing key to bind on
#body        == (str or unicode) – The message body
channel.basic_publish(exchange='', routing_key='db_conn', body=convertStringToJsonData)
print("[x] sent %s" % convertStringToJsonData)
#close the connection when finish
connection.close()
