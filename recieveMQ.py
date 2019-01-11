import pika
import json
import os
import sqlite3
from sqlite3 import Error
from sendMQ import options
from dicttoxml import dicttoxml
import csv
import platform
from sendMQ import stringJsonMsg

# THIS .PY CLASS RECEIVE A JSON MSG WITH THE DB PATH AND WITH THE QUERIES RESULT OPTION AND DOES THE FOLLOWING
#1)CREATES A DB CONNECTION FROM THE DB PATH STRING , PARSED FROM THE JSON MSG
#2)CALLS THE RELEVANT FUNCTION [JSON_RESULT , XML_RESULT , CSV_RESULT , DBTABLE_RESULT] AND RUN ALL THE QUERIES
#3)EXPORT THE RESULTS INTO A NEW DIR


## define the array with all the queries
#   query1     :    data to each song
#   query2     :    list of costumers data with quentity
#   query3     :    domain in each country
#   query4     :    songs with the word 'black' in it
#   query5     :    for each country how many disck/albums sold
#   query6     :    most selling disk/album in each country
#   query7     :    the most selling album in the US
QUREY_ARRAY = \
[
    "select tracks.Name as \"SongName\",artists.Name as \"Artist\",genres.Name as \"Genre\" from  tracks join albums on albums.AlbumId = tracks.AlbumId join artists on artists.ArtistId = albums.ArtistId join genres on genres.GenreId = tracks.GenreId where genres.GenreId!=18 and genres.GenreId!=19 and genres.GenreId!=20 and genres.GenreId!=21 and genres.GenreId!=22 order by tracks.Name",
    "SELECT  Name,Phone,Email,Quantity, CASE when Address IS NULL THEN \"NO-VALUE\" ELSE Address end as \"Address\" from (select customers.FirstName ||\" \"||customers.LastName as \"Name\", customers.Phone,customers.Email,customers.Address||\"-\"||customers.City||\",\"||customers.Country||\"-\"||customers.State as \"Address\",count(invoice_items.Quantity) as \"Quantity\" from customers join invoices on invoices.CustomerId = customers.CustomerId join invoice_items on invoice_items.InvoiceId = invoices.InvoiceId group by invoices.CustomerId order by Name)",
    "select Country,Domain,count(Full_data) as \"Amount\" from( select Country, Country||\" \"||replace(substr(Email, instr(Email, '@') + 1),ltrim(substr(Email, instr(Email, '@') + 1),replace(substr(Email, instr(Email, '@') + 1), '.', '')),'') as \"Full_data\", replace(substr(Email, instr(Email, '@') + 1),ltrim(substr(Email, instr(Email, '@') + 1),replace(substr(Email, instr(Email, '@') + 1), '.', '')),'') as \"Domain\" from customers order by Country)domain_status group by Full_data",
    "select distinct Name from tracks where instr(Name,'Black') >0",
    "select Country,Title,count(Full_data) as\"AmountOfAlbums\" from (select BillingCountry as \"Country\",albums.Title,BillingCountry||\" \"||albums.Title as \"Full_data\" from invoices join invoice_items on invoice_items.InvoiceId = invoices.InvoiceId join playlist_track on playlist_track.TrackId = invoice_items.TrackId join tracks on tracks.TrackId = playlist_track.TrackId join albums on albums.AlbumId = tracks.AlbumId join genres on genres.GenreId = tracks.GenreId where genres.GenreId!=18 and genres.GenreId!=19 and genres.GenreId!=20 and genres.GenreId!=21 and genres.GenreId!=22 order by BillingCountry)amount_of_albums group by Full_data",
    "select Country,Title,max(Amount_of_albums) as \"MostSelling\" from(select Country,Title,count(Full_data) as \"Amount_of_albums\" from (select BillingCountry as \"Country\",albums.Title,BillingCountry||\" \"||albums.Title as \"Full_data\" from invoices join invoice_items on invoice_items.InvoiceId = invoices.InvoiceId join playlist_track on playlist_track.TrackId = invoice_items.TrackId join tracks on tracks.TrackId = playlist_track.TrackId join albums on albums.AlbumId = tracks.AlbumId order by BillingCountry)amount_of_albums group by Full_data) group by Country",
    "select BillingCountry as \"Country\",Title as \"AlbumName\",max(Amount_of_albums) as \"MaxAmount\" from(select BillingCountry,Title,count(Full_data) as \"Amount_of_albums\" from(select BillingCountry,InvoiceDate,Title,BillingCountry||\" \"||albums.Title as \"Full_data\" from invoices join invoice_items on invoice_items.InvoiceId = invoices.InvoiceId join playlist_track on playlist_track.TrackId = invoice_items.TrackId join tracks on tracks.TrackId = playlist_track.TrackId join albums on albums.AlbumId = tracks.AlbumId where like('usa',BillingCountry)=1 and InvoiceDate between datetime('2010-01-01 00:00:00') and datetime('now') order by BillingCountry) group by Full_data order by Amount_of_albums desc)"
]
# define empty string for the received msg
DATA = stringJsonMsg
# define the DB_PATH string value
DB_PATH = ""
#define the FORMAT of the output
FORMAT = ""

# define a new function to init the key-value of the json msg and call to :create_connection_and_run_query
def parse_recievedMsg_and_CreateConnToDb(data):
    json_data = json.loads(data)
    DB_PATH = json_data['db-path']
    FORMAT = json_data['output-format']
    #call the function that will createa the connection to the db for us and will call to : run_query_as_{JSON MSG PARAM}
    create_connection_and_run_query(DB_PATH,FORMAT)
# define a new function to create a connection and then to run the query
def create_connection_and_run_query(db_file,format_to_parse):
    try:
        conn = sqlite3.connect(db_file)
        if (format_to_parse == options[0]):
            run_query_as_json(conn)

        if (format_to_parse == options[1]):
            run_query_as_xml(conn)

        if (format_to_parse == options[2]):
            run_query_as_csv( conn)

        if (format_to_parse == options[3]):
            run_query_as_dbTable( conn)

    except Error as e:
        print(e)
#function to run the queries and convert the results into json files
def run_query_as_json(conn):
    index=0
    cur = conn.cursor()
    for query in QUREY_ARRAY:
        index+=1
        cur.execute(query)
        row_headers = [x[0] for x in cur.description]
        rv = cur.fetchall()
        json_data=[]
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
        #C:/Users/AmitCoh/Desktop/home_test_PMO/queries_results/queryResultJSON' +str(index)+ '.json', 'w'
        f = open( 'queryResultJSON' +str(index)+ '.json', 'w' )
        f.write( json.dumps( json_data, indent=4, sort_keys=True ) )
        f.close()
#function to run the queries and convert the results into xml files
def run_query_as_xml(conn):
    index=0
    cur = conn.cursor()
    for query in  QUREY_ARRAY:
        index+=1
        cur.execute(query)
        row_headers = [x[0] for x in cur.description]
        rv = cur.fetchall()
        json_data=[]
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
        json.dumps(json_data)
        #'C:/Users/AmitCoh/Desktop/home_test_PMO/queries_results/queryResultXML' +str(index)+ '.xml', 'w'
        f = open( 'queryResultXML' +str(index)+ '.xml', 'w' )
        xml = str(dicttoxml(json_data, custom_root='query results data', attr_type=False))[2:-1]
        f.write(xml)
        f.close()
#function to run the queries and convert the results into csv files
def run_query_as_csv(conn):
    index=0
    cur = conn.cursor()
    for query in QUREY_ARRAY:
        index+=1
        cur.execute(query)
        row_headers = [x[0] for x in cur.description]
        rv = cur.fetchall()
        json_data=[]
        #'C:/Users/AmitCoh/Desktop/home_test_PMO/queries_results/queryResultCSV' +str(index)+ '.csv', 'w' ,encoding='utf-8'
        f = open( 'queryResultCSV' +str(index)+ '.csv', 'w' ,encoding='utf-8')
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
        spamwriter = csv.DictWriter( f, fieldnames=row_headers )
        spamwriter.writeheader()
        for res in json_data:
            spamwriter.writerow(res)
#function to run the queries and convert the results into databse tables (into a new db)
def run_query_as_dbTable(conn):
    index=0
    cur = conn.cursor()
    for query in  QUREY_ARRAY:
        index+=1
        cur.execute(query)
        row_headers = [x[0] for x in cur.description]
        rv = cur.fetchall()
        json_data=[]
        for result in rv:
            json_data.append(dict(zip(row_headers,result)))
        json.dumps(json_data)
        keys = []
        strToBuildTable = []
        strToInsertValues = []
        valuesToInsert = []
        #"C:/Users/AmitCoh/Desktop/home_test_PMO/db_relatives/sqlite-tools-win32-x86-3260000/chinookQueriesDB.db"
        conn1 = sqlite3.connect("chinookQueriesDB.db" )
        cur1 = conn1.cursor()
        for row in json_data:
            for key in row.keys():
                if key not in keys:
                    keys.append( key )
                    strToBuildTable.append(key + " TEXT")
                    strToInsertValues.append(key)
            cur1.execute("CREATE TABLE IF NOT EXISTS query"+str(index)+"("+ str(strToBuildTable)[2:-2].replace("\'","") +")")
            for value in row.values():
                valuesToInsert.append("\""+str(value).replace("\"","")+"\"")
            with conn1:
                cur1.execute("INSERT INTO query"+str(index)+"("+str(strToInsertValues)[2:-2].replace("\'","")+")  VALUES("+ str(valuesToInsert)[1:-1].replace("\'","")+")")
            valuesToInsert.clear()

#define a function in order to determine the user platform: Windows,Linux,Mac - in order to create a dir to contain the results
##Currently,unused - the files will be created in the project folder
def extract_user_platform():
    userPlatform = platform.platform().split("-")[0]
    return userPlatform
                                                        # # # MQ # # #

connection = pika.BlockingConnection( pika.ConnectionParameters(host= 'localhost' ) )
channel = connection.channel()

channel.queue_declare(queue='db_conn')

def callback(ch,method,properties,body):
    DATA = body
    parse_recievedMsg_and_CreateConnToDb(DATA)
    print(("[x] received json %s" % DATA))

channel.basic_consume(callback,queue='db_conn',no_ack=True)
print( '[*] Waiting for msgs' )
try:
    channel.start_consuming()
finally:
    connection.close()


