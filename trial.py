import mysql.connector
import pymongo
import sqlparse

# connect to the MongoDB server and retrieve the collection
client = pymongo.MongoClient('mongodb://localhost:27017/')
collection = client['mydatabase']['mycollection']；

# connect to the MySQL database
conn = mysql.connector.connect(user='root', password='password', host='localhost', database='mydatabase')
cursor = conn.cursor()

# retrieve all documents from the MongoDB collection
documents = collection.find()

# iterate over the documents and insert them into the MySQL table
for doc in documents:
    # convert the document to a SQL insert statement
    sql = sqlparse.format('INSERT INTO mytable (name, age) VALUES ({}, {})'.format(doc['name'], doc['age']), keyword_case='upper')

    # execute the SQL statement
    cursor.execute(sql)

# commit the changes and close the connection
conn.commit()
cursor.close()
conn.close()