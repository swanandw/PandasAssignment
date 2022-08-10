import pandas as pd
import pymongo
import logging
import mysql.connector as conn


## csvsql -i mysql --snifflimit 100000 "/Users/swanand-walke/Downloads/Superstore_USA.csv"  --- creates table structure
## csvsql --db mysql+mysqldb://root:Altitude@11@127.0.0.1/ineuron --tables Surerstore --insert /Users/swanand-walke/Downloads/Superstore_USA.csv--- insert records in db from csv
# above command does not work to insert data into table


logging.basicConfig(filename="SuperStore_USA.log", level=logging.DEBUG,format='%(asctime)s %(levelname)s %(name)s %(message)s')

logging.info("First step is to fetch the data from csv using pandas library ")
df= pd.read_csv('Superstore_USA.csv',encoding= "ISO-8859-1")
logging.info("Data is fetched successfully from csv file ")



logging.info("Convert a data from csv into json format")
convertedintojson=df.to_json()
#print(convertedintojson)
logging.info("Data Converted into Json format successfully")

logging.info("Insert json data into mongo db ")
try:
    logging.info("Make a connection with MongoDB")
    client = pymongo.MongoClient("mongodb+srv://prashant:palkar@cluster0.hee8g.mongodb.net/?retryWrites=true&w=majority")
    db = client.test
    #print(db)
    logging.info("Connection is successfully established")
except Exception as e:
    logging.info("Getting an error while connection to MongoDB",e)

try:
    logging.info("Create a new database here")
    Superstore = client['SuperstoreUSAinventory']
    logging.info("database  created ")

    logging.info("Create a new collection")
    collection_SuperstoreUSA = Superstore["fitbit_table"]
    logging.info("new collection  created ")

    logging.info("Insert json data into collection")
    collection_SuperstoreUSA.insert_many(convertedintojson)
    logging.info("data inserted into collection")

except Exception as e:
    logging.info("Data not inserted successfully into a collection")

logging.info("Second step is to make connection to MySQl database")
try:
    logging.info("Making a connection with MySql database")
    mydb= conn.connect(host = "localhost", user ="root", passwd ="Altitude@11" )
    print(mydb)
    cursor = mydb.cursor(buffered=True)
    cursor = mydb.cursor()
   # cursor = mydb.cursor(buffered=True)
   # cursor = mydb.cursor(buffered=True)

    logging.info("Connection is successful")
except Exception as e:
    print("Error while connecting to MySQL", e)

logging.info("Create a table Fitbitdata")
try:
    logging.info("Creating a new table SuperstoreUSA")
    #cursor.execute("create table sqlassignment.SuperstoreUSA(Id DECIMAL(38, 0) , ActivityDate text , TotalSteps DECIMAL(38, 0) ,TotalDistance DECIMAL(38, 17) ,TrackerDistance DECIMAL(38, 17) ,LoggedActivitiesDistance DECIMAL(38, 15) ,VeryActiveDistance DECIMAL(38, 17) ,ModeratelyActiveDistance DECIMAL(38, 16) ,LightActiveDistance DECIMAL(38, 17) ,SedentaryActiveDistance DECIMAL(38, 17) ,VeryActiveMinutes DECIMAL(38, 0) ,FairlyActiveMinutes DECIMAL(38, 0) ,LightlyActiveMinutes DECIMAL(38, 0) ,SedentaryMinutes DECIMAL(38, 0) ,Calories DECIMAL(38, 0) )")
    cursor.execute("create table sqlassignment.SuperstoreUSA(RowID DECIMAL(38, 0) , OrderPriority VARCHAR(13) , Discount DECIMAL(38, 2) , UnitPrice DECIMAL(38, 2) , ShippingCost DECIMAL(38, 2) , CustomerID DECIMAL(38, 0) , CustomerName VARCHAR(28) , ShipMode VARCHAR(14) , CustomerSegment VARCHAR(14) , ProductCategory VARCHAR(15) , ProductSubCategory VARCHAR(30) , ProductContainer VARCHAR(10) , ProductName VARCHAR(98) , ProductBaseMargin DECIMAL(38, 2), Region VARCHAR(7) , StateorProvince VARCHAR(20) , City VARCHAR(19) , PostalCode DECIMAL(38, 0) , OrderDate text , ShipDate text , Profit DECIMAL(38, 7) , Quantityorderednew DECIMAL(38, 0) , Sales DECIMAL(38, 2) , OrderID DECIMAL(38, 0) )")
    logging.info("table created")

    for i,row in df.iterrows() :
        sql = "insert into sqlassignment.SuperstoreUSA values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cursor.execute(sql,tuple(row))
        mydb.commit()
    logging.info("Data successfully inserted into SuperstoreUSA table")

except Exception as e:
    print("Error while inserting records to SuperstoreUSA table", e)


logging.info("Select all rows from SuperstoreUSA table")
sql = "SELECT * FROM sqlassignment.SuperstoreUSA"
logging.info("Execute the query")
cursor.execute(sql)
logging.info("query Executed successfully")

logging.info("select all the rows ")
allrecords =cursor.fetchall()
logging.info("All rows being selected")

logging.info("Print all the rows")
for i in allrecords:
    print(i)
logging.info("all rows are printed")



