import pandas as pd
import  mysql.connector as conn
import  logging
import pymongo

## csvsql -i mysql --snifflimit 100000 "/Users/swanand-walke/Downloads/FitBitdata.csv"  --- creates table structure
## csvsql --db mysql+mysqldb://root:Altitude@11@127.0.0.1/ineuron --tables FitBitdata --insert /Users/swanand-walke/Downloads/FitBitdata.csv--- insert records in db from csv
# above command does not work to insert data into table


logging.basicConfig(filename="FitBit.log", level=logging.DEBUG,format='%(asctime)s %(levelname)s %(name)s %(message)s')

logging.info("First step is to fetch the data from csv using pandas library ")
df= pd.read_csv('FitBitData.csv')
logging.info("Data is fetched successfully from csv file ")



logging.info("Convert a data from csv into json format")
convertedintojson=df.to_json()
print(convertedintojson)
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
    FitBitData = client['FitBitinventory']
    logging.info("database  created ")

    logging.info("Create a new collection")
    collection_FitBit = FitBitData["fitbit_table"]
    logging.info("new collection  created ")

    logging.info("Insert json data into collection")
    FitBitData.insert_many(convertedintojson)
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
    logging.info("Creating a new table Fitbitdata")
    #cursor.execute("create table sqlassignment.FitBitdata(Dress_ID int(20), Style varchar(50), Price varchar(50), Rating FLOAT(5.5), Size varchar(50), Season varchar(50), NeckLine varchar(50), SleeveLength varchar(50), waiseline varchar(50), Material varchar(50), FabricType varchar(50), Decoration varchar(50), PatternType varchar(50), Recommendation int(10))")
    cursor.execute("create table sqlassignment.FitBitdata(Id DECIMAL(38, 0) , ActivityDate text , TotalSteps DECIMAL(38, 0) ,TotalDistance DECIMAL(38, 17) ,TrackerDistance DECIMAL(38, 17) ,LoggedActivitiesDistance DECIMAL(38, 15) ,VeryActiveDistance DECIMAL(38, 17) ,ModeratelyActiveDistance DECIMAL(38, 16) ,LightActiveDistance DECIMAL(38, 17) ,SedentaryActiveDistance DECIMAL(38, 17) ,VeryActiveMinutes DECIMAL(38, 0) ,FairlyActiveMinutes DECIMAL(38, 0) ,LightlyActiveMinutes DECIMAL(38, 0) ,SedentaryMinutes DECIMAL(38, 0) ,Calories DECIMAL(38, 0) )")
    #cursor.execute("create table sqlassignment.FitBitdata1(Id DECIMAL(38, 0) , ActivityDate int , TotalSteps DECIMAL(38, 0) ,TotalDistance DECIMAL(38, 17) ,TrackerDistance DECIMAL(38, 17) ,LoggedActivitiesDistance DECIMAL(38, 15) ,VeryActiveDistance DECIMAL(38, 17) ,ModeratelyActiveDistance DECIMAL(38, 16) ,LightActiveDistance DECIMAL(38, 17) ,SedentaryActiveDistance DECIMAL(38, 17) ,VeryActiveMinutes DECIMAL(38, 0) ,FairlyActiveMinutes DECIMAL(38, 0) ,LightlyActiveMinutes DECIMAL(38, 0) ,SedentaryMinutes DECIMAL(38, 0) ,Calories DECIMAL(38, 0) )")
    logging.info("table created")

    for i,row in df.iterrows() :
        sql = "insert into sqlassignment.FitBitdata values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

        cursor.execute(sql,tuple(row))
        mydb.commit()
    logging.info("Data successfully inserted into FitBitdata table")

except Exception as e:
    print("Error while inserting records to FitBitdata table", e)

logging.info("Select all rows from FitBitdata table")
sql = "SELECT * FROM sqlassignment.FitBitdata"
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
