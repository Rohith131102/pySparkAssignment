import requests
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace
from pyspark.sql.functions import col
# import json


def getData():
	url = "https://covid-19-india2.p.rapidapi.com/details.php/"
	headers = {
		"X-RapidAPI-Key": "a38e317e27msh0fc8c330812f216p1a62f4jsn6f9077e5c4d8",
		"X-RapidAPI-Host": "covid-19-india2.p.rapidapi.com"
	}
	response = requests.request("GET", url, headers=headers)
	return response

def cleanData(df):
    df=df.drop('_corrupt_record') # droping the _corrupt_record column
    df=df.where((df.state.isNotNull()) & (df.state!='')) #removing the columns where state is null 
    # Casting the columns 
    df = df.withColumn("confirm",col("confirm").cast("Long")).withColumn("cured",col("cured").cast("Long")).withColumn("death",col("death").cast("Long"))
    df=df.withColumn('state', regexp_replace('state', '\*',""))	 # Striping state names who has * st their ends
    df=df.select("slno","state","confirm","cured","death","total")	#rearranging the data

    return df # returning the cleaned dataframe

spark = SparkSession.builder.master('local[*]').getOrCreate() # Creating a spark session
sc = SparkContext.getOrCreate() # creating a Spark context
sc.setLogLevel("ERROR")




spark = SparkSession.builder.master('local[*]').getOrCreate() # Creating a spark session
sc = SparkContext.getOrCreate() # creating a Spark context
sc.setLogLevel("ERROR")
response=getData() 
json_rdd = sc.parallelize(response.json().values()) # creating rdd from responsed
df = spark.read.json(json_rdd) # creating the dataframe covidData
covidData=cleanData(df) # cleaning the dataframe pi
