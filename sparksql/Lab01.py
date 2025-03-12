from pyspark import SparkContext
from pyspark.sql import SQLContext


def main():
    sc = SparkContext(master="local", appName="Lab01-SQL")
    sc.setLogLevel("ERROR")
    
    #Step-1: Create SQLContext object
    sqlc = SQLContext(sc)
    
    #Step-2: Create dataframe using sqlc(SQLContext)
    df = sqlc.read.format("csv").load("file:/home/hduser/hive/data/custs")
    
    #display 20 records by default
    
    #df.show()
    
    #df.show(numRows=50)
    
    #df.show(50)
    
    #by default, truncate=true
    
    #df.show(numRows=50,truncate=false)
    
    df.show(50, False)
    
    
    
    
main()