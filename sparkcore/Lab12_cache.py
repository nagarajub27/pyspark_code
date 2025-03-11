from pyspark import SparkContext
from pyspark import StorageLevel
def main():
    
    """
    24-01-2022
    """
    sc = SparkContext(appName="Lab11")
    sc.setLogLevel("ERROR")
    
    tdata = sc.textFile("file:/home/hduser/hive/data/txns")
    
    tdata1 = tdata.map(lambda line : line.split(","))
    
    tdata1.cache()
    
    tdata2 = tdata1.filter(lambda arr : arr[7] == "California" and arr[8] == "cash")
    
    #tdata2.cache()
    #or
    #tdata2.persist(StorageLevel.useMemory)
    #or
    tdata2.persist()
    
    print(f"No of tranaction in california with payment as cash: {tdata2.count()}")
    
    
    tdata3 = tdata2.map(lambda row : [row[0],row[3],row[6],row[7]])
    
    tdata3.foreach(print)
    
    
    tdata2.unpersist()
    
    tdata4 = tdata1.filter( lambda arr : arr[7] == "Texas" and arr[8] == "credit")
    
    print(f"No of tranaction in texas with payment as credit: {tdata4.count()}")
    
    tdata5 = tdata4.map(lambda row : [row[0],row[3],row[6],row[7]])
    
    tdata5.foreach(print)
    
    
main()
    