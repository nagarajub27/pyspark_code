from pyspark import SparkContext

def main():
    
    #core vs partition vs task
    #By Default, number of partitions is based on number of cores
    #Based on the number of partitions, number of tasks get created
    
    sc = SparkContext(master="local", appName="Lab05")
    sc.setLogLevel("ERROR")
    
    tdata = sc.textFile("file:/home/hduser/hive/data/txns")
    
    
    tdata1 = tdata.map(lambda line : line.split(","))
    
    tdata2 = tdata1.filter(lambda arr : arr[7] == "California" and arr[8] == "cash")
    
    #tdata2.foreach(print)
    
    #lstdata = tdata2.collect()
    
    print(tdata2.getNumPartitions())
    
    
    
    #print(lstdata)

main()