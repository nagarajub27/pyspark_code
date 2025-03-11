from pyspark import SparkContext
def main():
    
    """
    25-01-2022
    """
    sc = SparkContext(appName="Lab11")
    sc.setLogLevel("ERROR")
    
    custrdd = sc.textFile("file:/home/hduser/hive/data/custs")
     
    txnsrdd = sc.textFile("file:/home/hduser/hive/data/txns")
     
    custdata = custrdd.map(lambda x : x.split(",")).filter(lambda x : len(x) == 5).map(lambda x : (x[0],x[4]))
     
    custdata1 = custdata.collectAsMap()
     
    bccustdata = sc.broadcast(custdata1)
     
    txnsrdd1 = txnsrdd.map(lambda x : x.split(",")).map(lambda x : (x[2],x[7])).filter(lambda x : x[1].lower() == "california")
     
    #val joindata = txnsrdd1.join(custdata)
     
    joindata = txnsrdd1.map(lambda trans : (trans[0],trans[1],bccustdata.value[trans[0]]))
     
    joindata.foreach(print)
    
main()