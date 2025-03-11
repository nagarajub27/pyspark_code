from pyspark import SparkContext
def main():
    
    """
    25-01-2022
    """
    sc = SparkContext(appName="Lab11")
    sc.setLogLevel("ERROR")
    
    words = sc.broadcast(["of","the","is","an","and"])
     
    lst = ["learning spark is an intersting one","Hadoop is one of the bigdata storage and processing tool"]
     
    rdd = sc.parallelize(lst,4)
     
    rdd1 = rdd.flatMap(lambda x : x.split(" "))
         
    rdd1.foreach(print)
     
    rdd2 = rdd1.filter(lambda w :  w not in words.value)
     
    print("==========================")
     
    rdd2.foreach(print)


main()