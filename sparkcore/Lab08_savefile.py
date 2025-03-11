from pyspark import SparkContext

def main():
    
    #sc = SparkContext(master="local", appName="Lab08")
    
    sc = SparkContext(appName="Lab08")
    sc.setLogLevel("ERROR")
    cfees = sc.textFile("hdfs://localhost:54310/tmp/coursefees.txt")

    #val cfees = sc.textFile("/tmp/coursefees.txt")

    header = cfees.first()

    cfees1 = cfees.filter(lambda x : x != header)

    cfees2 = cfees1.map(lambda x : x.split("$"))

    cfees3 = cfees2.map(lambda x : (x[0], int(x[1]), int(x[2])))

    cfees4 = cfees3.map(lambda x : (x[0],x[1],x[2], x[1] * x[2] /100, x[1] * x[2] /100 + x[1]))

    cfees5 = cfees4.map(lambda x : x[0] + "|" + str(x[1]) + "|" + str(x[2]) + "|" + str(x[3]) + "|" + str(x[4]))

    headerdd = sc.parallelize(["CourseName|Fees|TaxInPer|Taxamount|TotalFees"])

    frdd = headerdd.union(cfees5)

    frdd1 = frdd.repartition(1)

    
    #val frdd1 = frdd.coalesce(1)

    #frdd1.saveAsTextFile("hdfs://localhost:54310/sparkworkouts/coursefeewithtax3")
    
    frdd1.foreach(print)
    
    print("Data written into hdfs")
    

main()    
    