from pyspark import SparkContext

def main():
    
    sc = SparkContext(master="local", appName="Lab04")
    sc.setLogLevel("ERROR")
    tdata = sc.textFile("file:/home/hduser/hive/data/txns")
    
    tdata1 = tdata.map(lambda line : line.split(","))
    
    tdata2 = tdata1.filter(lambda arr : arr[7] == "California" and arr[8] == "cash")
    
    #select * from tbltrans where state='California' and spendby='cash'
    tdata2.foreach(print)
    
    
    #tdata3 = tdata2.map(lambda row : [row[0],row[3],row[6],row[7]])
    
    #select txnid,amount,city,state from tbltrans where state='California' and spendby='cash'
    #tdata3.foreach(print)


main()














