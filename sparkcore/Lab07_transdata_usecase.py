from pyspark import SparkContext

def main():
    
    sc = SparkContext(master="local", appName="Lab05")
    sc.setLogLevel("ERROR")
    rdd = sc.textFile("file:/home/hduser/hive/data/txns")
    
    rdd1 = rdd.map(lambda line : line.split(","))
    
    #gettotalsalesintexas(rdd1)
    #getmaxsoldproductintexas(rdd1)
    
    getcountandamountbystate(rdd1)
    

def gettotalsalesintexas(rdd):

    rdd3 = rdd.filter(lambda line : "texas" in (line[7].lower()))
     
    rdd4 = rdd3.map(lambda line : float(line[3]))
     
    totalsales = rdd4.sum()
     
    print("Total sales in Texas: " , totalsales)


def getmaxsoldproductintexas(rdd):
  
    #select count(txnid),product from txns where state = 'Texas' group by product order by count(txnid) by desc limit 1
    
    rdd1 = rdd.filter(lambda line : "texas" in (line[7].lower()))
    
    rdd2 = rdd1.map(lambda row : (row[5],1))
    
    rdd3 = rdd2.reduceByKey(lambda x,y : x + y)
    
    rdd4 = rdd3.sortBy(lambda x : x[1],False,1)
    
    maxcountrec = rdd4.first()
    
    print(f"Product {maxcountrec[0]} has maximum sales of {maxcountrec[1]}")
    

def getcountandamountbystate(rdd):
    
    """select state,sum(amount),count(txnid)  from txns group by state
    /*
     ("California",(45.25,1))
     ("California",(20.25,1))
     ("California",(50.25,1))
     
     ("California",((45.25,1),(20.25,1) (50.25,1))
     
     x = (45.25,1)
     y = (20.25,1)
     
     x._1 + y._1 = 45.25 + 20.25 = 65.50
     
     x._2 + y._2 = 1 + 1 = 2
     
     x = (65.50,2)
     
     y = (50.25,1)
     
     x._1 + y._1 = 65.50 + 50.25 = 115.75
     x._2 + y._2 = 2 + 1 = 3
     
     (115.75,3)
     
     x = ("California",(115.75,3))
     
     x[0] = "California"
     x[1]= (115.75,3)
     
     x[1][0]
     x[1][1]
     """
    
    rdd1 = rdd.map(lambda x : (x[7],(float(x[3]),1)))
    
    """
     ("California",(45.25,1))
     ("Texas",(10,1))
     ("California",(50.25,1))
     ("California",(20.25,1))
     ("Texas",(20,1))
     
     ("California",((45.25,1),(20.25,1) (50.25,1))
     ("Texas",(10,1),(20,1))
     
     """
    
    rdd2 = rdd1.reduceByKey(lambda x,y : (x[0] + y[0], x[1] + y[1]))
    
    """
    ("California",(115.75,3))
    ("Texas",(30,2))
    
    """
    
    #rdd2.foreach(print)
    
    #select state,sum(amount),count(txnid),sum(amount)/count(txnid) as avgamt  from txns group by state
    
    rdd3 = rdd2.map(lambda x : (x[0],x[1][0],x[1][1], x[1][0] / x[1][1]))
    
    rdd3.foreach(print)
    
  
  
main()