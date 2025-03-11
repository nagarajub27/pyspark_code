from pyspark import SparkContext

def main():
    
    sc = SparkContext(master="local", appName="Lab05")
    sc.setLogLevel("ERROR")
    
    rdd1 = sc.textFile("file:/home/hduser/course")
    
    """
      rdd1 = RDD(
      "hadoop spark python java"
            "hive sqoop spark java"
            "scala java spark"
            "hbase kafka java"
            )
      """
    
    
    rdd2 = rdd1.map(lambda line : line.split(" "))
 
     
    """
     rdd2 =  RDD(
      Array("hadoop","spark","python","java")
            Array("hive"," sqoop","spark","java")
            Array("scala","java","spark")
            Array("hbase","kafka","java")
            )
     """
     
    rdd3 = rdd2.flatMap(lambda arr : arr)

    """
     rdd3 =  RDD(
      "hadoop"
      "spark"
      "python"
      "java"
      "hive"
      "sqoop"
      "spark"
      "java"
        "scala"
        "java"
        "spark"
        "hbase"
        "kafka"
        "java")
            )
    """
     
    rdd4 = rdd3.map(lambda word : (word,1))
     
    """
     rdd4 =  RDD(
      ("hadoop",1)
      ("spark",1)
      ("python",1)
      ("java",1)
      ("hive",1)
      ("sqoop",1)
      ("spark",1)
      ("java",1)
            ("scala",1)
            ("java",1)
            ("spark",1)
            ("hbase",1)
            ("kafka",1)
            ("java",1)
            )
                        
    """

     
    rdd5 = rdd4.reduceByKey(lambda a,b : a + b)
     
    """
      rdd5 = RDD(
      task2
      ("hadoop",1)
      ("hbase",1)
      ("hive",1)
      ("java",4)
      
      task3
      ("kafka",1)
      ("python",1)
      ("scala",1)
      ("spark",3)
      )
    """
    
    rdd5.foreach(print)
    
main() 
    
    