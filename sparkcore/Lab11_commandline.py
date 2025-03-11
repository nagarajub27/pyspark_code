from pyspark import SparkContext
import sys

def main():
    
    """
    21-Jan-2022
    
    Local Mode:
    
    export PYSPARK_PYTHON=/bin/python3
    spark-submit --master local 
    file:/home/hduser/workspacespark/Inceptezpyspark-27/sparkcore/Lab11_commandline.py hdfs://localhost:54310/user/hduser/coursefees.txt 2
    
    Standalone Mode client with driver core and memory:
    
    spark-submit 
    --master spark://localhost:7077 
    --deploy-mode client
    --executor-cores 1 
    --executor-memory 512M 
    --total-executor-cores 1 
    --driver-memory 512m 
    --driver-cores 1 file:/home/hduser/workspacespark/Inceptezpyspark-27/sparkcore/Lab11_commandline.py hdfs://localhost:54310/user/hduser/coursefees.txt 2
    
    
    Yarn-client mode:
    
    spark-submit --master yarn 
    --executor-cores 1 
    --executor-memory 512M 
    --num-executors 1 file:/home/hduser/workspacespark/Inceptezpyspark-27/sparkcore/Lab11_commandline.py hdfs://localhost:54310/user/hduser/coursefees.txt 2
    
    
    Yarn-cluster mode:

    spark-submit --master yarn 
    --deploy-mode cluster 
    --executor-cores 1 
    --executor-memory 512M 
    --num-executors 1 file:/home/hduser/workspacespark/Inceptezpyspark-27/sparkcore/Lab11_commandline.py hdfs://localhost:54310/user/hduser/coursefees.txt 2
    
    """
    if(len(sys.argv) > 2):
    
        filepath = sys.argv[1]
        partitions = int(sys.argv[2])
        
        sc = SparkContext(appName="Lab11")
        sc.setLogLevel("ERROR")
        
        rdd = sc.textFile(filepath,partitions)      
        rdd.foreach(print)
        
        datalst = rdd.collect()
        
        print(datalst)
        
    else:
        print("Invalid Input")

main()