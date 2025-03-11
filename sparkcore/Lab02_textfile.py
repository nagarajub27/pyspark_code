from pyspark import SparkContext

def main():
    sc = SparkContext(master="local", appName="Lab02")
    
    sc.setLogLevel("ERROR")
    
    #Read from one file
    fdata = sc.textFile("file:/home/hduser/sparkdata/employee.txt")
    fdata.foreach(print)
    
    print("===============")
    
    #Read from multiple files
    fdata1 = sc.textFile("file:/home/hduser/sparkdata/employee.txt,file:/home/hduser/sparkdata/emp")
    fdata1.foreach(print)
    
    print("=======================")
    
    #Read all the files from the folder
    fdata2 = sc.textFile("file:/home/hduser/sparkdata/sparkdata1")
    fdata2.foreach(print)
    
    
    #Read all the text files from the folder
    fdata3 = sc.textFile("file:/home/hduser/sparkdata/sparkdata1/*.txt")
    fdata3.foreach(print)
    
    #Read from multiple folder
    fdata4 = sc.textFile("file:/home/hduser/sparkdata/sparkdata1,file:/home/hduser/sparkdata/testdata")
    fdata4.foreach(print)
    
    print("=======================")
    
    #Read from hdfs filesystem
    fdata5 = sc.textFile("hdfs://localhost:54310/user/hduser/customerdata")
    fdata5.foreach(print)
    
    print("=======================")
    
    #Read from hdfs and local filesystem
    fdata6 = sc.textFile("hdfs://localhost:54310/user/hduser/customerdata,file:/home/hduser/sparkdata/sparkdata1")
    fdata6.foreach(print)
    
    print("=======================")
    """ By defalt takes form hadoop because spark contains by default hadoop librabries with out uri()"""
    #Read from hdfs filesystem without uri
    fdata7 = sc.textFile("/user/hduser/customerdata")
    fdata7.foreach(print)


main()