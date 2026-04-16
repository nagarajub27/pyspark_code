from pyspark import SparkContext

def main():

    sc = SparkContext(master = 'local[1]',appName = 'lab2')
    sc.setLogLevel('ERROR')

    #READ From file
    rdd = sc.textFile('hands-on/datafiles/emp.txt')
    rdd.foreach(print)

main()