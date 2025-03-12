from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    spark = SparkSession.builder.appName("Lab12").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    custdf = spark.read.format("csv") \
        .option("delimiter", ",") \
        .option("inferSchema", True) \
        .load("file:/home/hduser/hive/data/custs") \
        .toDF("custid", "fname", "lname", "age", "prof")

    # select * from customer where prof = 'Pilot' and age > 50
    # custdf.filter(" prof = 'Pilot' and age > 50").show()

    custdf.createOrReplaceTempView("tblcustomer")

    """
    df = spark.sql("select * from tblcustomer where prof = 'Pilot' and age > 50")

    df1 = df.select("custid","fname","age")

    df1.show()
    """
    
    spark.sql("select * from tblcustomer where prof = 'Pilot' and age > 50").select("custid","fname","age").show()

    student = spark.read.format("csv").option("inferschema", True).load("file:/home/hduser/sparkdata/student.csv") \
        .toDF("studid", "studname")

    mark = spark.read.format("csv").option("inferschema", True).option("delimiter", "~") \
        .load("file:/home/hduser/sparkdata/mark.csv").toDF("studid", "mark1", "mark2", "mark3")

    student.createOrReplaceTempView("tblstudent")

    mark.createOrReplaceTempView("tblmark")

    spark.sql("select * from tblstudent s inner join tblmark m on s.studid = m.studid").show()

    spark.sql("select prof, count(custid) as customercount from tblcustomer group by prof").show()

main()