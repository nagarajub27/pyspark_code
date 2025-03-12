from pyspark.sql import SparkSession
from pyspark.sql.functions import *


def main():
    spark = SparkSession.builder.appName("Lab10").master("local").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")



    student = spark.read.format("csv").option("inferschema", True).load("file:/home/hduser/sparkdata/student.csv")\
        .toDF("studid", "studname")

    mark = spark.read.format("csv").option("inferschema", True).option("delimiter", "~")\
        .load("file:/home/hduser/sparkdata/mark.csv").toDF("studid", "mark1", "mark2", "mark3")

    # select * from student inner join mark on student.studid = mark.studid

    studentmark = student.join(mark, student["studid"] == mark["studid"], "inner")
    studentmark.show()

    # select student.studid,student.studname,mark.mark1,mark.mark2,mark.mark3 from student inner join mark on student.studid = mark.studid

    studentmark.select(student["studid"].alias("id"), mark["studid"], student["studname"]\
                       ,mark["mark1"],mark["mark2"],mark["mark3"]).show()

    # select student.* from student inner join mark on student.studid = mark.studid
    studentmark.select(student["*"]).show()

    # select * from student left outer join mark on student.studid = mark.studid
    student.join(mark, student["studid"] == mark["studid"], "left_outer").show()

    # select * from student right outer join mark on student.studid = mark.studid
    student.join(mark, student["studid"] == mark["studid"], "right_outer").show()

    # select * from student full outer join mark on student.studid = mark.studid
    student.join(mark, student["studid"] == mark["studid"], "outer").show()

    # select * from student left semi join mark on student.studid = mark.studid
    # list only left side table columns
    # list only the left side table rows that are matching with right side table rows

    # select * from student where studid in (select studid from mark)

    student.join(mark, student["studid"] == mark["studid"], "left_semi").show()

    # list only left side table columns
    # list only the left side table rows that are not matching with right side table rows
    # select * from student left semi join mark on student.studid = mark.studid

    # select * from student where studid not in (select studid from mark)
    student.join(mark, student["studid"] == mark["studid"], "left_anti").show()

    # cross join
    student.crossJoin(mark)





main()