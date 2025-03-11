
"""
19-Jan-2022
Given filename student.csv and marks.csv

student.csv
1,Lokesh
2,Bhupesh
3,Amit
4,Ratan
5,Dinesh

marks.csv
1~90~80~95
2~88~90~89
3~78~76~70
4~92~69~89
5~88~70~86


Accomplish the followings:-

1. Load the files into RDD and get the studentid,studentname,mark1,mark2,marks3,totalmarks
2. Get the highest and lowest totalmarks scored  

tblstudent
-studid
-studname

tblmark
-studid
-mark1
-mark2
-mark3

//Highest Mark
select * from (
select s.studid,s.name,m.mark1,m.mark2,m.mark3,(m.mark1 + m.mark2 + m.mark3) as totalmarks 
from tblstudent s join tblmarks m on s.studid = m.studid ) A order by totalmarks desc limit 1

//Lowest Mark
select * from (
select s.studid,s.name,m.mark1,m.mark2,m.mark3,(m.mark1 + m.mark2 + m.mark3) as totalmarks 
from tblstudent s join tblmarks m on s.studid = m.studid ) A order by totalmarks limit 1


 """
 
from pyspark import SparkContext

def main():
    sc = SparkContext(appName="Lab09")
    sc.setLogLevel("ERROR")
    
    student = sc.textFile("file:/home/hduser/sparkdata/student.csv")
    
    student1 = student.map(lambda line : line.split(","))
    
    student2 = student1.map(lambda line : (line[0],line[1]))
    
    student.foreach(print)
      
    print("===============")
    
    student2.foreach(print)
    
    mark = sc.textFile("file:/home/hduser/sparkdata/mark.csv")
    
    mark1 = mark.map(lambda line : line.split("~"))
    
    mark2 = mark1.map(lambda line : (line[0],(line[1],line[2],line[3])))
    
    print("======Mark data =========")
      
    mark.foreach(print)
      
    print("===============")
      
    mark2.foreach(print)
      
    studentmark = student2.join(mark2)
      
    print("======Join=========")
      
    studentmark.foreach(print)
    
    studentmark1 = studentmark.map(lambda data : (data[0],data[1][0],int(data[1][1][0]),int(data[1][1][1]),int(data[1][1][2])))
    
    print("=================")
        
    studentmark1.foreach(print)
    
      
    studentmark2 = studentmark1.map(lambda s : (s[0], s[1], s[2], s[3], s[4], s[2] + s[3] + s[4]))
      
    print("=================")
      
    studentmark2.foreach(print)
      
    studentmarksort = studentmark2.sortBy(lambda x : x[5],False,1)
      
    maxstudent = studentmarksort.first()
      
    print(f"Maximum Mark scored Student Info: {maxstudent}")
      
      
    studentmarksort1 = studentmark2.sortBy(lambda x : x[5],True,1)
      
    minstudent = studentmarksort1.first()
      
    print("=================")
       
    print(f"Minimum Mark scored Student Info: {minstudent}")

    
main()
    
    