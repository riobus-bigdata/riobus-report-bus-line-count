#riobus-report-bus-line-count

spark project written in scala that reads bus registers from csv files saved on hadoop and filters bus registers inside a given inside a date interval and calculates the amount of buses in each existing bus line. As output, it will write to a file locally, which path is also given as argument to this application. The first line will contain the filter arguments and from the second line forwards it will contain a pair [bus line, amount of buses] separated by comma.

<h6>you only need to produce a jar</h6>
<ol>
    <li>install sbt (simple build tool)<br>
    more information in http://www.scala-sbt.org/0.13/tutorial/Setup.html<br>
    <li>cd to project folder <br>
    <code>$ cd path/to/project</code></li>
    <li>use pacakge command with sbt <br>
    <code>$ sbt package</code></li>
    <li>jar will be inside ./target/scala-2.10/ folder<br>
</ol>

<h6>now you need to submit this jar to spark<br></h6>
read <https://spark.apache.org/docs/latest/submitting-applications.html> for more information<br>
this project has been tested on spark 1.6.1
you can submit this job like this

    path/to/spark-submit 
    --driver-memory 1536m 
    --class "<class name>" 
    --master local[*] 
    path/to/project/jar
    <out file>
    <date begin>
    <date end>

where
	
* `<class name>` is the projects main class name.
* `<out file>` is the path to the output file this job will write it's result into.
* `<date begin>` is date in the format yyyy-MM-dd'T'HH:mm:ss (eg: 2015-04-14T13:00:00) that is the start of the interval.
* `<date end>` is date in the format yyyy-MM-dd'T'HH:mm:ss (eg: 2015-04-14T15:00:00) that is the end of the interval.