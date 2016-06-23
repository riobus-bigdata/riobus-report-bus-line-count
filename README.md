#riobus-report

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
<br>
<h6>now you need to submit this jar to spark</h6>
<ol>
	<li>
		For example, use the command:
		spark-submit --class "RiobusReportBusLineCout" --master local[2] target/scala-2.10/riobusuc2_2.10-1.0.jar NUMBER_OF_OUTPUT_SAMPLES START_DATE END_DATE
	</li>
</ol>



