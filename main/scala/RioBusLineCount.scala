
import java.io._
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkConf, SparkContext}

object RiobusReportBusLineCount {
	val conf = new SparkConf().setAppName("RiobusReportBusLineCount")//.setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)

    // TODO: set this up as environment variable
	val path = "hdfs://127.0.0.1:9000/"
	val filenameAndPath = path + "/riobusData/estudo_cassio_part_0000000000[0-1][0-9].csv" // path to file being read.
	// val filenameAndPath = path + "/riobusData/estudo_cassio_part_000000000000.csv" // path to file being read.
	val resultFilenameAndPath = "/riobusResult/bus-line-result.txt" // path to file that will be written.

	val dateFormatGoogle = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'") // format used by the data we have.
	val dateFormathttp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss") // format we use inside http message.

	def main(args: Array[String]) {

		var sampleLength = 5
		var dateBegin: java.util.Date = new Date();
		var dateEnd: java.util.Date = new Date();

		// Parsing the command line arguments by the number of arguments the user selected.
		// if no arguments, we will use the default values
		// if 1 argument, we will assume it is the sampleLength
		// if 2 arguments, we will asumme they are the dateBegin and dateEnd
		// if 3 or more arguments, we will assume they are the sampleLength, dateBegin and dateEnd
		if (args.length == 1){
			sampleLength = args(0).toInt
		} else if(args.length == 2) {
			dateBegin = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'")).parse(args(0))
			dateEnd = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'")).parse(args(1))
		} else if(args.length >= 3) {
			sampleLength = args(0).toInt
			dateBegin = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'")).parse(args(1))
			dateEnd = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'")).parse(args(2))
		}

		// Defining functions instead of methods. I am doing it because 'myApp' is an object, not a class, so I don't have
		// a constructor for it. That means I can't use the arguments 'args' out of 'main' method.

		// returns true if 'stringDate', converted to Date object is bigger than 'dateBeggin' and smaller than 'dateEnd'.
		val isdateInsideInterval = {(stringDate: String) =>
			//converting string to a date using pattern inside 'dateFormatGoogle'.
			var date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'")).parse(stringDate)
			// appearenlty I can't use the 'dateFormatGoogle' because 'SimpleDateFormat' is not thread safe. I need a
			// new instance for every thread, but I don't know how to do it in an spark application. That's why I create a
			// new instance inside every function call.

			// testing if date is inside date interval.
			if (args.length >= 2)
				date.compareTo(dateBegin) >= 0 && date.compareTo(dateEnd) <= 0
			else
				true // app called withou start date and end date param, returning true
		}

		// Code that will implement the bus line count. It will filter every register 
		// inside a date interval and count all the buses in the same bus line.

		// reading text file with 2 copies, then caching on memory.
		val busLineCount = sc.textFile(filenameAndPath, 2).cache()
			// spliting each line by commas.
			// Mapping file to (bus line, (bus id, datetime))
			.map(x => ( if (x.split(",")(2) == "") "undefined" else x.split(",")(2), (x.split(",")(1),x.split(",")(0)) ) )

			// Filtering by time frame, from dateBegin to dateEnd
			.filter(x => isdateInsideInterval(x._2._2) )

			// Re-mapping RDD to (bus line, (bus id, 1)), so we can use the distinct function
			// to weed out the same bus in a line
			.map(x => (x._1,(x._2._1,1)))
			.distinct()

			// Counting all the diferent buses in a bus line
			.reduceByKey( (x,y) => ("", x._2 + y._2) )

		// This is necessary to write the file to hdfs
		val conf = new Configuration()
	    conf.set("fs.defaultFS", path)
	    val fs = FileSystem.get(conf)
	    val output = fs.create(new Path(resultFilenameAndPath))

		// we will need to write in a file the argument we have received (as a confirmation), the size of the result and 
		// a sample of it, of a small size.
		val pw = new PrintWriter(output) // creating file to be written on.

		busLineCount.take(sampleLength).foreach(x => pw.write(x._1 + ": " + x._2._2.toString + "\n")) // writing the first records.
		pw.close // closing file.
	}
}