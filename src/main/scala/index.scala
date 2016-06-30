import org.apache.spark.{SparkConf, SparkContext}
import java.io._
import java.util.Date
import java.text.SimpleDateFormat

object BusLineCount {
	val conf = new SparkConf().setAppName("BusLineCount")//.setMaster("spark://localhost:7077")
    val sc = new SparkContext(conf)

	// path to files being read.
	val filenameAndPath = "hdfs://localhost:8020/riobusData/estudo_cassio_part_00000000000[0-19]*"

	val dateFormatGoogle = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'") // format used by the data we have.
	val dateFormathttp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss") // format we use inside http message.

	def main(args: Array[String]) {

		val resultFilenameAndPath = args(0) // path to file that will be written.

		val dateBegin = dateFormathttp.parse(args(1))
		val dateEnd = dateFormathttp.parse(args(2))

		// Defining functions instead of methods. I am doing it because 'BusLineCount' is an object, not a class, so
		// I don't have a constructor for it. That means I can't use the arguments 'args' out of 'main' method.

		// returns true if 'stringDate', converted to Date object is bigger than 'dateBegin' and smaller than 'dateEnd'.
		val isdateInsideInterval = {(stringDate: String) =>
			//converting string to a date using pattern inside 'dateFormatGoogle'.
			var date = (new SimpleDateFormat("yyyy-MM-dd HH:mm:ss 'UTC'")).parse(stringDate)
			/* apparently I can't use the 'dateFormatGoogle' because 'SimpleDateFormat' is not thread safe. I need a
			new instance for every thread, but I don't know how to do it in an spark application. That's why I create a
			new instance inside every function call. */
			date.compareTo(dateBegin) >= 0 && date.compareTo(dateEnd) <= 0 // testing if date is inside date interval.
		}

		// Code that will implement the bus line count. It will filter every register 
		// inside a date interval and count all the buses in the same bus line.

		// reading text file with 2 copies, then caching on memory.
		val busLineCount = sc.textFile(filenameAndPath)
			// spliting each line by commas.
			// Mapping file to (bus line, (bus id, datetime))
			.map(x => ( if (x.split(",")(2) == "") "undefined" else x.split(",")(2), (x.split(",")(1),x.split(",")(0)) ) )

			// Filtering by time frame, from dateBegin to dateEnd
			.filter(x => isdateInsideInterval(x._2._2) )

			// Re-mapping RDD to (bus line, (bus id, 1)), so we can use the distinct function
			// to weed out the same bus in a line
			.map(x => (x._1,(x._2._1,1)))
			.distinct()

			// Counting all the different buses in a bus line
			.reduceByKey( (x,y) => ("", x._2 + y._2) )
			//bringing the whole rdd to master so we can write in one place
			.collect()

		// we will need to write in a file the argument we have received (as a confirmation) and the results
		val pw = new PrintWriter(new File(resultFilenameAndPath), "UTF-8") // creating file to be written on.
		// writing the arguments we have received. just to give a feedback.
		pw.write(args(1)+","+args(2)+ "\n")
		// writing all records (there shouldn't bee too many).
		busLineCount.foreach(x => pw.write(x._1 + "," + x._2._2.toString + "\n"))
		pw.close // closing file.
	}
}