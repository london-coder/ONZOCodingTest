import java.time.ZonedDateTime

import scala.io.Source._
import scala.language.postfixOps
import scala.math.BigDecimal.RoundingMode

import org.json4s._
import org.json4s.native.JsonMethods._
/**
 * This is NOT a production quality application. It is ONLY meant to serve as
 * an exercise in solving a problem in the context of an interview coding test.
 * It meets that criterion and ONLY that criterion. It is done with someone
 * 'looking over your shoulder' in a short, timed exercise to understand candidate
 * thinking, approach to solving a problem and understanding of Scala, no more.
 */
object ExtractData {
	case class RawRecord(sensor_id: String, timestamp: String, consumption_Wh: Int)
	case class Record(sensor_id: String, timestamp: ZonedDateTime, consumption_Wh: Int)

	// for a production system, these would be either runtime parameters or
	// perhaps read from a stream.
	private val csvFileName = "src/main/resources/consumption_data.csv"
	private val jsonFileName = "src/main/resources/consumption_data.json"
	// this 'caches' the data from the file for convenience, as it's reused
	// for both json and csv formatted data.
	private var sensorDataCollection : Seq[Record] = List()

	// The sensor ID, (first field), is a UUID, but a string is used 
	// for the purpose of the exercise. It should take little effort time to 
	// change the String to a java.util.UUID
	private def readDataFromCSV: Seq[Record] = {
		// drop 1st row from data file as it contains only column names and not values.
		val data = fromFile(csvFileName).getLines.toList.map(x => x.split(" ")).drop(1)
		for {
			l <- data 
			v = l(0).split(",")
		} yield Record(v(0), ZonedDateTime.parse(v(1)), v(2).toInt)
	}

	implicit lazy val formats = org.json4s.DefaultFormats
	// TODO:  create a formatter to convert String to ZonedDateTime. That would
	// eliminate the need to convert between 2 case classes!

	private def readDataFromJson: Seq[Record] = {
		var json = parse(fromFile(jsonFileName).mkString)
		// this can be fixed with implicit format function
		val something = json.extract[List[RawRecord]] 
		for {
			r <- something
			rec = r match {
				case RawRecord(i,t,kw) => Record(i, ZonedDateTime.parse(t), kw)
			}
		} yield rec
	}

	def allSensorDataCSV: Seq[Record] = {
		if (sensorDataCollection.isEmpty) sensorDataCollection = readDataFromCSV
		sensorDataCollection
	}

	def allSensorDataJson: Seq[Record] = {
		if (sensorDataCollection.isEmpty) sensorDataCollection = readDataFromJson
		sensorDataCollection
	}

	def sensorData(sensor: String): Seq[Record] = {
		if(sensorDataCollection.isEmpty) sensorDataCollection = readDataFromCSV
		sensorDataCollection filter { case Record(s,t,k) => s.equals(sensor) }
	}

	def aggregateDailyConsumptionkWh(sensor: String): BigDecimal = 
		BigDecimal((sensorData(sensor).map{ case Record(s,t,k) => k }.sum.toDouble)/1000).setScale(3, RoundingMode.UP)

	def averageConsumptionForPeriod(duration: Range, sensor: String): BigDecimal = {
		val bySensor = sensorData(sensor) filter { case Record(id, dt, kw) => duration.contains(dt.getHour()) }
		BigDecimal(((bySensor map { case Record(s, d, k) => k } sum).toDouble / bySensor.length)/1000 ).setScale(6, RoundingMode.UP)
	}
}