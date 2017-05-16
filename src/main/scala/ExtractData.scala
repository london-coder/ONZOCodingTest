import java.time.ZonedDateTime
import java.util.UUID
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
	case class Record(sensor_id: UUID, timestamp: ZonedDateTime, consumption_Wh: Int)

	// for a production system, these would be either runtime parameters or
	// perhaps read from a stream.
	private val csvFileName = "src/main/resources/consumption_data.csv"
	private val jsonFileName = "src/main/resources/consumption_data.json"
	// this 'caches' the data from the file for convenience, as it's reused
	// for both json and csv formatted data.
	private var sensorDataCollection: Seq[Record] = List()

	private def readDataFromCSV: Seq[Record] = {
		// drop 1st row from data file as it contains only column names and not values.
		val data = fromFile(csvFileName).getLines.toList.map(_.split(" ")).drop(1) 
		for {
			l <- data
			v = l(0).split(",")
		} yield Record(UUID.fromString(v(0)), ZonedDateTime.parse(v(1)), v(2).toInt)
	}

	implicit lazy val formats = org.json4s.DefaultFormats

	// TODO:  create a formatter to convert String to ZonedDateTime. That would
	// eliminate the need to convert between 2 case classes!
	private def readDataFromJson: Seq[Record] = {
		val json = parse(fromFile(jsonFileName).mkString).extract[List[RawRecord]]
		for {
			r <- json
			rec = r match {
				case RawRecord(s,t,k) => Record(UUID.fromString(s), ZonedDateTime.parse(t), k)
			}
		} yield rec
/*		
		val JArray(list) = json
		for {
			record <- list
			JString(sensor) = record \ "sensor_id"
			JString(timestamp) = record \ "timestamp"
			JLong(wh) = record \ "consumption_Wh"
		} yield Record(UUID.fromString(sensor), ZonedDateTime.parse(timestamp), wh.toInt)
*/
	}

	def allSensorDataCSV: Seq[Record] = {
		if (sensorDataCollection.isEmpty) sensorDataCollection = readDataFromCSV
		sensorDataCollection
	}

	def allSensorDataJson: Seq[Record] = {
		if (sensorDataCollection.isEmpty) sensorDataCollection = readDataFromJson
		sensorDataCollection
	}

	def sensorData(sensor: UUID): Seq[Record] = {
		if(sensorDataCollection.isEmpty) sensorDataCollection = readDataFromCSV
		sensorDataCollection filter { case Record(s,t,k) => sensor.equals(s) }
	}

	def aggregateDailyConsumptionkWh(sensor: UUID): BigDecimal = 
		BigDecimal((sensorData(sensor).map{ case Record(s,t,k) => k }.sum.toDouble)/1000).setScale(3, RoundingMode.UP)

	def averageConsumptionForPeriod(duration: Range, sensor: UUID): BigDecimal = {
		val bySensor = sensorData(sensor) filter { case Record(s,t,k) => duration.contains(t.getHour()) }
		BigDecimal(((bySensor map { case Record(s,t,k) => k } sum).toDouble / bySensor.length)/1000 ).setScale(6, RoundingMode.UP)
	}
}