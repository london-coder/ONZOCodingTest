import java.time.ZonedDateTime
import java.util.UUID

trait MeterData
	final case class RawRecord(sensor_id: String, timestamp: String, consumption_Wh: Int)
	final case class Record(sensor_id: UUID, timestamp: ZonedDateTime, consumption_Wh: Int)
