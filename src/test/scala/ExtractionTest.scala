import org.scalatest.{FlatSpec, Matchers}
import java.util.UUID

class ExtractionTest extends FlatSpec with Matchers {

	"Parsing JSON formatted source data" must "retrieve 72 records" in {
		assert(ExtractData.allSensorDataJson.size === 72)
	}

	"Parsing CSV formatted source data" should "retrieve 72 records" in {
		assert(ExtractData.allSensorDataCSV.length === 72)
	}

	val targetSensor = UUID.fromString("b08c6195-8cd9-43ab-b94d-e0b887dd73d2")

	"Data for a single sensor" should "have 24 values" in {
		assert(ExtractData.sensorData(targetSensor).length === 24)
	}

	"Single sensor daily consumption" should "evaluate to correct amount" in {
		assert(ExtractData.aggregateDailyConsumptionkWh(targetSensor) === 10.713)
	}

	"Average consumption during night" should "evaluate to < 1 kwH" in {
		assert(ExtractData.averageConsumptionForPeriod((0 to 7), targetSensor) === 0.322875)
	}

	"Average consumption from morning to afternoon" should "evaluate to < 0.5 kwH" in {
		assert(ExtractData.averageConsumptionForPeriod((8 to 15), targetSensor) === 0.44925)
	}

	"Average consumption for the evening" should "evaluate to just > 0.5kwH" in {
		assert(ExtractData.averageConsumptionForPeriod((16 to 23), targetSensor) === 0.567)
	}
}