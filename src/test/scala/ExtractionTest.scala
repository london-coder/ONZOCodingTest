import org.scalatest.{FlatSpec, Matchers}


class ExtractionTest extends FlatSpec with Matchers {


	"A test parse of the JSON source data" must "produce valid record collection" in {
		// assert(ExtractData.allSensorData.size === 72)
	}

	"Reading all sensor data" should "retrieve 72 records" in {
		assert(ExtractData.allSensorData.length === 72)
	}

	val targetSensor = "b08c6195-8cd9-43ab-b94d-e0b887dd73d2"

	"Data for a single sensor" should "have 24 values" in {
		assert(ExtractData.sensorData(targetSensor).length === 24)
	}

	"Total daily consumption" should "evaluate to correct amount" in {
		assert(ExtractData.aggregateDailyConsumptionkWh(targetSensor) === 10.713)
	}

	"Average consumption during night" should "evaluate to < 1 kwH" in {
		assert(ExtractData.averageConsumptionForPeriod((0 to 7), targetSensor) === BigDecimal(0.322875))
	}

	"Average consumption from morning to afternoon" should "evaluate to < 0.5 kwH" in {
		assert(ExtractData.averageConsumptionForPeriod((8 to 15), targetSensor) === BigDecimal(0.44925))
	}

	"Average consumption for the evening" should "evaluate to just > 0.5kwH" in {
		assert(ExtractData.averageConsumptionForPeriod((16 to 23), targetSensor) === BigDecimal(0.567))
	}

}