package inspection
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._

class Inspector(spark: SparkSession){

	def extractInfo(anomaliesFile: String, nbAnomalies: Int, rowNumber: Int,  eType: String, interval: Duration):(String,Long,Long)={
		if(rowNumber >= nbAnomalies){
			println("row number is "+rowNumber+" but must be between 0 and "+(nbAnomalies-1))
			System.exit(1)
		}
		val anomalies = spark.read.parquet(anomaliesFile)//should always be small enough to be able to collect
		val entityIndex = anomalies.columns.indexOf(eType+"entity")
		val timeIndex = anomalies.columns.indexOf("timeinterval")
		val row = anomalies.collect()(rowNumber)
		val entity = row.getString(entityIndex)
		val beginTimestamp = (row.getDouble(timeIndex)).toLong
		val endTimeStamp = beginTimestamp+interval.toMillis
		(entity, beginTimestamp, endTimeStamp)
	}
}