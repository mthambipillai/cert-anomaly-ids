package inspection
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._
import org.apache.spark.sql.DataFrame
import features._
import org.apache.spark.sql.functions._
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.Column

class Inspector(spark: SparkSession){
	private val dateFormatter = new SimpleDateFormat("dd.MM'-'HH:mm:ss:SSS");
	private def toDate(df: SimpleDateFormat) = udf((t: Long) => df.format(new Date(t)))

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

	def extractLogs(filePath: String, features: List[Feature], entity: String,
		eType: String, beginTimestamp: Long, endTimeStamp: Long, extractor: String):DataFrame ={
		val ee = EntityExtractor.getByName(extractor)
		val (colType, originalEntity) = ee.reverse(entity)
		val col = eType+colType
		val logFile = spark.read.parquet(filePath)
		logFile.createOrReplaceTempView("logfiles")
		val sqlStmt = "SELECT "+features.map(_.name).mkString(",")+" FROM logfiles WHERE "+col+"='"+originalEntity+
			"' AND timestamp>="+beginTimestamp+" AND timestamp<="+endTimeStamp
		val df = spark.sql(sqlStmt).sort(asc("timestamp"))
		val df2 = df.withColumn("timestamp2", toDate(dateFormatter)(df("timestamp")))
			.drop("timestamp").withColumnRenamed("timestamp2", "timestamp")
		val newCols = "timestamp"::df2.columns.toList.dropRight(1)
		df2.select(newCols.head, newCols.tail:_*)
	}

	def inspect(filePath: String, features: List[Feature], extractor: String, anomaliesFile: String, nbAnomalies: Int, rowNumber: Int,
		eType: String, interval: Duration):Unit = {
		val (entity, beginTimestamp, endTimeStamp) = extractInfo(anomaliesFile, nbAnomalies, rowNumber, eType, interval)
		val logs = extractLogs(filePath, features, entity, eType, beginTimestamp, endTimeStamp, extractor)
		logs.show()
	}
}