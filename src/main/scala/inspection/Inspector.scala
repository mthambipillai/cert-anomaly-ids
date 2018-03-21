package inspection
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._
import org.apache.spark.sql.DataFrame
import features._
import org.apache.spark.sql.functions._
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.Column
import scala.io.Source
import org.apache.spark.sql.Row

class Inspector(spark: SparkSession){
	private val dateFormatter = new SimpleDateFormat("dd.MM'-'HH:mm:ss:SSS");
	private def toDate(df: SimpleDateFormat) = udf((t: Long) => df.format(new Date(t)))

	def inspect(filePath: String, features: List[Feature], extractor: String, anomaliesFile: String, nbAnomalies: Int, rowNumber: Int,
		eType: String, interval: Duration):Unit = {
		if(rowNumber >= nbAnomalies){
			println("row number is "+rowNumber+" but must be between 0 and "+(nbAnomalies-1))
			System.exit(1)
		}
		println("Loading anomaly...")
		val row = loadAnoms(anomaliesFile)(rowNumber)
		val logFile = spark.read.parquet(filePath)
		logFile.createOrReplaceTempView("logfiles")
		val ee = EntityExtractor.getByName(extractor)
		val sqlStmt = getStmt(row, features, interval, eType, ee)
		println("Fetching matching logs...")
		val logs = getLogs(sqlStmt)
		logs.show(truncate = false)
	}

	def inspectAll(filePath: String, features: List[Feature], extractor: String, anomaliesFile: String,
		eType: String, interval: Duration, resultsFile: String): Unit = {
		val ee = EntityExtractor.getByName(extractor)
		val logFile = spark.read.parquet(filePath)
		logFile.createOrReplaceTempView("logfiles")
		println("Loading anomalies...")
		val anoms = loadAnoms(anomaliesFile)
		println("Fetching matching logs...")
		val dfs = anoms.zipWithIndex.map{case (row,id) =>
			val sqlStmt = getStmt(row, features, interval, eType, ee)
			val logs = getLogs(sqlStmt)
			val withID = logs.withColumn("id", lit(id))
			val newCols = "id"::(logs.columns.toList.dropRight(1))
			withID.select(newCols.head, newCols.tail:_*)
		}
		val all = dfs.tail.foldLeft(dfs.head)(_.union(_))
		println("Writing results to "+resultsFile+"...")
		all.coalesce(1).write.format("com.databricks.spark.csv").option("header", "true").save(resultsFile)
	}

	private def loadAnoms(anomaliesFile: String): Array[Row]={
		spark.read.format("com.databricks.spark.csv")
		.option("header", "true").load(anomaliesFile).collect
	}

	private def extractFromRow(row: Row, interval: Duration):(String,Long,Long) ={
		val entity = row.getString(0)
		val beginTimestamp = row.getString(1).toDouble.toLong
		val endTimeStamp = beginTimestamp+interval.toMillis
		(entity, beginTimestamp, endTimeStamp)
	}

	private def getStmt(row: Row, features: List[Feature], interval: Duration, eType: String, ee: EntityExtractor):String ={
		val entity = row.getString(0)
		val beginTimestamp = row.getString(1).toDouble.toLong
		val endTimeStamp = beginTimestamp+interval.toMillis
		val (colType, originalEntity) = ee.reverse(entity)
		val col = eType+colType
		"SELECT "+features.map(_.name).mkString(",")+" FROM logfiles WHERE "+col+"='"+originalEntity+
			"' AND timestamp>="+beginTimestamp+" AND timestamp<="+endTimeStamp
	}

	private def getLogs(sqlStmt: String):DataFrame = {
		val df = spark.sql(sqlStmt).sort(asc("timestamp"))
		val df2 = df.withColumn("timestamp2", toDate(dateFormatter)(df("timestamp")))
			.drop("timestamp").withColumnRenamed("timestamp2", "timestamp")
		val newCols = "timestamp"::df2.columns.toList.dropRight(1)
		df2.select(newCols.head, newCols.tail:_*)
	}
}