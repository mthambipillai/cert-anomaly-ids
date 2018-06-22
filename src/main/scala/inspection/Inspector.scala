/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
*/
package inspection
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._
import org.apache.spark.sql.DataFrame
import features._
import org.apache.spark.sql.functions._
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.broadcast.Broadcast
import scala.util.Try
import scalaz._
import Scalaz._

/*
Contains methods to load the persisted anomalies, fetch the matching original logs
from the data source and apply rules to the logs to evaluate precision and persist results.
*/
class Inspector(spark: SparkSession){
	private val dateFormatter = new SimpleDateFormat("dd.MM'-'HH:mm:ss:SSS");
	private def toDate(df: SimpleDateFormat) = udf((t: Long) => df.format(new Date(t)))

	/*
	Applies each rule in 'rules' to each of the reconstructed logs in 'allLogs'. Precision is
	then printed and the tagged results are persisted to 'resultsFile'.
	*/
	def inspectLogs(allLogs: List[DataFrame], rules: List[Rule], resultsFile: String):Unit = {
		val nbCols = allLogs.head.columns.size
		val tagIndexB = spark.sparkContext.broadcast(nbCols-2)
		val commentIndexB = spark.sparkContext.broadcast(nbCols-1)
		val newSchema = allLogs.head.schema
		val encoder = RowEncoder(newSchema)
		println("Inspecting logs...")
		val dfs = allLogs.map(df => flag(df, rules, newSchema, encoder, tagIndexB, commentIndexB))
		val all = dfs.tail.foldLeft(dfs.head)(_.union(_))

		printPrecision(all, dfs.size, resultsFile)

		println("Writing results to "+resultsFile+"...")
		all.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
		.option("header", "true").save(resultsFile)
	}

	/*
	* Returns the reconstructed logs of the anomalies from the original log source as 2 DataFrames :
	* One for the real logs and one for the fake injected logs if any.
	* @param filePath  path of the original logs source
	* @param features  list of features of the schema
	* @param extractor  name of the entity extractor to use
	* @param anomaliesFile  file path of the anomalies
	* @param eType  entity type (src or dst)
	* @param interval  time window used for aggregation
	* @param recall  true if we injected fake intrusions, false otherwise.
	* @param intrusionsDir  directory of the persisted intrusions
	*/
	def getAllLogs(filePath: String, features: List[Feature], extractor: String, anomaliesFile: String,
		eType: String, interval: Duration, recall: Boolean, intrusionsDir: String):String\/(List[DataFrame],List[DataFrame])={
		val ee = EntityExtractor.getByName(extractor)
		val featuresNames = features.map(_.name)
		val featuresString = featuresNames.mkString(",")
		for{
			tempLogFile <- Try(spark.read.parquet(filePath)).toDisjunction.leftMap(e =>
				"Could not read '"+filePath+"' because of "+e.getMessage)
			_ = tempLogFile.createOrReplaceTempView("temp")
			logFile = spark.sql("SELECT "+featuresString+" FROM temp")
			_ = logFile.createOrReplaceTempView("logfiles")
			_ <- if(recall) registerIntrusionsLogs(intrusionsDir, featuresNames) else Unit.right
			(realAnoms, injectedAnoms) <- loadAnoms(anomaliesFile, recall)
			_ = println("Fetching matching logs...")
			allLogs <- realAnoms.zipWithIndex.traverseU{case (row,id) =>
				for{
					sqlStmt <- getStmt(row, featuresString, interval, eType, ee)
				}yield{
					val logs = getLogs(sqlStmt)
					val withID = logs.withColumn("id", lit(id))
					val newCols = "id"::(withID.columns.toList.dropRight(1))
					val ordered = withID.select(newCols.head, newCols.tail:_*)
					ordered.withColumn("anomalytag",lit("")).withColumn("comments",lit(""))
				}
			}
		}yield (allLogs, if(recall) getInjectedLogs(injectedAnoms, interval) else Nil)
	}

	private def registerIntrusionsLogs(intrusionsDir: String, featuresNames: List[String]):String\/Unit = {
		val tempIntrusionsLogsDisj = Try(spark.read.parquet(intrusionsDir+"/logs/*"))
		.toDisjunction.leftMap(e => "Could not read '"+intrusionsDir+"' because of "+e.getMessage)
		tempIntrusionsLogsDisj.map(logs => {
			val ordered = logs.select(featuresNames.head, featuresNames.tail:_*)
			ordered.createOrReplaceTempView("injectedLogs")
		})
	}

	private def getInjectedLogs(injectedAnoms: List[Row], interval: Duration):List[DataFrame] = {
		injectedAnoms.map(row => {
			val beginTimestamp = row.getString(1).toDouble.toLong
			val endTimeStamp = beginTimestamp+interval.toMillis
			val stmt = "SELECT * FROM injectedLogs WHERE srchost='"+row.getString(0)+
				"' AND timestamp>="+beginTimestamp+" AND timestamp<="+endTimeStamp
			spark.sql(stmt).sort(asc("timestamp"))
		})
	}

	/*
	Filter the logs 'all' to count the ones that were flagged as anomalies, then compute
	precision using 'nbAll' and print it.
	*/
	private def printPrecision(all: DataFrame, nbAll:Int, resultsFile: String):Unit = {
		val nbPositives = all.filter(col("anomalytag")===lit("yes")).count.toInt
		val nbNegatives = nbAll - nbPositives
		val precision = (nbPositives.toDouble/nbAll.toDouble)*100.0
		println("Number of detected anomalies tagged as true anomalies (Precision) : "+
			nbPositives+"/"+nbAll+" = "+precision+"%")
		val text = if(nbNegatives==0){
			"The system was able to tag every anomaly. See "+resultsFile+" for further investigation."
		}else{
			val begin = "The system was unable to tag the remaining "+nbNegatives+" detected "
			val middle = if(nbNegatives==1){
				"anomaly. Tag it "
			}else{
				"anomalies. Tag them "
			}
			val end = "manually in "+resultsFile+" to obtain the exact precision measurement."
			begin+middle+end
		}
		println(text+"\n")
	}

	/*
	Loads anomalies in 2 separate lists : one for real anomalies and one for fake injected anomalies.
	*/
	private def loadAnoms(anomaliesFile: String, recall: Boolean): String\/(List[Row], List[Row])={
		println("Loading anomalies from "+anomaliesFile+"...")
		for{
			all <- Try(spark.read.format("com.databricks.spark.csv")
				.option("header", "true").load(anomaliesFile).collect.toList)
				.toDisjunction.leftMap(e => "Could not read '"+anomaliesFile+"' because of "+e.getMessage)
			injected = if(recall) all.filter(_.getString(0).contains("dummy")) else Nil
			real = all.filterNot(_.getString(0).contains("dummy"))
		}yield (real, injected)
	}

	private def getStmt(row: Row, featuresString: String, interval: Duration,
		eType: String, ee: EntityExtractor):String\/String ={
		val entity = row.getString(0)
		val beginTimestamp = row.getString(1).toDouble.toLong
		val endTimeStamp = beginTimestamp+interval.toMillis
		for{
			(colType, originalEntity) <- ee.reverse(entity)
		}yield{
			val col = eType+colType
			"SELECT "+featuresString+" FROM logfiles WHERE "+col+"='"+originalEntity+
				"' AND timestamp>="+beginTimestamp+" AND timestamp<="+endTimeStamp
		}
	}

	private def getLogs(sqlStmt: String):DataFrame = {
		val df = spark.sql(sqlStmt).dropDuplicates("timestamp").sort(asc("timestamp"))
		val df2 = df.withColumn("timestamp2", toDate(dateFormatter)(df("timestamp")))
			.drop("timestamp").withColumnRenamed("timestamp2", "timestamp")
		val newCols = "timestamp"::df2.columns.toList.dropRight(1)
		df2.select(newCols.head, newCols.tail:_*).coalesce(1)
	}

	/*
	Returns a new DataFrame from 'logs' with 2 additional columns : anomalytag and comments. Each rule
	in 'rules' is applied.
	*/
	private def flag(logs: DataFrame, rules: List[Rule], schema: StructType, encoder: ExpressionEncoder[Row],
		tagIndexB: Broadcast[Int], commentIndexB: Broadcast[Int]):DataFrame = {
		val rulesWithAccsB = spark.sparkContext.broadcast(rules.map(r => (r, r.initAcc(spark))))
		logs.mapPartitions(iter => {
			val rows = iter.toList
			val tagIndex = tagIndexB.value
			val commentIndex = commentIndexB.value
			val (tag, nRows) = rulesWithAccsB.value.foldLeft((false, rows)){case ((tag, rows), (rule, acc)) => 
				val (nextTag, nextRows) = rule.flag(rows, acc, schema, tagIndex, commentIndex)
				(tag||nextTag, nextRows)
			}
			nRows.toIterator
		})(encoder)
	}
}