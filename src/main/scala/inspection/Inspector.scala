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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.SaveMode
import org.apache.spark.broadcast.Broadcast
import scala.util.Try
import scalaz._
import Scalaz._

class Inspector(spark: SparkSession){
	private val dateFormatter = new SimpleDateFormat("dd.MM'-'HH:mm:ss:SSS");
	private def toDate(df: SimpleDateFormat) = udf((t: Long) => df.format(new Date(t)))

	def inspectAll(filePath: String, features: List[Feature], extractor: String, anomaliesFile: String,
		eType: String, interval: Duration, resultsFile: String): String\/Unit = {
		val ee = EntityExtractor.getByName(extractor)
		for{
            logFile <- Try(spark.read.parquet(filePath)).toDisjunction.leftMap(e =>
            	"Could not read '"+filePath+"' because of "+e.getMessage)
            _ = logFile.createOrReplaceTempView("logfiles")
            anoms <- loadAnoms(anomaliesFile)
            _ = println("Fetching matching logs...")
			allLogs <- anoms.zipWithIndex.traverseU{case (row,id) =>
				for{
					sqlStmt <- getStmt(row, features, interval, eType, ee)
				}yield{
					val logs = getLogs(sqlStmt)
					val withID = logs.withColumn("id", lit(id))
					val newCols = "id"::(withID.columns.toList.dropRight(1))
					val ordered = withID.select(newCols.head, newCols.tail:_*)
					ordered.withColumn("anomalytag",lit("")).withColumn("comments",lit(""))
				}
			}
		}yield{
			val nbCols = features.size+3//columns 'id', 'anomalytag' and 'comments' were added
			val tagIndexB = spark.sparkContext.broadcast(nbCols-2)
			val commentIndexB = spark.sparkContext.broadcast(nbCols-1)
			val newSchema = allLogs.head.schema
			val encoder = RowEncoder(newSchema)
			val (tags, dfs) = allLogs.map(df => 
				flag(df, Rule.BroSSHRules, newSchema, encoder, tagIndexB, commentIndexB)).toList.unzip
			val all = dfs.tail.foldLeft(dfs.head)(_.union(_))

			printPrecision(tags, resultsFile)

			println("Writing results to "+resultsFile+"...")
			all.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "true").save(resultsFile)
		}
	}

	private def printPrecision(tags: List[Boolean], resultsFile: String):Unit = {
		val nbPositives = tags.filter(_==true).size
		val nbAll = tags.size
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

	private def loadAnoms(anomaliesFile: String): String\/List[Row]={
		println("Loading anomalies...")
		Try(spark.read.format("com.databricks.spark.csv")
			.option("header", "true").load(anomaliesFile).collect.toList)
			.toDisjunction.leftMap(e =>
            "Could not read '"+anomaliesFile+"' because of "+e.getMessage)
	}

	private def extractFromRow(row: Row, interval: Duration):(String,Long,Long) ={
		val entity = row.getString(0)
		val beginTimestamp = row.getString(1).toDouble.toLong
		val endTimeStamp = beginTimestamp+interval.toMillis
		(entity, beginTimestamp, endTimeStamp)
	}

	private def getStmt(row: Row, features: List[Feature], interval: Duration,
		eType: String, ee: EntityExtractor):String\/String ={
		val entity = row.getString(0)
		val beginTimestamp = row.getString(1).toDouble.toLong
		val endTimeStamp = beginTimestamp+interval.toMillis
		for{
			(colType, originalEntity) <- ee.reverse(entity)
		}yield{
			val col = eType+colType
			"SELECT "+features.map(_.name).mkString(",")+" FROM logfiles WHERE "+col+"='"+originalEntity+
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

	private def flag(logs: DataFrame, rules: List[Rule], schema: StructType, encoder: ExpressionEncoder[Row],
		tagIndexB: Broadcast[Int], commentIndexB: Broadcast[Int]):(Boolean, DataFrame) = {
		val finalTag = spark.sparkContext.longAccumulator("Tag")
		val rulesWithAccsB = spark.sparkContext.broadcast(rules.map(r => (r, r.initAcc(spark))))
		val tagged = logs.mapPartitions(iter => {
			val rows = iter.toList
			val tagIndex = tagIndexB.value
			val commentIndex = commentIndexB.value
			val (tag, nRows) = rulesWithAccsB.value.foldLeft((false, rows)){case ((tag, rows), (rule, acc)) => 
				val (nextTag, nextRows) = rule.flag(rows, acc, schema, tagIndex, commentIndex)
				(tag||nextTag, nextRows)
			}
			if(tag) finalTag.add(1)
			nRows.toIterator
		})(encoder)
		(finalTag.value == 0, tagged)
	}
}