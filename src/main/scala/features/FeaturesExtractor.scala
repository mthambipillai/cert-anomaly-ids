/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package features
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import scala.concurrent.duration._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column
import scala.util.Try
import scalaz._
import Scalaz._

/*
Contains methods to extract features from logs, parse them, aggregate them and
scale them. More details about features extraction can be found in the wiki.
*/
class FeatureExtractor(spark: SparkSession, inject: (Long,Long,DataFrame) => String\/DataFrame) extends Serializable{

	/*
	Returns a DataFrame from the file 'filePath' with all the basic features defined by 'features'
	as columns. Every feature is parsed to doubles. src and dst entities are extracted according
	to the 'extractor'. If recall computation is activated in the IDSConfig, intrusions are injected
	with the 'inject' function in the constructor. They need to be further processed before
	they can be used for machine learning.
	*/
	def extractRawBasicFeatures(filePath: String, features: List[Feature],
		extractor: String = "hostsWithIpFallback", eType: String): String\/(DataFrame,List[Feature]) = {
		println("Begin to extract basic features...")
		for{
			logFile <- Try(spark.read.parquet(filePath)).toDisjunction.leftMap(e =>
				"Could not read '"+filePath+"' because of "+e.getMessage)
			_ = logFile.createOrReplaceTempView("logfiles")
			sqlStmt = "SELECT "+features.filter(_.parent.isEmpty).map(_.name).mkString(",")+" FROM logfiles"
			df <- Try(spark.sql(sqlStmt)).toDisjunction.leftMap(e =>
				"Could not execute statement '"+sqlStmt+"' because of "+e.getMessage)
			minMax = df.agg(min("timestamp"),max("timestamp")).head
			minTime = minMax.getLong(0)
			maxTime = minMax.getLong(1)
			dfInjected <- inject(minTime, maxTime, df)
			ee = EntityExtractor.getByName(extractor)
			(df2,newFeatures) <- ee.extract(dfInjected, features, eType)
		}yield{
			val res = newFeatures.map(f => f.parseCol(_)).foldLeft(df2){ (previousdf, parser) => parser(previousdf) }
			(res, newFeatures)
		}
	}

	/*
	Returns a DataFrame representing the traffic features from the basic features in 'df' : aggregation over
	each interval of size 'interval' per src or dst entity defined by 'eType'. They need to be further
	processed before they can be used for machine learning.
	*/
	def extractRawTrafficFeatures(df: DataFrame, features: List[Feature], interval: Duration,
		eType: String): DataFrame = {
		println("Begin to extract traffic features...")
		val minMax = df.agg(min("timestamp"),max("timestamp")).head
		val minTime = minMax.getDouble(0)
		val maxTime = minMax.getDouble(1)
		val nbIntervals = (((maxTime-minTime)/interval.toMillis)+1).toInt
		val aggs = features.flatMap(_.aggregate())
		val splits = (1 to nbIntervals).map(i => {
			val lowB = spark.sparkContext.broadcast(minTime + (i-1)*interval.toMillis)
			val highB = spark.sparkContext.broadcast(lowB.value + interval.toMillis)
			val subdf = df.filter(col("timestamp") >= lowB.value).filter(col("timestamp") < highB.value)
			val aggdf = aggregate(subdf, eType, aggs)
			aggdf.withColumn("timeinterval",lit(lowB.value))
		})
		splits.tail.foldLeft(splits.head){case (df1, df2) => df1.union(df2)}
	}

	private def aggregate(df: DataFrame, eType: String, aggs: List[Column]): DataFrame = {
		df.groupBy(eType, eType+"Index").agg(aggs.head, aggs.tail:_*)
	}

	/*
	Returns a DataFrame of final features ready for unsupervised machine learning to be
	applied from a DataFrame 'df' of raw features. Each feature is normalized and kept as a column.
	'scaleMode' defines the technique used for feature scaling.
	*/
	def getFinalFeaturesAsColumns(df: DataFrame, scaleMode: String, eType: String): String\/DataFrame = {
		println("Begin to scale the features...")
		scaleMode match {
			case "unit" =>  normalizeToUnit(df, eType).right
			case "rescale" => rescale(df, eType)
			case _ => ("Scale mode '"+scaleMode+"' is not recognized.").left
		}
	}

	/*
	Returns a DataFrame of final features normalized from 'df'. Each row is scaled such that its norm is 1.0.
	*/
	private def normalizeToUnit(df: DataFrame, eType: String): DataFrame = {
		val schemaB = spark.sparkContext.broadcast(df.dtypes.zipWithIndex.map{case ((colName, colType),index) => 
			(colName, colType, index)}.filter(_._1!=eType))
		val eTypeIndexB = spark.sparkContext.broadcast(df.columns.indexOf(eType))
		val entityField = StructField(eType, StringType, true)
		val timeField = StructField("timeinterval", DoubleType, true)
		val newSchema = StructType(Seq(entityField, timeField)++schemaB.value.map(sf => 
			StructField("scaled"+sf._1, DoubleType, true)))
		val encoder = RowEncoder(newSchema)
		df.mapPartitions(iter => {
			val schema = schemaB.value
			val eTypeIndex = eTypeIndexB.value
			iter.map(r => normalizeRowToUnit(r, schema, eTypeIndex))
		})(encoder)
	}

	private def normalizeRowToUnit(row: Row, schema: Array[(String, String, Int)], eTypeIndex: Int): Row = {
		val values = schema.map{case (colName, colType, index) =>
			colType match{
				case "LongType" => row.getLong(index).toDouble
				case "DoubleType" => row.getDouble(index)
			}
		}
		val norm = scala.math.sqrt(values.map(v => v*v).sum)
		val scaled = values.map(_/norm)
		val entity = row.getString(eTypeIndex)
		val t = row.getDouble(row.fieldIndex("timeinterval"))
		Row.fromSeq(Seq(entity, t) ++ scaled.toSeq)
	}

	/*
	Returns a DataFrame of final features rescaled from 'df'. The range of each feature is
	mapped to a 0.0 - 1.0 range.
	*/
	private def rescale(df: DataFrame, eType: String): String\/DataFrame = {
		for{
			scalesMins <- df.dtypes.filter(_._1!=eType).map{case (colName, colType) =>
				println("Computing max and min for "+colName)
				val minMax = df.agg(min(colName),max(colName)).head
				colType match {
					case "LongType" => {
						val minVal = minMax.getLong(0).toDouble
						val maxVal = minMax.getLong(1).toDouble
						val scale = maxVal-minVal
						(colName, colType, scale, minVal).right
					}
					case "DoubleType" => {
						val minVal = minMax.getDouble(0)
						val maxVal = minMax.getDouble(1)
						val scale = maxVal-minVal
						(colName, colType, scale, minVal).right
					}
					case _ => ("Unable to rescale with the column data type : "+colType).left
				}
			}.toList.sequenceU
		}yield{
			val scalesMinsB = spark.sparkContext.broadcast(scalesMins)
			val eTypeB = spark.sparkContext.broadcast(eType)
			println("Scaling...")
			val encoder = RowEncoder(df.schema)
			val dfScaled = df.mapPartitions(iter => {
				val scalesMins = scalesMinsB.value
				val eType = eTypeB.value
				iter.map(r => rescaleRow(r, scalesMins, eType))
			})(encoder)
			df.columns.foldLeft(dfScaled){(prevDF, colName) => 
				prevDF.withColumnRenamed(colName, "scaled"+colName).drop(colName)}
		}
	}

	private def rescaleRow(row: Row, scalesMins: List[(String, String, Double, Double)], eType: String):Row = {
		val eIndex = row.fieldIndex(eType)
		val initSeq:Seq[Any] = Seq(row.getString(eIndex))
		val newSeq = scalesMins.foldLeft(initSeq){case (prevSeq, (colName, colType, scale, minVal)) =>
			val index = row.fieldIndex(colName)
			val x = colType match{
				case "LongType" => row.getLong(index).toDouble
				case "DoubleType" => row.getDouble(index)
			}
			val scaled = if(scale==0) 1.0 else (x-minVal)/scale
			prevSeq :+ scaled
		}
		Row.fromSeq(newSeq)
	}

	/*
	*Returns a DataFrame of final features ready for unsupervised machine learning to be applied.
	*@param filePath  source of the DataFrame.
	*@param features  list of features to parse, aggregate and normalize.
	*@param extractor  defines the entity extractor to use.
	*@param interval  time window of the aggregation.
	*@param trafficMode  defines whether the logs are grouped by "src" or "dst".
	*@param scaleMode  defines the feature scaling technique to use.
	*/
	def extractFeatures(filePath: String, features: List[Feature], extractor: String, interval: Duration,
		trafficMode: String, scaleMode: String): String\/DataFrame = {
		val entity = trafficMode+"entity"
		for{
			(basic, newFeatures) <- extractRawBasicFeatures(filePath, features, extractor, entity)
			traffic = extractRawTrafficFeatures(basic, newFeatures, interval, entity)
			res <- getFinalFeaturesAsColumns(traffic, scaleMode, entity)
		}yield res
	}

	def writeFeaturesToFile(features: DataFrame, fileName: String, statsFileName: String):Unit = {
		println("Writing features to "+fileName+".parquet...")
		val w = features.columns.foldLeft(features){(prevdf, col) => rename(prevdf, col)}
		w.write.mode(SaveMode.Overwrite).parquet(fileName+".parquet")
		val stats = w.describe()
		println("Writing features stats to "+statsFileName+".parquet...")
		stats.write.mode(SaveMode.Overwrite).parquet(statsFileName+".parquet")
	}

	private def rename(df: DataFrame, col: String):DataFrame = {
		val toRemove = " ()".toSet
		val newCol = col.filterNot(toRemove)
		df.withColumnRenamed(col, newCol)
	}

	private def frequentValues(df: DataFrame): Unit = {
		df.columns.foreach(f => df.groupBy(f).count().orderBy(desc("count")).show())
		df.columns.foreach(f => df.groupBy(f).count().orderBy(asc("count")).show())
	}
}