package features
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.Normalizer
import org.apache.spark.ml.feature.MinMaxScaler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.functions._
import scala.concurrent.duration._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.language.postfixOps
import evaluation._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.Column

/*
API to extract and preprocess features from data.
*/
class FeatureExtractor(spark: SparkSession, inject: DataFrame => DataFrame) extends Serializable{

	/*
	Returns a DataFrame from the file 'filePath' with all the features defined by 'features'
	as columns. Every feature is parsed to integers or doubles. They need to be further processed before
	they can be used for machine learning.
	*/
	def extractRawBasicFeatures(filePath: String, features: List[Feature],
		extractor: String = "hostsWithIpFallback", eType: String): (DataFrame,List[Feature]) = {
		val logFile = spark.read.parquet(filePath)
		logFile.createOrReplaceTempView("logfiles")
		val sqlStmt = "SELECT "+features.filter(_.parent.isEmpty).map(_.name).mkString(",")+" FROM logfiles"
		val df = spark.sql(sqlStmt)

		val ee = EntityExtractor.getByName(extractor)
		val (df2,newFeatures) = ee.extract(df, features, eType)

		val dfInjected = inject(df2)

		val res = newFeatures.map(f => f.parseCol(_)).foldLeft(dfInjected){ (previousdf, parser) => parser(previousdf) }
		(res, newFeatures)
	}

	/*
	Returns a DataFrame representing the traffic features from the basic features in 'df': aggregation over each interval of size 'interval'
	per src or dst entity. They need to be further processed before they can be used for machine learning.
	*/
	def extractRawTrafficFeatures(df: DataFrame, features: List[Feature], interval: Duration, eType: String): DataFrame = {
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

	/*
	Returns a DataFrame representing the traffic features : aggregation over the whole 'df' per src entity,
	or dst entity.It needs to be further processed before they can be used for machine learning.
	*/
	private def aggregate(df: DataFrame, eType: String, aggs: List[Column]): DataFrame = {
		df.groupBy(eType, eType+"Index").agg(aggs.head, aggs.tail:_*)
	}

	/*
	Returns a subspace 'features' from an already extracted DataFrame 'df'.
	*/
	def getSubSpaceFeatures(df: DataFrame, features: List[Feature]): DataFrame = {
		val cols = features.map(f => col(f.name))
		df.select(cols:_*)
	}

	/*
	Returns a DataFrame of final features ready for unsupervised machine learning to be
	applied from a DataFrame 'df' of raw features. Each feature is normalized and the fields
	are assembled into a single vector column.
	*/
	def getFinalFeatures(df: DataFrame, scaleMode: String = "unit", eType: String): DataFrame = {
		println("assembling...")
		val assembler = new VectorAssembler().setInputCols(df.columns.filter(_!=eType)).setOutputCol("features")
		val assembled = assembler.transform(df).select(eType, "timeinterval", "features")
		println("scaling...")
		scaleMode match{
			case "unit" => {
				val norm = new Normalizer().setInputCol("features").setOutputCol("scaledFeatures")
				norm.transform(assembled).drop("features")
			}
			case "rescale" => {
				val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
				val scalerModel = scaler.fit(assembled)
				scalerModel.transform(assembled).drop("features")
			}
		}
	}

	/*
	Returns a DataFrame of final features ready for unsupervised machine learning to be
	applied from a DataFrame 'df' of raw features. Each feature is normalized and kept as a column.
	*/
	def getFinalFeaturesAsColumns(df: DataFrame, scaleMode: String = "unit", eType: String): DataFrame = scaleMode match {
		case "unit" =>  normalizeToUnit(df, eType)
		case "rescale" => rescale(df, eType)
	}

	private def normalizeToUnit(df: DataFrame, eType: String): DataFrame = {
		val schemaB = spark.sparkContext.broadcast(df.dtypes.zipWithIndex.map{case ((colName, colType),index) => 
			(colName, colType, index)}.filter(_._1!=eType))
		val eTypeIndexB = spark.sparkContext.broadcast(df.columns.indexOf(eType))
		import spark.implicits._
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

	private def rescale(df: DataFrame, eType: String): DataFrame = {
		val scalesMins = df.dtypes.filter(_._1!=eType).map{case (colName, colType) =>
			println("Computing max and min for "+colName)
			val minMax = df.agg(min(colName),max(colName)).head
			colType match {
				case "LongType" => {
					val minVal = minMax.getLong(0).toDouble
					val maxVal = minMax.getLong(1).toDouble
					val scale = maxVal-minVal
					(colName, colType, scale, minVal)
				}
				case "DoubleType" => {
					val minVal = minMax.getDouble(0)
					val maxVal = minMax.getDouble(1)
					val scale = maxVal-minVal
					(colName, colType, scale, minVal)
				}
				case _ => throw new Exception("Unable to parse the column data type : "+colType)
			}
		}
		val scalesMinsB = spark.sparkContext.broadcast(scalesMins)
		val eTypeB = spark.sparkContext.broadcast(eType)
		println("Scaling...")
		val scaledRDD = df.rdd.mapPartitions(iter => {
			val scalesMins = scalesMinsB.value
			val eType = eTypeB.value
			iter.map(r => rescaleRow(r, scalesMins, eType))
		})
		val dfScaled = df.sqlContext.createDataFrame(scaledRDD , df.schema)
		df.columns.foldLeft(dfScaled){(prevDF, colName) => prevDF.withColumnRenamed(colName, "scaled"+colName).drop(colName)}
	}

	private def rescaleRow(row: Row, scalesMins: Array[(String, String, Double, Double)], eType: String):Row = {
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
	Returns a DataFrame of final features ready for unsupervised machine learning to be
	applied from the file 'filePath' with all the features defined by 'features'. Every feature
	is parsed and normalized. The 'colsMode' parameter defines whether these features are kept in
	separate columns (value "columns") or assembled in a single vector column (value "assembled"). The
	src and dst entities are extracted according to the 'extractor' parameter. The aggregation of logs by entities
	is done over each interval of duration 'interval'. 'trafficMode' defines whether the logs are grouped by "src" or "dst".
	*/
	def extractFeatures(filePath: String, features: List[Feature],
		extractor: String = "hostsWithIpFallback", interval: Duration,
		trafficMode: String = "src", scaleMode: String = "unit"): DataFrame = {
		println("Begin to extract basic features...")
		val entity = trafficMode+"entity"
		val (basic, newFeatures) = extractRawBasicFeatures(filePath, features, extractor, entity)
		println("Done.")
		println("Begin to extract traffic features...")
		val traffic = extractRawTrafficFeatures(basic, newFeatures, interval, entity)
		println("Done.")
		println("Begin to scale the features...")
		getFinalFeaturesAsColumns(traffic, scaleMode, entity)
	}

	def writeFeaturesToFile(features: DataFrame, fileName: String):Unit = {
		val w = features.columns.foldLeft(features){(prevdf, col) => rename(prevdf, col)}
		w.write.mode(SaveMode.Overwrite).parquet(fileName+".parquet")
		val stats = w.describe()
		stats.write.mode(SaveMode.Overwrite).parquet(fileName+"-stats.parquet")
	}

	private def rename(df: DataFrame, col: String):DataFrame = {
		val toRemove = " ()".toSet
		val newCol = col.filterNot(toRemove)
		df.withColumnRenamed(col, newCol)
	}

	/*
	Outputs the counts of the 20 most frequent values for each feature. This function
	should be used solely to gain insight about the data.
	*/
	private def frequentValues(df: DataFrame): Unit = {
		df.columns.foreach(f => df.groupBy(f).count().orderBy(desc("count")).show())
		df.columns.foreach(f => df.groupBy(f).count().orderBy(asc("count")).show())
	}
}