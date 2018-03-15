package features
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
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

/*
API to extract and preprocess features from data.
*/
class FeatureExtractor(spark: SparkSession, inject: DataFrame => DataFrame) extends Serializable{
	private var srcEntityReverser:DataFrame=null
	private var dstEntityReverser:DataFrame=null
	private var timeIntervalReverser:DataFrame=null

	/*
	Returns a DataFrame from the file 'filePath' with all the features defined by 'features'
	as columns. Every feature is parsed to integers or doubles. They need to be further processed before
	they can be used for machine learning.
	*/
	def extractRawBasicFeatures(filePath: String, features: List[Feature] = Feature.getSSHFeatures(),
		extractor: String = "hostsWithIpFallback"): (DataFrame,List[Feature]) = {
		val logFile = spark.read.parquet(filePath)
		logFile.createOrReplaceTempView("logfiles")
		val sqlStmt = "SELECT "+features.map(_.name).mkString(",")+" FROM logfiles"
		val df = spark.sql(sqlStmt)

		val ee = EntityExtractor.getByName(extractor)
		val (df2,newFeatures) = ee.extract(df, features)

		val dfInjected = inject(df2)

		val res = newFeatures.map(f => f.parseCol(_)).foldLeft(dfInjected){ (previousdf, parser) => parser(previousdf) }
		(res, newFeatures)
	}

	/*
	Returns 2 DataFrames representing the traffic features from the basic features in 'df': aggregation over each interval of size 'interval'
	per 1) src entity, and 2) dst entity.They need to be further processed before they can be used for machine learning.
	*/
	def extractRawTrafficFeatures(df: DataFrame, features: List[Feature], interval: Duration, mode: String): List[DataFrame] = {
		val minMax = df.agg(min("timestamp"),max("timestamp")).head
		val minTime = minMax.getDouble(0)
		val maxTime = minMax.getDouble(1)
		val nbIntervals = (((maxTime-minTime)/interval.toMillis)+1).toInt
		
		val splits = (1 to nbIntervals).map(i => {
			val low = minTime + (i-1)*interval.toMillis
			val high = low + interval.toMillis
			val subdf = df.filter(col("timestamp") >= low).filter(col("timestamp") < high)
			lazy val dfSrc = aggregate(subdf, features, "srcentity")
			lazy val dfDst = aggregate(subdf, features, "dstentity")
			val dfs = mode match {
				case "src" => List(dfSrc)
				case "dst" => List(dfDst)
				case "all" => List(dfSrc, dfDst)
			}
			dfs.map(_.withColumn("timeinterval",lit(low)))
		})

		splits.tail.foldLeft(splits.head){ (ls1, ls2) => ls1.zip(ls2).map{case (df1, df2) => df1.union(df2)}}
	}

	/*
	Returns 2 DataFrames representing the traffic features : aggregation over the whole 'df' per 1) src entity,
	and 2) dst entity.They need to be further processed before they can be used for machine learning.
	*/
	private def aggregate(df: DataFrame, features: List[Feature], eType: String): DataFrame = {
		val aggs1 = features.filter(f => f.name!=eType || f.name!=eType+"Index").flatMap(_.aggregate())
		df.groupBy(eType,eType+"Index").agg(aggs1.head, aggs1.tail:_*)
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
	def getFinalFeatures(df: DataFrame): DataFrame = {
		println("assembling...")
		val assembler = new VectorAssembler().setInputCols(df.columns).setOutputCol("features")
		val assembled = assembler.transform(df).select("features")
		println("scaling...")
		val scaler = new MinMaxScaler().setInputCol("features").setOutputCol("scaledFeatures")
		val scalerModel = scaler.fit(assembled)
		scalerModel.transform(assembled).select("scaledFeatures")
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
		val schemaB = spark.sparkContext.broadcast(df.dtypes.filter(_._1!=eType))
		val scaledRDD = df.rdd.mapPartitions(iter => {
			val schema = schemaB.value
			iter.map(r => normalizeRowToUnit(r, schema, eType))
		})
		val entityField = StructField(eType, StringType, true)
		val timeField = StructField("timeinterval", DoubleType, true)
		val newSchema = StructType(Seq(entityField, timeField)++schemaB.value.map(sf => StructField("scaled"+sf._1, DoubleType, true)))
		df.sqlContext.createDataFrame(scaledRDD, newSchema)
	}

	private def normalizeRowToUnit(row: Row, schema: Array[(String, String)], eType: String): Row = {
		val values = schema.map{case (colName, colType) =>
			val index = row.fieldIndex(colName)
			colType match{
				case "LongType" => row.getLong(index).toDouble
				case "DoubleType" => row.getDouble(index)
			}
		}
		val norm = scala.math.sqrt(values.map(v => v*v).sum)
		val scaled = values.map(_/norm)
		val entity = row.getString(row.fieldIndex(eType))
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
	is done over each interval of duration 'interval'. 'trafficMode' defines whether the logs are grouped by "src",
	"dst" or "all".
	*/
	def extractFeatures(filePath: String, features: List[Feature] = Feature.getSSHFeatures(),
		extractor: String = "hostsWithIpFallback", interval: Duration = 30 minute,
		trafficMode: String = "src", scaleMode: String = "unit"): List[DataFrame] = {
		println("Begin to extract basic features...")
		val (basic, newFeatures) = extractRawBasicFeatures(filePath, features, extractor)
		println("Done.")
		println("Begin to extract traffic features...")
		val traffic = extractRawTrafficFeatures(basic, newFeatures, interval, trafficMode)
		//frequentValues(traffic.head)
		println("Done.")
		val entities = trafficMode match{
			case "src" => List("srcentity")
			case "dst" => List("dstentity")
			case "all" => List("srcentity", "dstentity")
		}
		println("Begin to scale the features...")
		traffic.zip(entities).map{case (df,eType) => getFinalFeaturesAsColumns(df, scaleMode, eType)}
	}

	/*
	Computes the original entity and timestamp interval for each parsed and scaled intrusion of 'intrusions'
	back from the original values computed with 'extractRawBasicFeatures'.
	*/
	def reverseResults(intrusions: DataFrame, eType: String = "srcentity"):DataFrame = {
		println("Reversing the intrusions detected...")
		intrusions.drop("scaled"+eType+"Index", "scaledtimeinterval")
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