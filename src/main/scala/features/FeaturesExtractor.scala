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

/*
API to extract and preprocess features from data.
*/
class FeatureExtractor(spark: SparkSession){
	private var entityReverser:DataFrame=null
	private var timeIntervalReverser:DataFrame=null
	private var interval:Duration=null

	/*
	Returns a DataFrame from the file 'filePath' with all the features defined by 'features'
	as columns. Every feature is parsed to integers or doubles. They need to be further processed before
	they can be used for machine learning.
	*/
	def extractRawBasicFeatures(filePath: String, features: List[Feature] = Feature.getSSHFeatures(),
		extractor: String = "hostsWithIpFallback"): (DataFrame,List[Feature]) = {
		val logFile = spark.read.parquet(filePath)
		logFile.createOrReplaceTempView("logfile")
		val sqlStmt = "SELECT "+features.map(_.name).mkString(",")+" FROM logfile"
		val df = spark.sql(sqlStmt)
		val ee = EntityExtractor.getByName(extractor)
		val (df2,newFeatures) = ee.extract(df, features)
		//frequentValues(df2)
		val res = newFeatures.map(f => f.parseCol(_)).foldLeft(df2){ (previousdf, parser) => parser(previousdf) }
		this.entityReverser = res.select("srcentity","srcentityIndex","dstentity","dstentityIndex")
								.withColumnRenamed("srcentityIndex","srcentityTransformed")
								.withColumnRenamed("dstentityIndex", "dstentityTransformed").distinct
		val finalRes = res.drop("srcentity").withColumnRenamed("srcentityIndex","srcentity")
							.drop("dstentity").withColumnRenamed("dstentityIndex","dstentity")

		(finalRes, newFeatures)
	}

	/*
	Returns 2 DataFrames from 'df' representing the traffic features : aggregation over each interval of size 'interval'
	per 1) src entity, and 2) dst entity.They need to be further processed before they can be used for machine learning.
	*/
	def extractRawTrafficFeatures(df: DataFrame, features: List[Feature], interval: Duration): (DataFrame,DataFrame) = {
		this.interval = interval
		val minMax = df.agg(min("timestamp"),max("timestamp")).head
		val minTime = minMax.getLong(0)
		val maxTime = minMax.getLong(1)
		val nbIntervals = (((maxTime-minTime)/interval.toMillis)+1).toInt
		
		val splits = (1 to nbIntervals).map(i => {
			val low = minTime + (i-1)*interval.toMillis
			val high = low + interval.toMillis
			val subdf = df.filter(col("timestamp") >= low).filter(col("timestamp") < high)
			val (dfSrc, dfDst) = aggregate(subdf, features)
			(dfSrc.withColumn("timeinterval",lit(low)), dfDst.withColumn("timeinterval",lit(low)))
		}).unzip

		val traffic1 = splits._1.tail.foldLeft(splits._1.head)(_.union(_))
		val traffic2 = splits._2.tail.foldLeft(splits._2.head)(_.union(_))
		df.unpersist()
		//frequentValues(traffic1)
		(traffic1, traffic2)
	}

	/*
	Returns 2 DataFrames representing the traffic features : aggregation over the whole 'df' per 1) src entity,
	and 2) dst entity.They need to be further processed before they can be used for machine learning.
	*/
	private def aggregate(df: DataFrame, features: List[Feature]): (DataFrame,DataFrame) = {
		val aggs1 = features.filter(f => f.name!="srcentity").flatMap(_.aggregate())
		val dfSrc = df.groupBy("srcentity").agg(aggs1.head, aggs1.tail:_*)
		val aggs2 = features.filter(f => f.name!="dstentity").flatMap(_.aggregate())
		val dfDst = df.groupBy("dstentity").agg(aggs2.head, aggs2.tail:_*)
		(dfSrc, dfDst)
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
	def getFinalFeaturesAsColumns(df: DataFrame): DataFrame = {
		df.dtypes.foldLeft(df){ case (previousdf,(colName,colType)) => scaleColumn(previousdf, colName, colType)}
	}

	/*
	Maps the values of column 'colName' in 'df' to the range [0,1].
	*/
	private def scaleColumn(df: DataFrame, colName: String, colType: String): DataFrame = {
		val minMax = df.agg(min(colName),max(colName)).head
		val scaleUDF = colType match {
			case "LongType" => {
				val minVal = minMax.getLong(0)
				val maxVal = minMax.getLong(1)
				val diff = maxVal-minVal
				if(diff==0){
					udf((x:Long) => 1L)
				}else{
					udf((x:Long) => (x-minVal)/diff)
				}
			}
			case "DoubleType" => {
				val minVal = minMax.getDouble(0)
				val maxVal = minMax.getDouble(1)
				val diff = maxVal-minVal
				if(diff==0){
					udf((x:Double) => 1.0)
				}else{
					udf((x:Double) => (x-minVal)/diff)
				}
			}
			case _ => throw new Exception("Unable to parse the column data type : "+colType)
		}
		val res = df.withColumn("scaled"+colName,scaleUDF(col(colName)))
		if(colName=="srcentity" || colName=="dstentity"){
			val scaled = res.select(colName,"scaled"+colName).withColumnRenamed(colName,colName+"2")
			entityReverser = entityReverser.join(scaled, scaled(colName+"2") === entityReverser(colName+"Transformed"), "inner")
								.drop(colName+"2").drop(colName+"Transformed").withColumnRenamed("scaled"+colName, colName+"Transformed")
		}else if(colName=="timeinterval"){
			timeIntervalReverser = res.select("timeinterval","scaledtimeinterval")
		}
		res.drop(colName)
	}

	/*
	Returns a DataFrame of final features ready for unsupervised machine learning to be
	applied from the file 'filePath' with all the features defined by 'features'. Every feature
	is parsed and normalized. The 'mode' parameter defines whether these features are kept in
	separate columns (value "columns") or assembled in a single vector column (value "assembled"). The
	src and dst entities are extracted according to the 'extractor' parameter. The aggregation of logs by entities
	is done over each interval of duration 'interval'.
	*/
	def extractFeatures(filePath: String, features: List[Feature] = Feature.getSSHFeatures(), mode: String = "columns",
		extractor: String = "hostsWithIpFallback", interval: Duration = 6 hour): List[DataFrame] = {
		println("Begin to extract basic features...")
		val (basic, newFeatures) = extractRawBasicFeatures(filePath, features, extractor)
		println("Done.")
		println("Begin to extract traffic features...")
		val traffic = extractRawTrafficFeatures(basic, newFeatures, interval)
		val trafficList = List(traffic._1, traffic._2)
		println("Done.")
		mode match{
			case "columns" =>{
				println("Begin to scale the features...")
				trafficList.map(df => getFinalFeaturesAsColumns(df))
			}
			case "assembled" =>{
				println("Begin to assemble and scale the features...")
				trafficList.map(df => getFinalFeatures(df))
			}
		}
		
	}

	/*
	Computes the original entity and timestamp interval for each parsed and scaled intrusion of 'intrusions'
	back from the original values computed with 'extractRawBasicFeatures'.
	*/
	def reverseResults(intrusions: DataFrame, eType: String):DataFrame = {
		val neededCols = entityReverser.select(eType+"Transformed", eType)
		val withentity = intrusions.join(neededCols, intrusions("scaled"+eType) === neededCols(eType+"Transformed"), "inner")
							.drop("scaled"+eType).drop(eType+"Transformed")
		withentity.join(timeIntervalReverser, withentity("scaledtimeinterval") === timeIntervalReverser("scaledtimeinterval"), "inner")
			.drop("scaledtimeinterval")
	}

	/*
	Outputs the counts of the 20 most frequent values for each feature. This function
	should be used solely to gain insight about the data.
	*/
	private def frequentValues(df: DataFrame): Unit = {
		df.columns.foreach(f => df.groupBy(f).count().orderBy(desc("count")).show())
	}
}