package optimization
import config.IDSConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import detection.Detector
import isolationforest.IsolationForest
import kmeans.KMeansDetector
import lof.LOFDetector
import features.Feature
import scala.concurrent.duration._
import inspection.Inspector
import inspection.Rule
import scalaz._
import Scalaz._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

case class Optimizer(
	name: String,
	parameters: List[Int],
	detectors: List[Detector],
	threshold: Double,
	topAnomalies: Int,
	ins: Inspector,
	filePath: String,
	featuresschema: List[Feature],
	extractor: String,
	trafficMode: String,
	interval: Duration,
	rules: List[Rule]){

	/*
	Applies the different detectors on the same data set in 'filePath' and prints the obtained precision
	for each one of them along with the parameter value associated with the detector.
	*/
	def optimize():String\/Unit = {
		println("Starting to optimize parameter "+name+"...")
		detectors.zip(parameters).foreach{ case (d,p) => 
			val detected = d.detect(threshold)
			val scoreCol = detected.columns.last
			val top = detected.sort(desc(scoreCol)).limit(topAnomalies).withColumnRenamed(scoreCol,"score")
			top.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
				.option("header", "true").save("temp"+p)
		}
		val precisionsDisj = parameters.traverseU(p => {
			for{
				(realLogs, _) <- ins.getAllLogs(filePath, featuresschema, extractor,
				"temp"+p, trafficMode, interval, false, "", topAnomalies)
				inspectedLogs <- ins.flagLogs(realLogs, rules)

			}yield ins.getPrecision(inspectedLogs)
		})
		precisionsDisj.map(precisions => precisions.zip(parameters).map{ case (precision, param) =>
			if(precision == -1){
				println("Parameter value '"+param+"' gives an empty result set.")
			}else{
				println("Parameter value '"+param+"' gives a precision of "+precision)
			}
		})
	}
}

object Optimizer{

	/*
	Builds an optimizer from a function that generates detectors to evaluate.
	*/
	def buildOptimizer(spark: SparkSession, data: DataFrame, conf: IDSConfig,
		generateDetectors: (SparkSession,DataFrame,IDSConfig) => String\/(String,List[Int],List[Detector])):String\/Optimizer={
		generateDetectors(spark, data, conf).map{ case (name, p,d) => Optimizer(name, p, d, conf.threshold, conf.topAnomalies,
			new Inspector(spark), conf.filePath, conf.featuresschema, conf.extractor, conf.trafficMode,
			conf.interval, conf.rules)}
	}

	/*
	Returns a list of IsolationForest with number of trees between 10 and 200 with a step of 10.
	*/
	def iforestNbTrees(spark: SparkSession, data: DataFrame,
		conf: IDSConfig):String\/(String,List[Int],List[IsolationForest]) = {
		val parameters = (10 to 200 by 10).toList
		val detectors = parameters.traverseU(nbTrees => IsolationForest.build(spark, data,
			conf.featuresStatsFile, nbTrees, conf.isolationForest.nbSamples))
		detectors.map(d => ("nbtrees (Number of trees)", parameters, d))
	}

	/*
	Returns a list of IsolationForest with number of samples either 128, 256, 512 or 1024.
	*/
	def iforestNbSamples(spark: SparkSession, data: DataFrame,
		conf: IDSConfig):String\/(String,List[Int],List[IsolationForest]) = {
		val parameters = List(128,256,512,1024)
		val detectors = parameters.traverseU(nbSamples => IsolationForest.build(spark, data,
			conf.featuresStatsFile, conf.isolationForest.nbTrees, nbSamples))
		detectors.map(d => ("nbsamples (Number of samples)", parameters, d))
	}

	/*
	Returns a list of KMeansDetector with number of clusters between 5 and 35.
	*/
	def kmeansNbClusters(spark: SparkSession, data: DataFrame,
		conf: IDSConfig):String\/(String,List[Int],List[KMeansDetector]) = {
		val parameters = (5 to 35).toList
		val detectors = parameters.map(k => new KMeansDetector(spark, data, conf.kMeans.trainRatio,
			conf.kMeans.minNbK, conf.kMeans.maxNbK, conf.kMeans.elbowRatio, k, conf.kMeans.lowBound,
			conf.kMeans.upBound))
		("nbk (Number of clusters)", parameters, detectors).right
	}

	/*
	Returns a list of LOFDetector with number of nearest neighbors between 4 and 10.
	*/
	def lofNbkNN(spark: SparkSession, data: DataFrame,
		conf: IDSConfig):String\/(String,List[Int],List[LOFDetector]) = {
		val parameters = (4 to 10).toList
		val detectors = parameters.map(k => new LOFDetector(spark, data, k, conf.lof.hashNbDigits,
			conf.lof.hashNbVects, conf.lof.maxScore))
		("k (Number of nearest neighbors)", parameters, detectors).right
	}
}