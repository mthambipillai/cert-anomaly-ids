import features._
import isolationforest._
import kmeans._
import scala.concurrent.duration._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode
import evaluation._
import scala.language.postfixOps
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
import config._
import inspection._
import detection.Ensembler
import detection.Detector
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.Try
import scalaz._
import Scalaz._

class Dispatcher(spark: SparkSession, conf: IDSConfig){

	def dispatch(command: String):String\/Unit= command match{
		case "extract" => handleExtract
		case "detect" => handleDetect
		case "inspectall" => handleInspect
		case _ => ("Invalid command '"+conf.mode+"'.").left
	}

	private def handleExtract():String\/Unit = {
		val fe = new FeatureExtractor(spark, df => df)
		for(
			finalFeatures <- fe.extractFeatures(conf.filePath, conf.features, conf.extractor, conf.interval,
				conf.trafficMode, conf.scaleMode)
		)yield{
			fe.writeFeaturesToFile(finalFeatures, conf.featuresFile)
		}
	}

	private def handleDetect():String\/Unit = {
		val featuresFile = conf.featuresFile+".parquet"
		println("Reading features from "+featuresFile+"...")
		for{
			features <- Try(spark.read.parquet(featuresFile)).toDisjunction.leftMap(e =>
				"Could not read '"+featuresFile+"' because of "+e.getMessage)
			_ = features.cache()
			detectors <- Detector.getDetectors(spark, conf, features)
			anomalies <- new Ensembler().detectAndCombine(conf.trafficMode,conf.ensembleMode, conf.threshold, detectors)
		}yield {
			features.unpersist()
			new Evaluator().evaluateResults(anomalies, conf.trafficMode, conf.topAnomalies, conf.anomaliesFile)
		}
	}

	private def handleInspect():String\/Unit = {
		val ins = new Inspector(spark)
		ins.inspectAll(conf.filePath, conf.features, conf.extractor, conf.anomaliesFile, 
			conf.trafficMode, conf.interval, conf.inspectionResults)
	}

}