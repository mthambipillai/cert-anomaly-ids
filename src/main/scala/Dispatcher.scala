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

class Dispatcher(spark: SparkSession, conf: IDSConfig) extends Serializable{

	def dispatch(command: String):String\/Unit= command match{
		case "extract" => handleExtract
		case "detect" => handleDetect
		case "inspect" => handleInspect
		case _ => ("Invalid command '"+conf.mode+"'.").left
	}

	private def handleExtract():String\/Unit = {
		val eval = new Evaluator(spark)
		def inject(min: Long, max: Long, df: DataFrame): String\/DataFrame = {
			eval.injectIntrusions(df, IntrusionKind.allKinds.map(k => (k,3)),
				min, max, conf.interval, conf.intrusionsDir)
		}
		val fe = new FeatureExtractor(spark, inject)
		for(
			finalFeatures <- fe.extractFeatures(conf.filePath, conf.featuresschema, conf.extractor, conf.interval,
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
			ensembler = new Ensembler()
			anomalies <- ensembler.detectAndCombine(conf.trafficMode,conf.ensembleMode, conf.threshold, detectors)
		}yield {
			features.unpersist()
			ensembler.persistAnomalies(anomalies, conf.trafficMode, conf.topAnomalies, conf.anomaliesFile)
		}
	}

	private def handleInspect():String\/Unit = {
		val ins = new Inspector(spark)
		val eval = new Evaluator(spark)
		for{
			(realLogs, injectedLogs) <- ins.getAllLogs(conf.filePath, conf.featuresschema, conf.extractor,
				conf.anomaliesFile, conf.trafficMode, conf.interval, conf.intrusionsDir)
			_ <- eval.evaluateIntrusions(injectedLogs, conf.intrusionsDir)

		}yield ins.inspectLogs(realLogs, conf.rules, conf.inspectionResults)
	}
}