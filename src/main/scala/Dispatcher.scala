import features._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import evaluation._
import config._
import inspection._
import detection.Ensembler
import detection.Detector
import scala.util.Try
import scalaz._
import Scalaz._

/*
Contains methods to execute the different commands of the IDS by calling
the corresponding handlers.
*/
class Dispatcher(spark: SparkSession, conf: IDSConfig) extends Serializable{

	def dispatch(command: String):String\/Unit= command match{
		case "extract" => handleExtract
		case "detect" => handleDetect
		case "inspect" => handleInspect
		case _ => ("Invalid command '"+conf.mode+"'. Try --help for more information.").left
	}

	private def handleExtract():String\/Unit = {
		def inject(min: Long, max: Long, df: DataFrame): String\/DataFrame = {
			if(conf.recall){
				val eval = new Evaluator(spark)
				eval.injectIntrusions(df, conf.intrusions, min, max, conf.interval, conf.intrusionsDir)
			}else{
				df.right
			}
		}
		val fe = new FeatureExtractor(spark, inject)
		for(
			finalFeatures <- fe.extractFeatures(conf.filePath, conf.featuresschema, conf.extractor, conf.interval,
				conf.trafficMode, conf.scaleMode)
		)yield fe.writeFeaturesToFile(finalFeatures, conf.featuresFile)
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
				conf.anomaliesFile, conf.trafficMode, conf.interval, conf.recall, conf.intrusionsDir)
			_ <- if(conf.recall) eval.evaluateIntrusions(injectedLogs, conf.intrusionsDir) else "".right

		}yield ins.inspectLogs(realLogs, conf.rules, conf.inspectionResults)
	}
}