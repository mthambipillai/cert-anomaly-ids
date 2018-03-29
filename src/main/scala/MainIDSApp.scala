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

object MainIDSApp {
  def main(args: Array[String]) {
    val t0 = System.nanoTime()
    val conf = IDSConfig.loadConf(args, "application.conf")
    val spark = SparkSession.builder.appName("MainIDSApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
   
    val eval = new Evaluator()
    //def inject(df: DataFrame):DataFrame = eval
      //.injectIntrusions(df, IntrusionKind.allKinds.map(ik => (ik,4)), 1496361600902L, 1496447999253L, conf.interval)
    
    conf.mode match{
      case "extract" => {
        val fe = new FeatureExtractor(spark, df => df)
        val finalFeatures = fe.extractFeatures(conf.filePath, conf.features, conf.extractor, conf.interval,
          conf.trafficMode, conf.scaleMode)
        fe.writeFeaturesToFile(finalFeatures, conf.featuresFile)
      }
      case "detect" => {
        val features = spark.read.parquet(conf.featuresFile)
        features.cache()
        val en = new Ensembler()
        val kmd = new KMeansDetector(spark, features, conf.kMeans.trainRatio, conf.kMeans.minNbK,
          conf.kMeans.maxNbK, conf.kMeans.elbowRatio, conf.kMeans.nbK, conf.kMeans.lowBound, conf.kMeans.upBound)
        val iForest = new IsolationForest(spark, features, features.count, conf.isolationForest.nbTrees, conf.isolationForest.nbSamples)
        val anomalies = en.detectAndCombine(conf.trafficMode, conf.threshold, List(kmd, iForest))
        features.unpersist()
        eval.evaluateResults(anomalies, conf.trafficMode, conf.topAnomalies, conf.anomaliesFile)
      }
      case "inspect" => {
        val ins = new Inspector(spark)
        ins.inspect(conf.filePath, conf.features, conf.extractor, conf.anomaliesFile, conf.topAnomalies,
          conf.anomalyIndex,  conf.trafficMode, conf.interval)
      }
      case "inspectall" => {
        val ins = new Inspector(spark)
        ins.inspectAll(conf.filePath, conf.features, conf.extractor, conf.anomaliesFile, 
          conf.trafficMode, conf.interval, conf.inspectionResults)
      }
      case _ => println("Invalid command.")
    }

    spark.stop()
    val t1 = System.nanoTime()
    println("Elapsed time: " + (((t1 - t0)/1000000000.0)/60.0) + "min")
  }
}