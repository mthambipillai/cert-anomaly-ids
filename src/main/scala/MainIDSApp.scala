import features._
import isolationforest._
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

object MainIDSApp {
  def main(args: Array[String]) {
    val t0 = System.nanoTime()
    val conf = IDSConfig.loadConf(args, "local.conf")
    val spark = SparkSession.builder.appName("MainIDSApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
   
    val eval = new Evaluator()
    def inject(df: DataFrame):DataFrame = eval
      .injectIntrusions(df, IntrusionKind.allKinds.map(ik => (ik,4)), 1496361600902L, 1496447999253L, conf.interval)
    val fe = new FeatureExtractor(spark, inject)

    conf.mode match{
      case "extract" => {
        writeFeatures(fe, eval, conf)
      }
      case "detect" => {
        val features = readFeatures(spark, fe, eval, conf)
        features.cache()
        val iForest = new IsolationForest(spark, features, 10, 256)
        val anomalies = iForest.detect(conf.threshold)
        features.unpersist()
        val resolvedAnomalies = fe.reverseResults(anomalies)
        eval.evaluateResults(resolvedAnomalies, conf.trafficMode, conf.topAnomalies, conf.anomaliesFile)
      }
      case "inspect" => {
        val ins = new Inspector(spark)
        ins.inspect(conf.filePath, conf.features, conf.extractor, conf.anomaliesFile, conf.topAnomalies,
          conf.anomalyIndex,  conf.trafficMode, conf.interval)
      }
      case _ => println("Invalid command.")
    }

    spark.stop()
    val t1 = System.nanoTime()
    println("Elapsed time: " + (((t1 - t0)/1000000000.0)/60.0) + "min")
  }

  private def writeFeatures(fe: FeatureExtractor, eval: Evaluator, conf: IDSConfig):Unit = {
    val finalFeatures = fe.extractFeatures(conf.filePath, conf.features, conf.extractor, conf.interval, conf.trafficMode, conf.scaleMode)
    val finalFeaturesSrc = finalFeatures.head
    finalFeaturesSrc.show()
    val w = finalFeaturesSrc.columns.foldLeft(finalFeaturesSrc){(prevdf, col) => rename(prevdf, col)}
    w.write.mode(SaveMode.Overwrite).parquet(conf.featuresFile)
    fe.persistReversers()
    eval.persistIntrusions()
  }

  private def rename(df: DataFrame, col: String):DataFrame = {
    val toRemove = " ()".toSet
    val newCol = col.filterNot(toRemove)
    df.withColumnRenamed(col, newCol)
  }

  private def readFeatures(spark: SparkSession, fe: FeatureExtractor, eval: Evaluator, conf: IDSConfig):DataFrame = {
    fe.loadReversers()
    eval.loadIntrusions()
    spark.read.parquet(conf.featuresFile)
  }
}