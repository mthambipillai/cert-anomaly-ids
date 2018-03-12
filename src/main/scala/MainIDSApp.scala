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

object MainIDSApp {
  def main(args: Array[String]) {
    val t0 = System.nanoTime()
    val conf = IDSConfig.loadConf("local.conf")
    val spark = SparkSession.builder.appName("MainIDSApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val localRun = true
    val rwxMode = args(0)

    val eval = new Evaluator()
    def inject(df: DataFrame):DataFrame = eval
      .injectIntrusions(df, IntrusionKind.allKinds.map(ik => (ik,4)), 1496361600902L, 1496447999253L, conf.interval)

    val fe = new FeatureExtractor(spark, inject)
    val finalFeaturesSrc = getFeatures(spark, localRun, rwxMode, fe, eval, conf)
    println(finalFeaturesSrc.count)
    if(rwxMode=="w"){
      spark.stop()
      val t1 = System.nanoTime()
      println("Elapsed time: " + (((t1 - t0)/1000000000.0)/60.0) + "min")
      System.exit(0)
    }
    finalFeaturesSrc.cache()
    finalFeaturesSrc.printSchema

    val iForest = new IsolationForest(spark, finalFeaturesSrc, 100, 256)
    val anomalies = iForest.detect(0.51)
    finalFeaturesSrc.unpersist()
    println("nb intrusions : "+anomalies.count)
    val resolvedAnomalies = fe.reverseResults(anomalies)

    eval.evaluateResults(resolvedAnomalies)

    val t1 = System.nanoTime()
    println("Elapsed time: " + (((t1 - t0)/1000000000.0)/60.0) + "min")
    spark.stop()
  }

  private def rename(df: DataFrame, col: String):DataFrame = {
    val toRemove = " ()".toSet
    val newCol = col.filterNot(toRemove)
    df.withColumnRenamed(col, newCol)
  }

  private def getFeatures(spark: SparkSession, localRun: Boolean, rwxMode: String,
    fe: FeatureExtractor, eval: Evaluator, conf: IDSConfig):DataFrame = {
    if(!localRun || rwxMode=="x" || rwxMode=="wx" || rwxMode=="w"){
      val finalFeatures = fe.extractFeatures(conf.filePath, conf.features, conf.extractor, conf.interval, conf.trafficMode, conf.scaleMode)
      val finalFeaturesSrc = finalFeatures.head
      if(rwxMode!="x"){
        val w = finalFeaturesSrc.columns.foldLeft(finalFeaturesSrc){(prevdf, col) => rename(prevdf, col)}
        w.write.mode(SaveMode.Overwrite).parquet("features.parquet")
        fe.persistReversers()
        eval.persistIntrusions()
      }
      finalFeaturesSrc
    }else{
      fe.loadReversers()
      eval.loadIntrusions()
      spark.read.parquet("features.parquet")
    }
  }
}