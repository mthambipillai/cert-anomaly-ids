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

object MainIDSApp {
  def main(args: Array[String]) {
    val t0 = System.nanoTime()
    val spark = SparkSession.builder.appName("MainIDSApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sqlContext.udf.register("MCV", Feature.mostCommonValue)
    val localRun = true
    val rwxMode = args(0)

    val eval = new Evaluator()
    def inject(df: DataFrame):DataFrame = eval
      .injectIntrusions(df, IntrusionKind.allKinds.map(ik => (ik,4)), 1496361600902L, 1496447999253L, 1 hour)

    val fe = new FeatureExtractor(spark, inject)
    val finalFeaturesSrc = getFeatures(spark, localRun, rwxMode, fe, eval)
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

  private def getFeatures(spark: SparkSession, localRun: Boolean, rwxMode: String, fe: FeatureExtractor, eval: Evaluator):DataFrame = {
    val filePath = if(localRun){
      "../brossh/*"
    }else{
      "/project/security/logs/BroSSH/year=2017/month=06/day=02/*"
    }
    if(!localRun || rwxMode=="x" || rwxMode=="wx" || rwxMode=="w"){
      val config = ConfigFactory.parseFile(new File("ids.conf"))
      val filePath = config.getString("filepath")
      val features = config.getString("features") match{
        case "BroSSH" => Feature.getSSHFeatures()
        case "BroConn" => Feature.getConnFeatures()
      }
      val extractor = config.getString("extractor")
      val interval = Duration(config.getString("aggregationtime"))
      val trafficMode = config.getString("trafficmode")
      val scaleMode = config.getString("scalemode")
      val finalFeatures = fe.extractFeatures(filePath, features, extractor, interval, trafficMode, scaleMode)
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