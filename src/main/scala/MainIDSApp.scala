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
import evaluation.Signer

object MainIDSApp {
  def main(args: Array[String]) {
    val t0 = System.nanoTime()
    val spark = SparkSession.builder.appName("MainIDSApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    spark.sparkContext.register(Signer.acc, "signerAccumulator")
    val finalRes = for{
      conf <- IDSConfig.loadConf(args, "application.conf")
      res <- new Dispatcher(spark, conf).dispatch(conf.mode)
    }yield res
    finalRes.leftMap(s => println("Error: "+s))

    spark.stop()
    val t1 = System.nanoTime()
    println("Elapsed time: " + (((t1 - t0)/1000000000.0)/60.0) + "min")
  }
}