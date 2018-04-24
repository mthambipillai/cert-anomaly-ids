import org.apache.spark.sql.SparkSession
import evaluation._
import config._
import evaluation.Signer

object MainIDSApp {
	def main(args: Array[String]) {
		val t0 = System.nanoTime()
		val spark = SparkSession.builder.appName("MainIDSApp").getOrCreate()
		spark.sparkContext.setLogLevel("ERROR")
		spark.sparkContext.register(Signer.acc, "signerAccumulator")
		val idsHome = scala.util.Properties.envOrElse("SPARK_IDS_HOME", ".")
		val finalRes = for{
			conf <- IDSConfig.loadConf(args, idsHome+"/conf/application.conf")
			res <- new Dispatcher(spark, conf).dispatch(conf.mode)
		}yield res
		finalRes.leftMap(s => println("Error: "+s))

		spark.stop()
		val t1 = System.nanoTime()
		println("Elapsed time: " + (((t1 - t0)/1000000000.0)/60.0) + "min")
	}
}