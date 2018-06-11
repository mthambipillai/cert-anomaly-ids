/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
import org.apache.spark.sql.SparkSession
import evaluation._
import config._
import evaluation.Signer
import scala.util.Try
import scalaz._
import Scalaz._

object MainIDSApp {
	def main(args: Array[String]) {
		val t0 = System.nanoTime()
		val spark = SparkSession.builder.appName("CERT-Anomaly-IDS").getOrCreate()
		spark.sparkContext.register(Signer.acc, "signerAccumulator")
		val idsHome = scala.util.Properties.envOrElse("CERT_ANOMALY_IDS_HOME", ".")
		val finalRes = for{
			conf <- new IDSConfigParser(spark).loadConf(args, idsHome+"/conf/application_loader.conf")
			status = Try(spark.sparkContext.setLogLevel(conf.logLevel)).toDisjunction.leftMap(e =>
				"Unknown log level '"+conf.logLevel+"'")
			res <- if(status.isRight) new Dispatcher(spark, conf).dispatch(conf.mode) else status
		}yield res
		finalRes.leftMap(s => println("Error: "+s))

		spark.stop()
		val t1 = System.nanoTime()
		println("Elapsed time: " + (((t1 - t0)/1000000000.0)/60.0) + "min")
	}
}