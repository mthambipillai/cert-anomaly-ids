import features._
import isolationforest._
import scala.concurrent.duration._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.security.UserGroupInformation

object MainIDSApp {
  def main(args: Array[String]) {
    //configureKrbAuthentication("mthambip@CERN.CH", "mthambip.keytab")
    val spark = SparkSession.builder.appName("MainIDSApp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  	val fe = new FeatureExtractor(spark)
  	val filePath = "/project/security/logs/BroSSH/year=2017/month=06/day=02/94dd65c7-6da2-45c5-b342-6ddc964dbad1.parquet"
  	//val filePath = "../brossh/sshfile1.parquet"
  	val finalFeatures = fe.extractFeatures(filePath)
    val finalFeaturesSrc = finalFeatures.head
    finalFeaturesSrc.cache()

    val iForest = new IsolationForest(spark,finalFeaturesSrc,4,256)
    val anomalies = iForest.detect()
    finalFeaturesSrc.unpersist()
    val resolvedAnomalies = fe.reverseResults(anomalies, "srcentity")
    resolvedAnomalies.take(10).foreach(println)

    val r = resolvedAnomalies.select("srcentity","timeinterval").take(1)(0)
    val logs = fe.inspectResult(resolvedAnomalies, "srcentity", r.getString(0), r.getLong(1))
    println(r.getString(0)+" "+r.getLong(1))
    logs.show()

    spark.stop()
  }

   def configureKrbAuthentication(krbPrincipal: String, krbKeytabDir: String): Unit = {
    // Kerberos authentication
    val hdpconf = new org.apache.hadoop.conf.Configuration()
    hdpconf.set("hadoop.security.authentication", "Kerberos")
    UserGroupInformation.setConfiguration(hdpconf)
    // Use keytab
    println("Logging in...")
    UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeytabDir)
  }

}