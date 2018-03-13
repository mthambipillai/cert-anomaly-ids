package config
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
import features.Feature
import scala.concurrent.duration._

case class IDSConfig(
	val mode: String,
	val filePath: String,
	val features: List[Feature],
	val extractor: String,
	val interval: Duration,
	val trafficMode: String,
	val scaleMode: String,
	val featuresFile: String,
	val threshold: Double,
	val topAnomalies: Int,
	val anomaliesFile: String,
	val anomalyIndex: Int
)

object IDSConfig{
	val parser = new scopt.OptionParser[IDSConfig]("ids") {
      head("ids", "1.0")
      opt[String]('b', "features").action( (x, c) => x match {
        case "BroSSH" => c.copy(features = Feature.getSSHFeatures())
        case "BroConn" =>  c.copy(features = Feature.getConnFeatures()) 
      }).text("Source of features from Bro. Default is BroSSH.").validate(x => 
        if(x=="BroSSH" || x=="BroConn") success else failure("Invalid parameter value"))
      opt[String]('e', "extractor").action( (x, c) =>
        c.copy(extractor = x) ).text("Type of entity extractor. Default is hostsWithIpFallback")
      opt[Duration]('i', "interval").action( (x, c) =>
        c.copy(interval = x) ).text("Interval for aggregation per src/dst entity in Scala Duration format. Default is 1 hour.")
      opt[String]('t', "trafficmode").action( (x, c) =>
        c.copy(trafficMode = x) ).text("With which entity to aggregate. Can be either src, dst or all.")
      opt[String]('f', "featuresfile").action( (x, c) =>
        c.copy(featuresFile = x) ).text("Parquet file to read/write the scaled features.")
      help("help").text("Prints this usage text.")

      cmd("writefeatures").action( (_, c) => c.copy(mode = "writefeatures") ).
        text("Extract features from the logs, scale them and write them to a parquet file.").
        children(
          opt[String]('l', "logspath").action( (x, c) =>
            c.copy(filePath = x) ).text("Input path for parquet log file(s)."),
          opt[String]('s', "scalemode").action( (x, c) =>
            c.copy(scaleMode = x) ).text("How to scale the features. Can be either unit or rescale.")
        )
      cmd("detectanomalies").action( (_, c) => c.copy(mode = "detectanomalies") ).
        text("Read the computed features, detects anomalies and write them to a parquet file.").
        children(
          opt[Double]('t', "threshold").action( (x, c) =>
            c.copy(threshold = x) ).text("Threshold between 0.0 and 1.0 above which logs are considered as anomalies."),
          opt[Int]('n', "nbtopanomalies").action( (x, c) =>
            c.copy(topAnomalies = x) ).text("Number of top anomalies to store."),
          opt[String]('f', "anomaliesfile").action( (x, c) =>
            c.copy(anomaliesFile = x) ).text("Parquet file where to write detected anomalies.")
        )
      cmd("inspectanomaly").action( (_, c) => c.copy(mode = "inspectanomaly") ).
        text("Inspect the logs for a specific already detected anomaly.").
        children(
          opt[String]('f', "anomaliesfile").action( (x, c) =>
            c.copy(anomaliesFile = x) ).text("Parquet file where to read detected anomalies."),
          opt[Int]('i', "anomalyindex").action( (x, c) =>
            c.copy(anomalyIndex = x) ).text("Index in the anomalies file of the anomaly that will be inspected.")
        )
    }
	def loadConf(args: Array[String], confFile: String):IDSConfig = {
		val config = ConfigFactory.parseFile(new File(confFile))
		val mode = config.getString("mode")
		val filePath = config.getString("logspath")
		val features = config.getString("features") match{
			case "BroSSH" => Feature.getSSHFeatures()
			case "BroConn" => Feature.getConnFeatures()
		}
		val extractor = config.getString("extractor")
		val interval = Duration(config.getString("aggregationtime"))
		val trafficMode = config.getString("trafficmode")
		val scaleMode = config.getString("scalemode")
		val featuresFile = config.getString("featuresfile")
		val threshold = config.getDouble("threshold")
		val topAnomalies = config.getInt("nbtopanomalies")
		val anomaliesFile = config.getString("anomaliesfile")
		val anomalyIndex = config.getInt("anomalyindex")
		val fromFile = IDSConfig(mode, filePath, features, extractor, interval, trafficMode, scaleMode,
			featuresFile, threshold, topAnomalies, anomaliesFile, anomalyIndex)

		parser.parse(args, fromFile).getOrElse{
			System.exit(1)
			fromFile
		}
	}
}