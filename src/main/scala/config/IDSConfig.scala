package config
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
import features.Feature
import features.FeaturesParser
import scala.concurrent.duration._
import inspection.Rule
import inspection.RulesParser

case class IDSConfig(
  //Global parameters
	val mode: String,
	val filePath: String,
	val features: List[Feature],
	val extractor: String,
	val interval: Duration,
	val trafficMode: String,
	val scaleMode: String,
  val ensembleMode: String,
  val featuresFile: String,
  val detectors: String,
	val threshold: Double,
	val topAnomalies: Int,
	val anomaliesFile: String,
	val anomalyIndex: Int,
  val rules: List[Rule],
  val inspectionResults: String,
  //IsolationForest parameters
  val isolationForest: IsolationForestConfig,
  //KMeans parameters
  val kMeans: KMeansConfig
)

object IDSConfig{
	val parser = new scopt.OptionParser[IDSConfig]("ids") {
      head("ids", "1.0")
      opt[String]('f', "features").action( (x, c) =>
        c.copy(features = FeaturesParser.parse(x)) ).text("Source of features.")
      opt[String]('e', "extractor").action( (x, c) =>
        c.copy(extractor = x) ).text("Type of entity extractor. Default is hostsWithIpFallback")
      opt[Duration]('i', "interval").action( (x, c) =>
        c.copy(interval = x) ).text("Interval for aggregation per src/dst entity in Scala Duration format. Default is 1 hour.")
      opt[String]('t', "trafficmode").action( (x, c) =>
        c.copy(trafficMode = x) ).text("With which entity to aggregate. Can be either src or dst.")
      help("help").text("Prints this usage text.")

      cmd("extract").action( (_, c) => c.copy(mode = "extract") ).
        text("Extract features from the logs, scale them and write them to a parquet file.").
        children(
          opt[String]('l', "logspath").action( (x, c) =>
            c.copy(filePath = x) ).text("Input path for parquet log file(s)."),
          opt[String]('s', "scalemode").action( (x, c) =>
            c.copy(scaleMode = x) ).text("How to scale the features. Can be either unit or rescale."),
          opt[String]('f', "featuresfile").action( (x, c) =>
            c.copy(featuresFile = x) ).text("Parquet file to write the scaled features to.")
        )
      cmd("detect").action( (_, c) => c.copy(mode = "detect") ).
        text("Read the computed features, detects anomalies and write them to a CSV file.").
        children(
          opt[String]('f', "featuresfile").action( (x, c) =>
            c.copy(featuresFile = x) ).text("Parquet file to read the scaled features from."),
          opt[String]('d', "detectors").action( (x, c) =>
            c.copy(detectors = x) ).text("Detectors to use. Names separated by commas."),
          opt[Double]('t', "threshold").action( (x, c) =>
            c.copy(threshold = x) ).text("Threshold between 0.0 and 1.0 above which logs are considered as anomalies."),
          opt[Int]('n', "nbtopanomalies").action( (x, c) =>
            c.copy(topAnomalies = x) ).text("Number of top anomalies to store."),
          opt[String]('a', "anomaliesfile").action( (x, c) =>
            c.copy(anomaliesFile = x) ).text("CSV file to write the detected anomalies to."),
          opt[String]('e', "ensemblemode").action( (x, c) =>
            c.copy(ensembleMode = x) ).text("Determines how detectors' scores are combined. mean or max.")
        )
      cmd("inspect").action( (_, c) => c.copy(mode = "inspect") ).
        text("Inspects all the logs for every anomaly detected.").
        children(
          opt[String]('a', "anomaliesfile").action( (x, c) =>
            c.copy(anomaliesFile = x) ).text("CSV file to read the detected anomalies from."),
          opt[String]('r', "rules").action( (x, c) =>
            c.copy(rules = RulesParser.parse(x)) ).text("Source of rules."),
          opt[String]('i', "inspectionresultsfile").action( (x, c) =>
            c.copy(inspectionResults = x) ).text("CSV file to write the results of the inspection.")
        )
    }
	def loadConf(args: Array[String], confFile: String):IDSConfig = {
		val config = ConfigFactory.parseFile(new File(confFile))
		val mode = config.getString("mode")
		val filePath = config.getString("logspath")
		val features = FeaturesParser.parse(config.getString("features"))
		val extractor = config.getString("extractor")
		val interval = Duration(config.getString("aggregationtime"))
		val trafficMode = config.getString("trafficmode")
		val scaleMode = config.getString("scalemode")
    val ensembleMode = config.getString("ensemblemode")
		val featuresFile = config.getString("featuresfile")
    val detectors = config.getString("detectors")
		val threshold = config.getDouble("threshold")
		val topAnomalies = config.getInt("nbtopanomalies")
		val anomaliesFile = config.getString("anomaliesfile")
		val anomalyIndex = config.getInt("anomalyindex")
    val rules = RulesParser.parse(config.getString("rules"))
    val inspectionResults = config.getString("resultsfile")
    val isolationForest = IsolationForestConfig.load(config)
    val kMeans = KMeansConfig.load(config)
		val fromFile = IDSConfig(mode, filePath, features, extractor, interval, trafficMode, scaleMode, ensembleMode,
			featuresFile, detectors, threshold, topAnomalies, anomaliesFile, anomalyIndex, rules, inspectionResults,
      isolationForest, kMeans)

		parser.parse(args, fromFile).getOrElse{
			System.exit(1)
			fromFile
		}
	}
}