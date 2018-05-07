package config
import java.io.File
import com.typesafe.config.ConfigFactory
import features.Feature
import features.FeaturesParser
import scala.concurrent.duration._
import inspection.Rule
import inspection.RulesParser
import evaluation.IntrusionKind
import evaluation.IntrusionsParser
import scala.util.Try
import scalaz._
import Scalaz._

case class IDSConfig(
	//Global parameters
	val mode: String,
	val logLevel: String,
	val filePath: String,
	val featuresschema: List[Feature],
	val extractor: String,
	val interval: Duration,
	val trafficMode: String,
	val scaleMode: String,
	val ensembleMode: String,
	val featuresFile: String,
	val featuresStatsFile : String,
	val detectors: String,
	val threshold: Double,
	val topAnomalies: Int,
	val anomaliesFile: String,
	val rules: List[Rule],
	val inspectionResults: String,
	val recall: Boolean,
	val intrusions: List[(IntrusionKind,Int)],
	val intrusionsDir: String,
	//IsolationForest parameters
	val isolationForest: IsolationForestConfig,
	//KMeans parameters
	val kMeans: KMeansConfig
)

object IDSConfig{
	val parser = new scopt.OptionParser[IDSConfig]("cert-anomaly-ids") {
		head("CERT-Anomaly-IDS", "1.0")
		opt[String]('e', "extractor").action( (x, c) =>
			c.copy(extractor = x) ).text("Type of entity extractor. Default is hostsWithIpFallback")
		opt[String]('f', "featuresschema").action( (x, c) =>
			c.copy(featuresschema = FeaturesParser.parse(x).getOrElse(Nil)) ).text("Source of features.")
		opt[Duration]('i', "interval").action( (x, c) =>
			c.copy(interval = x) ).text("Interval for aggregation per src/dst entity in Scala Duration format. Default is '60 min'.")
		opt[String]('l', "loglevel").action( (x, c) =>
			c.copy(logLevel = x) ).text("Log level of the console.")
		opt[String]('t', "trafficmode").action( (x, c) =>
			c.copy(trafficMode = x) ).text("With which entity to aggregate. Can be either src or dst.")
		help("help").text("Prints this usage text.")

		cmd("extract").action( (_, c) => c.copy(mode = "extract") ).
		text("Extract features from the logs, scale them and write them to a parquet file.").
		children(
			opt[String]('l', "logspath").action( (x, c) =>
				c.copy(filePath = x) ).text("Input path for parquet log file(s)."),
			opt[String]('m', "scalemode").action( (x, c) =>
				c.copy(scaleMode = x) ).text("How to scale the features. Can be either unit or rescale."),
			opt[String]('f', "featuresfile").action( (x, c) =>
				c.copy(featuresFile = x) ).text("Parquet file to write the scaled features to."),
			opt[String]('s', "featuresstatsfile").action( (x, c) =>
				c.copy(featuresStatsFile = x) ).text("Parquet file to write the features' statistics to."),
			opt[Boolean]('r', "recall").action( (x, c) =>
				c.copy(recall = x) ).text("True if intrusions should be injected to compute recall."),
			opt[String]('i', "intrusions").action( (x, c) =>
				c.copy(intrusions = IntrusionsParser.parse(x).getOrElse(Nil)) ).text("Source of intrusions."),
			opt[String]('d', "intrusionsdir").action( (x, c) =>
				c.copy(intrusionsDir = x) ).text("Folder to write the injected intrusions to.")
		)
		cmd("detect").action( (_, c) => c.copy(mode = "detect") ).
		text("Read the computed features, detects anomalies and write them to a CSV file.").
		children(
			opt[String]('f', "featuresfile").action( (x, c) =>
				c.copy(featuresFile = x) ).text("Parquet file to read the scaled features from."),
			opt[String]('s', "featuresstatsfile").action( (x, c) =>
				c.copy(featuresStatsFile = x) ).text("Parquet file to read the features' statistics from."),
			opt[String]('d', "detectors").action( (x, c) =>
				c.copy(detectors = x) ).text("Detectors to use. Names separated by commas."),
			opt[Double]('t', "threshold").action( (x, c) =>
				c.copy(threshold = x) )
			.text("Threshold between 0.0 and 1.0 above which logs are considered as anomalies.").validate( x => {
				if(x>=0.0 && x<=1.0) success else failure("Value must be between 0.0 and 1.0.")
			}),
			opt[Int]('n', "nbtopanomalies").action( (x, c) =>
				c.copy(topAnomalies = x) ).text("Number of top anomalies to store.").validate( x => {
					if(x>0 && x<=1000) success else failure("Value must be between 1 and 1000.")
				}),
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
				c.copy(rules = RulesParser.parse(x).getOrElse(Nil)) ).text("Source of rules."),
			opt[String]('i', "inspectionfiles").action( (x, c) =>
				c.copy(inspectionResults = x) ).text("CSV files to write the results of the inspection to."),
			opt[String]('d', "intrusionsdir").action( (x, c) =>
				c.copy(intrusionsDir = x) ).text("Folder to read the injected intrusions from.")
		)
	}

	/*
	Returns an IDSConfig first parsed from 'confFile' and then overriden with the command
	line arguments 'args'.
	*/
	def loadConf(args: Array[String], confFile: String):String\/IDSConfig = {
		for{
			configTemp <- Try(ConfigFactory.parseFile(new File(confFile))).toDisjunction.leftMap(e =>
				"Could not parse '"+confFile+"' because of "+e.getMessage)
			config = configTemp.resolve()
			filePath = config.getString("logspath")
			logLevel = config.getString("loglevel")
			featuresschema <- FeaturesParser.parse(config.getString("featuresschema"))
			extractor = config.getString("extractor")
			interval <- Try(Duration(config.getString("aggregationtime"))).toDisjunction.leftMap(e =>
				"Could not parse duration because of "+e.getMessage)
			trafficMode = config.getString("trafficmode")
			scaleMode = config.getString("scalemode")
			ensembleMode = config.getString("ensemblemode")
			featuresFile = config.getString("featuresfile")
			featuresStatsFile = config.getString("featuresstatsfile")
			detectors = config.getString("detectors")
			threshold <- Try(config.getDouble("threshold")).toDisjunction.leftMap(e =>
				"Could not parse double for 'threshold'")
			trafficMode = config.getString("trafficmode")
			topAnomalies <- Try(config.getInt("nbtopanomalies")).toDisjunction.leftMap(e =>
				"Could not parse int for 'topAnomalies'")
			anomaliesFile = config.getString("anomaliesfile")
			rules <- RulesParser.parse(config.getString("rules"))
			inspectionResults = config.getString("inspectionfiles")
			recall <- Try(config.getBoolean("recall")).toDisjunction.leftMap(e =>
				"Could not parse boolean for 'recall'")
			intrusions <- IntrusionsParser.parse(config.getString("intrusions"))
			intrusionsDir = config.getString("intrusionsdir")
			isolationForest = IsolationForestConfig.load(config)
			kMeans = KMeansConfig.load(config)

			fromFile = IDSConfig("", logLevel, filePath, featuresschema, extractor, interval, trafficMode, scaleMode,
				ensembleMode, featuresFile, featuresStatsFile, detectors, threshold, topAnomalies, anomaliesFile,
				rules, inspectionResults, recall, intrusions, intrusionsDir, isolationForest, kMeans)
			res <- parser.parse(args, fromFile).toRightDisjunction("Unable to parse cli arguments.")
			checkFeatures <- if(res.featuresschema.isEmpty) "Could not parse features.".left else res.right
			checkRules <- if(checkFeatures.rules.isEmpty) "Could not parse rules.".left else checkFeatures.right
			checkIntrusions <- if(checkRules.intrusions.isEmpty) "Could not parse intrusions.".left else checkRules.right
		}yield checkIntrusions
	}
}