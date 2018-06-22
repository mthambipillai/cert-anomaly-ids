/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package config
import java.io.File
import com.typesafe.config.ConfigFactory
import features.FeaturesParser
import scala.concurrent.duration._
import inspection.RulesParser
import evaluation.IntrusionsParser
import org.apache.spark.sql.SparkSession
import scala.util.Try
import scalaz._
import Scalaz._

class IDSConfigParser(spark: SparkSession){
	val parser = new scopt.OptionParser[IDSConfig]("cert-anomaly-ids") {
		head("CERT-Anomaly-IDS", "1.0")
		opt[String]("extractor").action( (x, c) =>
			c.copy(extractor = x) ).text("Type of entity extractor. Default is hostsWithIpFallback")
		opt[String]("featuresschema").action( (x, c) =>
			c.copy(featuresschema = new FeaturesParser(spark).parse(x).getOrElse(null)) ).text("Source of features.")
		opt[Duration]("interval").action( (x, c) =>
			c.copy(interval = x) ).text("Interval for aggregation per src/dst entity in Scala Duration format. Default is '60 min'.")
		opt[String]("loglevel").action( (x, c) =>
			c.copy(logLevel = x) ).text("Log level of the console.")
		opt[String]("trafficmode").action( (x, c) =>
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
				c.copy(intrusions = IntrusionsParser.parse(x).getOrElse(null)) ).text("Source of intrusions."),
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
				c.copy(rules = RulesParser.parse(x).getOrElse(null)) ).text("Source of rules."),
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
	def loadConf(args: Array[String], appLoaderFile: String):String\/IDSConfig = {
		for{
			appLoader <- Try(ConfigFactory.parseFile(new File(appLoaderFile))).toDisjunction.leftMap(e =>
				"Could not parse '"+appLoaderFile+"' because of "+e.getMessage).map(_.resolve())
			confFile <- tryGet(appLoader.getString)("configpath")
			config <- Try(ConfigFactory.parseFile(new File(confFile))).toDisjunction.leftMap(e =>
				"Could not parse '"+confFile+"' because of "+e.getMessage).map(_.resolve())
			filePath <- tryGet(config.getString)("logspath")
			logLevel <- tryGet(config.getString)("loglevel")
			featuresschema <- tryGet(config.getString)("featuresschema").flatMap(f => 
				new FeaturesParser(spark).parse(f))
			extractor <- tryGet(config.getString)("extractor")
			interval <- tryGet(config.getString)("interval").flatMap(i =>
				Try(Duration(i)).toDisjunction.leftMap(e => "Could not parse duration because of "+e.getMessage))
			trafficMode <- tryGet(config.getString)("trafficmode")
			scaleMode <- tryGet(config.getString)("scalemode")
			ensembleMode <- tryGet(config.getString)("ensemblemode")
			featuresFile <- tryGet(config.getString)("featuresfile")
			featuresStatsFile <- tryGet(config.getString)("featuresstatsfile")
			detectors <- tryGet(config.getString)("detectors")
			threshold <- tryGet(config.getDouble)("threshold")
			trafficMode <- tryGet(config.getString)("trafficmode")
			topAnomalies <- tryGet(config.getInt)("nbtopanomalies")
			anomaliesFile <- tryGet(config.getString)("anomaliesfile")
			rules <- tryGet(config.getString)("rules").flatMap(r => RulesParser.parse(r))
			inspectionResults <- tryGet(config.getString)("inspectionfiles")
			recall <- tryGet(config.getBoolean)("recall")
			intrusions <- tryGet(config.getString)("intrusions").flatMap(i => IntrusionsParser.parse(i)) 
			intrusionsDir <- tryGet(config.getString)("intrusionsdir")
			isolationForest <- IsolationForestConfig.load(config)
			kMeans <- KMeansConfig.load(config)
			lof <- LOFConfig.load(config)

			fromFile = IDSConfig("", logLevel, filePath, featuresschema, extractor, interval, trafficMode, scaleMode,
				ensembleMode, featuresFile, featuresStatsFile, detectors, threshold, topAnomalies, anomaliesFile,
				rules, inspectionResults, recall, intrusions, intrusionsDir, isolationForest, kMeans, lof)
			res <- parser.parse(args, fromFile).toRightDisjunction("Unable to parse cli arguments.")
			checkFeatures <- if(res.featuresschema == null) "Could not parse features.".left else res.right
			checkRules <- if(checkFeatures.rules == null) "Could not parse rules.".left else checkFeatures.right
			checkIntrusions <- if(checkRules.intrusions == null) "Could not parse intrusions.".left else checkRules.right
		}yield checkIntrusions
	}

	def tryGet[T](get: String => T)(paramName: String):String\/T = {
		Try(get(paramName)).toDisjunction.leftMap(e => e.getMessage)
	}
}