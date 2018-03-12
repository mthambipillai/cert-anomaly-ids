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
	val featuresFile: String
)

object IDSConfig{
	val parser = new scopt.OptionParser[IDSConfig]("ids") {
      head("ids", "1.0")
      opt[String]('f', "features").action( (x, c) => x match {
        case "BroSSH" => c.copy(features = Feature.getSSHFeatures())
        case "BroConn" =>  c.copy(features = Feature.getConnFeatures()) 
      }).text("Source of features. Default is BroSSH.").validate(x => 
        if(x=="BroSSH" || x=="BroConn") success else failure("Invalid parameter value"))
      opt[String]('e', "extractor").action( (x, c) =>
        c.copy(extractor = x) ).text("Type of entity extractor. Default is hostsWithIpFallback")
      opt[Duration]('i', "interval").action( (x, c) =>
        c.copy(interval = x) ).text("Interval for aggregation per src/dst entity in Scala Duration format. Default is 1 hour.")
      opt[String]('t', "trafficmode").action( (x, c) =>
        c.copy(trafficMode = x) ).text("With which entity to aggregate. Can be either src, dst or all.")
      opt[String]('t', "featuresfile").action( (x, c) =>
        c.copy(featuresFile = x) ).text("Parquet file to read/write the scaled features.")
      help("help").text("Prints this usage text.")

      cmd("writefeatures").action( (_, c) => c.copy(mode = "writefeatures") ).
        text("extract features from the logs, scale them and write them to a parquet file.").
        children(
          opt[String]('l', "logspath").action( (x, c) =>
            c.copy(filePath = x) ).text("Input path for parquet log file(s)."),
          opt[String]('s', "scalemode").action( (x, c) =>
            c.copy(scaleMode = x) ).text("How to scale the features. Can be either unit or rescale.")
        )
      cmd("detectanomalies").action( (_, c) => c.copy(mode = "detectanomalies") ).
        text("read the computed features and detects anomalies.")
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
		val fromFile = IDSConfig(mode, filePath, features, extractor, interval, trafficMode, scaleMode, featuresFile)

		parser.parse(args, fromFile).getOrElse{throw new Exception("Invalid command or parameter.")}
	}
}