package config
import java.io.File
import com.typesafe.config.{ Config, ConfigFactory }
import features.Feature
import scala.concurrent.duration._

case class IDSConfig(
	val filePath: String,
	val features: List[Feature],
	val extractor: String,
	val interval: Duration,
	val trafficMode: String,
	val scaleMode: String
)

object IDSConfig{
	def loadConf(confFile: String):IDSConfig = {
		val config = ConfigFactory.parseFile(new File(confFile))
		val filePath = config.getString("filepath")
		val features = config.getString("features") match{
			case "BroSSH" => Feature.getSSHFeatures()
			case "BroConn" => Feature.getConnFeatures()
		}
		val extractor = config.getString("extractor")
		val interval = Duration(config.getString("aggregationtime"))
		val trafficMode = config.getString("trafficmode")
		val scaleMode = config.getString("scalemode")
		IDSConfig(filePath, features, extractor, interval, trafficMode, scaleMode)
	}
}