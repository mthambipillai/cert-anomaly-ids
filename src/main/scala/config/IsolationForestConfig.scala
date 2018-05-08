package config
import com.typesafe.config.Config
import scalaz._

case class IsolationForestConfig(
	val nbTrees: Int,
	val nbSamples: Int
)

object IsolationForestConfig{

	def load(conf: Config):String\/IsolationForestConfig = {
		for{
			nbTrees <- IDSConfig.tryGet(conf.getInt)("nbtrees")
			nbSamples <- IDSConfig.tryGet(conf.getInt)("nbsamples")
		}yield IsolationForestConfig(nbTrees, nbSamples)
	}
}