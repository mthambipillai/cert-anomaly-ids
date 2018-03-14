package config
import com.typesafe.config.Config

case class IsolationForestConfig(
	val nbTrees: Int,
	val nbSamples: Int
)

object IsolationForestConfig{

	def load(conf: Config):IsolationForestConfig = {
		val nbTrees = conf.getInt("nbtrees")
		val nbSamples = conf.getInt("nbsamples")
		IsolationForestConfig(nbTrees, nbSamples)
	}
}