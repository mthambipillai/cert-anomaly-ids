package config
import com.typesafe.config.Config

case class KMeansConfig(
	val trainRatio: Double,
	val minNbK: Int,
	val maxNbK: Int,
	val elbowRatio: Double
)

object KMeansConfig{

	def load(conf: Config):KMeansConfig = {
		val trainRatio = conf.getDouble("trainratio")
		val minNbK = conf.getInt("minnbk")
		val maxNbK = conf.getInt("maxnbk")
		val elbowRatio = conf.getDouble("elbowratio")
		KMeansConfig(trainRatio, minNbK, maxNbK, elbowRatio)
	}
}