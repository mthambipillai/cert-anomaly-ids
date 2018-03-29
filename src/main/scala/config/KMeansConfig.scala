package config
import com.typesafe.config.Config

case class KMeansConfig(
	val trainRatio: Double,
	val minNbK: Int,
	val maxNbK: Int,
	val elbowRatio: Double,
	val nbK: Int,
	val lowBound: Long,
	val upBound: Long
)

object KMeansConfig{

	def load(conf: Config):KMeansConfig = {
		val trainRatio = conf.getDouble("trainratio")
		val minNbK = conf.getInt("minnbk")
		val maxNbK = conf.getInt("maxnbk")
		val elbowRatio = conf.getDouble("elbowratio")
		val nbK = conf.getInt("nbk")
		val lowBound = conf.getLong("lowbound")
		val upBound = conf.getLong("upbound")
		KMeansConfig(trainRatio, minNbK, maxNbK, elbowRatio, nbK, lowBound, upBound)
	}
}