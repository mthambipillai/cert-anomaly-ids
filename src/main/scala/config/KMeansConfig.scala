package config
import com.typesafe.config.Config
import scalaz._

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

	def load(conf: Config):String\/KMeansConfig = {
		for{
			trainRatio <- IDSConfig.tryGet(conf.getDouble)("trainratio")
			minNbK <- IDSConfig.tryGet(conf.getInt)("minnbk")
			maxNbK <- IDSConfig.tryGet(conf.getInt)("maxnbk")
			elbowRatio <- IDSConfig.tryGet(conf.getDouble)("elbowratio")
			nbK <- IDSConfig.tryGet(conf.getInt)("nbk")
			lowBound <- IDSConfig.tryGet(conf.getLong)("lowbound")
			upBound <- IDSConfig.tryGet(conf.getLong)("upbound")
		}yield KMeansConfig(trainRatio, minNbK, maxNbK, elbowRatio, nbK, lowBound, upBound)
	}
}