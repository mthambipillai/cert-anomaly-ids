package config
import com.typesafe.config.Config
import scalaz._

case class LOFConfig(
	val k: Int,
	val hashNbDigits: Int,
	val hashNbVects: Int,
	val maxScore: Double
)

object LOFConfig{

	def load(conf: Config):String\/LOFConfig = {
		for{
			k <- IDSConfig.tryGet(conf.getInt)("k")
			hashNbDigits <- IDSConfig.tryGet(conf.getInt)("hashnbdigits")
			hashNbVects <- IDSConfig.tryGet(conf.getInt)("hashnbvects")
			maxScore <- IDSConfig.tryGet(conf.getInt)("maxscore")
		}yield LOFConfig(k, hashNbDigits, hashNbVects, maxScore)
	}
}