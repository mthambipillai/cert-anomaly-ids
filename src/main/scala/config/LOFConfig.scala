package config
import com.typesafe.config.Config

case class LOFConfig(
	val k: Int,
	val hashNbDigits: Int,
	val maxScore: Double
)

object LOFConfig{

	def load(conf: Config):LOFConfig = {
		val k = conf.getInt("k")
		val hashNbDigits = conf.getInt("hashnbdigits")
		val maxScore = conf.getInt("maxscore")
		LOFConfig(k, hashNbDigits, maxScore)
	}
}