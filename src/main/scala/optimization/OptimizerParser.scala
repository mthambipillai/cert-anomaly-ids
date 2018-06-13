package optimization
import detection.Detector
import config.IDSConfig
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import scalaz._
import Scalaz._

class OptimizerParser(spark: SparkSession, features: DataFrame, conf: IDSConfig) extends Serializable{

	def getOptimizers(detectorName: String):String\/List[Optimizer]= {
		val functions = detectorName match {
			case "iforest" => List(Optimizer.iforestNbTrees _, Optimizer.iforestNbSamples _)
			case "kmeans" => List(Optimizer.kmeansNbClusters _)
			case "lof" => List(Optimizer.lofNbkNN _)
			case d => List(OptimizerParser.errorF(d) _)
		}
		functions.traverseU(f => Optimizer.buildOptimizer(spark, features, conf, f))
	}
}

object OptimizerParser{
	def errorF(d: String)(spark: SparkSession, data: DataFrame,
		conf: IDSConfig):String\/(String,List[Int],List[Detector]) = {
		("Unknown detector '"+d+"' to optimize").left
	}
}