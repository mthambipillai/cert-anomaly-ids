package detection
import org.apache.spark.sql.DataFrame
import config.IDSConfig
import isolationforest.IsolationForest
import kmeans.KMeansDetector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Try
import scalaz._
import Scalaz._

/*
A Detector applies an anomaly detection algorithm to a DataFrame to assign scores
to rows and find row above some anomaly threshold.
Any new class for anomaly detection must extend this class.
*/
abstract class Detector(){
	/*
	Computes a DataFrame of detected anomalies along with their scores between 'threshold' and 1.0
	since scores below 'threshold' are not considered anomalies. The original DataFrame must be
	defined by the constructor of the concrete class.
	*/
	def detect(threshold: Double):DataFrame
}

object Detector{

	def getDetector(spark: SparkSession, name: String, conf: IDSConfig, features: DataFrame):String\/Detector ={
		name match{
			case "iforest" => {
				val fileName = conf.featuresFile+"-stats.parquet"
				for(
					stats <- Try(spark.read.parquet(fileName)).toDisjunction.leftMap(e =>
            			"Could not read '"+fileName+"' because of "+e.getMessage)
				)yield{
					val count = stats.filter(col("summary")===lit("count")).agg(sum(stats.columns(1)))
					.first.getDouble(0).toLong
					new IsolationForest(spark, features, count, conf.isolationForest.nbTrees,
						conf.isolationForest.nbSamples)
				}
			}
			case "kmeans" => {
				val km = new KMeansDetector(spark, features, conf.kMeans.trainRatio, conf.kMeans.minNbK, 
				conf.kMeans.maxNbK, conf.kMeans.elbowRatio, conf.kMeans.nbK, conf.kMeans.lowBound,
				conf.kMeans.upBound)
				km.right
			}
			case "lof" => ???
			case _ => ("Detector '"+name+"' does not exist.").left
		}
	}

	def getDetectors(spark: SparkSession, conf: IDSConfig, features: DataFrame):String\/List[Detector] = {
		conf.detectors.split(",").toList.traverseU(name => getDetector(spark, name, conf, features))
	}
}