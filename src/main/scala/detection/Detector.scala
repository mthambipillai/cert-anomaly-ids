package detection
import org.apache.spark.sql.DataFrame
import config.IDSConfig
import isolationforest.IsolationForest
import kmeans.KMeansDetector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
Any intrusion detection algorithm must extend this class.
*/
abstract class Detector(){
	/*
	Computes a DataFrame of detected intrusions along with their scores. The
	original DataFrame must be defined by the constructor of the concrete class.
	*/
	def detect(threshold: Double):DataFrame
}

object Detector{
	def getDetector(spark: SparkSession, name: String, conf: IDSConfig, features: DataFrame):Detector = name match {
		case "iforest" => {
			val stats = spark.read.parquet(conf.featuresFile+"-stats.parquet")
			val count = stats.filter(col("summary")===lit("count")).agg(sum(stats.columns(1))).first.getDouble(0).toLong
			new IsolationForest(spark, features, count, conf.isolationForest.nbTrees, conf.isolationForest.nbSamples)
		}
		case "kmeans" => new KMeansDetector(spark, features, conf.kMeans.trainRatio, conf.kMeans.minNbK, 
			conf.kMeans.maxNbK, conf.kMeans.elbowRatio, conf.kMeans.nbK, conf.kMeans.lowBound, conf.kMeans.upBound)
		case "lof" => ???
		case _ => throw new Exception("Detector '"+name+"' does not exist.")
	}

	def getDetectors(spark: SparkSession, conf: IDSConfig, features: DataFrame):List[Detector] = {
		conf.detectors.split(",").toList.map(name => getDetector(spark, name, conf, features))
	}
}