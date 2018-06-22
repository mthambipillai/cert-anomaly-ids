/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
*/
package detection
import org.apache.spark.sql.DataFrame
import config.IDSConfig
import isolationforest.IsolationForest
import kmeans.KMeansDetector
import lof.LOFDetector
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
				val statsFileName = conf.featuresStatsFile+".parquet"
				for(
					stats <- Try(spark.read.parquet(statsFileName)).toDisjunction.leftMap(e =>
            			"Could not read '"+statsFileName+"' because of "+e.getMessage)
				)yield{
					val count = stats.filter(col("summary")===lit("count")).agg(sum(stats.columns(1)))
					.first.getDouble(0).toLong
					new IsolationForest(spark, features, count, conf.isolationForest.nbTrees,
						conf.isolationForest.nbSamples)
				}
			}
			case "kmeans" => {
				new KMeansDetector(spark, features, conf.kMeans.trainRatio, conf.kMeans.minNbK, 
					conf.kMeans.maxNbK, conf.kMeans.elbowRatio, conf.kMeans.nbK, conf.kMeans.lowBound,
					conf.kMeans.upBound).right
			}
			case "lof" => {
				new LOFDetector(spark, features, conf.lof.k, conf.lof.hashNbDigits,
					conf.lof.hashNbVects, conf.lof.maxScore).right
			}
			case _ => ("Detector '"+name+"' does not exist.").left
		}
	}

	def getDetectors(spark: SparkSession, conf: IDSConfig, features: DataFrame):String\/List[Detector] = {
		conf.detectors.split(",").toList.traverseU(name => getDetector(spark, name, conf, features))
	}
}
