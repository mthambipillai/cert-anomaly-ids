/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package detection
import scalaz._
import Scalaz._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.SaveMode

/*
Contains methods to apply different Detectors on the same data and combine
the different resulting scores into a single score.
*/
class Ensembler(){

	private val mean2UDF = udf((score1: Double, score2: Double) => (score1+score2))
	private def meanFinalUDF(n: Double) = udf((score: Double) => score/n)
	private val maxUDF = udf((score1: Double, score2: Double) => scala.math.max(score1, score2))

	/*
	Apply all detectors on the DataFrame passed to their constructor with 'threshold', then
	combine the different scores with the UDF defined by 'ensembleMode'.
	*/
	def detectAndCombine(eType:String, ensembleMode: String, threshold: Double,
		detectors: List[Detector]):String\/DataFrame = {
		for(
			(eUDF, finalUDFOpt) <- ensembleMode match{
						case "mean" => (mean2UDF, Some(meanFinalUDF(detectors.size))).right
						case "max" => (maxUDF, None).right
						case _ => ("Unknown ensembleMode "+ensembleMode).left
					}
		)yield{
			val detected = detectAll(threshold, detectors)
			if(detectors.size > 1){
				println("\nCombining different detectors results with mode '"+ensembleMode+"'...")
			}
			combineAll(eType, eUDF, threshold, detected, finalUDFOpt)
		}
	}

	/*
	Sorts the anomalies in 'detected' and writes the top 'nbTop' of them to 'destFile'.
	*/
	def persistAnomalies(detected: DataFrame, trafficMode: String, nbTop: Int, destFile: String):Unit = {
		val eType = trafficMode+"entity"
		val distDetected = detected.dropDuplicates(Array(trafficMode+"entity", "timeinterval"))
		val otherCols = detected.columns.toList.filterNot(c => c==eType || c=="timeinterval")
		val newCols = eType::("timeinterval"::otherCols)
		val scoreCol = newCols.filter(_.contains("score")).head
		val top = distDetected.sort(desc(scoreCol)).limit(nbTop).select(newCols.head, newCols.tail:_*)
		println("Writing top "+nbTop+" intrusions detected to "+destFile+".")
		top.write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv")
		.option("header", "true").save(destFile)
	}

	

	def detectAll(threshold: Double, detectors: List[Detector]):Seq[DataFrame] = {
		detectors.map(_.detect(threshold))
	}

	def combineAll(eType: String, eUDF: UserDefinedFunction, threshold: Double,
		anomalies: Seq[DataFrame], finalUDFOpt: Option[UserDefinedFunction]):DataFrame = {
		val combined = anomalies.tail.foldLeft(anomalies.head){case (anoms1, anoms2) => 
			combine2(eType, eUDF, threshold, anoms1, anoms2)}.drop("scaled"+eType+"Index", "scaledtimeinterval")
		finalUDFOpt match{
			case Some(finalUDF) => {
				val scoreColName = combined.columns.last
				combined.withColumn(scoreColName+"2", finalUDF(col(scoreColName)))
				.drop(scoreColName).withColumnRenamed(scoreColName+"2", scoreColName)
			}
			case None => combined
		}
	}

	/*
	Joins the 2 DataFrame 'anomalies1' and 'anomalies2' and combine their scores with 'eUDF'.
	*/
	private def combine2(eType: String, eUDF: UserDefinedFunction, threshold: Double,
		anomalies1: DataFrame, anomalies2: DataFrame):DataFrame = {
		val cols = anomalies2.columns.filterNot(c => c.contains("score")).toSeq
		val joined = anomalies1.join(anomalies2,cols, "fullouter")
		val anom1ScoreCol = anomalies1.columns.toList.last
		val anom2ScoreCol = anomalies2.columns.toList.last
		val filled1 = joined.na.fill(0.5, anom1ScoreCol :: Nil)//TODO : find a better way than just filling with 0.5
		val filled2 = filled1.na.fill(0.5, anom2ScoreCol :: Nil)//same
		val scoreColName = anom1ScoreCol+"_"+anom2ScoreCol
		val combined = filled2.withColumn(scoreColName, eUDF(col(anom1ScoreCol), col(anom2ScoreCol)))
			.drop(anom1ScoreCol, anom2ScoreCol)
		combined.filter(col(scoreColName).geq(threshold))
	}
}