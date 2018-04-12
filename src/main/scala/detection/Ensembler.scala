package detection
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scalaz._
import Scalaz._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.UserDefinedFunction

class Ensembler(){

	private val meanUDF = udf((score1: Double, score2: Double) => (score1+score2)/2.0)
	private val maxUDF = udf((score1: Double, score2: Double) => scala.math.max(score1, score2))
	def detectAndCombine(eType:String, ensembleMode: String, threshold: Double,
		detectors: List[Detector]):String\/DataFrame = {
		for(
			eUDF <- ensembleMode match{
						case "mean" => meanUDF.right
						case "max" => maxUDF.right
						case _ => ("Unknown ensembleMode "+ensembleMode).left
					}
		)yield{
			val detected = detectAll(threshold, detectors)
			println("\nCombining different detectors results with mode '"+ensembleMode+"'...")
			combineAll(eType, eUDF, threshold, detected)
		}
	}

	def detectAll(threshold: Double, detectors: List[Detector]):Seq[DataFrame] = {
		detectors.map(_.detect(threshold))
	}

	def combineAll(eType: String, eUDF: UserDefinedFunction, threshold: Double, anomalies: Seq[DataFrame]):DataFrame = {
		val combined = anomalies.tail.foldLeft(anomalies.head){case (anoms1, anoms2) => 
			combine2(eType, eUDF, threshold, anoms1, anoms2)}
		combined.drop("scaled"+eType+"Index", "scaledtimeinterval")
	}

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