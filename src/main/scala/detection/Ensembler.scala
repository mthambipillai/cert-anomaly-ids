package detection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class Ensembler(){

	def detectAndCombine(eType:String, ensembleMode: String, threshold: Double, detectors: List[Detector]):DataFrame = {
		val detected = detectAll(threshold, detectors)
		println("\nCombining different detectors results with mode '"+ensembleMode+"'...")
		combineAll(eType, ensembleMode, detected)
	}

	def detectAll(threshold: Double, detectors: List[Detector]):Seq[DataFrame] = {
		detectors.map(_.detect(threshold))
	}

	def combineAll(eType: String, ensembleMode: String, anomalies: Seq[DataFrame]):DataFrame = {
		val combined = anomalies.tail.foldLeft(anomalies.head){case (anoms1, anoms2) => 
			combine2(eType, ensembleMode, anoms1, anoms2)}
		combined.drop("scaled"+eType+"Index", "scaledtimeinterval")
	}

	private val meanUDF = udf((score1: Double, score2: Double) => (score1+score2)/2.0)
	private val maxUDF = udf((score1: Double, score2: Double) => scala.math.max(score1, score2))
	private def combine2(eType: String, ensembleMode: String, anomalies1: DataFrame, anomalies2: DataFrame):DataFrame = {
		val cols = anomalies2.columns.filterNot(c => c.contains("score")).toSeq
		val joined = anomalies1.join(anomalies2,cols, "fullouter")
		val anom1ScoreCol = anomalies1.columns.toList.last
		val anom2ScoreCol = anomalies2.columns.toList.last
		val filled1 = joined.na.fill(0.5, anom1ScoreCol :: Nil)//TODO : find a better way than just filling with 0.5
		val filled2 = filled1.na.fill(0.5, anom2ScoreCol :: Nil)//same
		val eUDF = ensembleMode match{
			case "mean" => meanUDF
			case "max" => maxUDF
			case _ => throw new Exception("Unknown ensembleMode "+ensembleMode)
		}
		filled2.withColumn(ensembleMode+"_"+anom1ScoreCol+"_"+anom2ScoreCol, 
			eUDF(col(anom1ScoreCol), col(anom2ScoreCol))).drop(anom1ScoreCol, anom2ScoreCol)
	}
}