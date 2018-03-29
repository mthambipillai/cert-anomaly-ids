package detection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

class Ensembler(){

	def detectAndCombine(eType:String, threshold: Double, detectors: List[Detector]):DataFrame = {
		val detected = detectAll(threshold, detectors)
		println("Combining different detectors results...")
		combineAll(eType, detected)
	}

	def detectAll(threshold: Double, detectors: List[Detector]):Seq[DataFrame] = {
		detectors.map(_.detect(threshold))
	}

	def combineAll(eType: String, anomalies: Seq[DataFrame]):DataFrame = {
		val combined = anomalies.tail.foldLeft(anomalies.head){case (anoms1, anoms2) => combine2(eType, anoms1, anoms2)}
		combined.drop("scaled"+eType+"Index", "scaledtimeinterval")
	}

	private val meanUDF = udf((score1: Double, score2: Double) => (score1+score2)/2.0)
	private def combine2(eType: String, anomalies1: DataFrame, anomalies2: DataFrame):DataFrame = {
		val cols = anomalies2.columns.filterNot(c => c.contains("score")).toSeq
		val joined = anomalies1.join(anomalies2,cols, "fullouter")
		val anom1ScoreCol = anomalies1.columns.toList.last
		val anom2ScoreCol = anomalies2.columns.toList.last
		val filled1 = joined.na.fill(0.0, anom1ScoreCol :: Nil)
		val filled2 = filled1.na.fill(0.0, anom2ScoreCol :: Nil)
		filled2.withColumn("mean_"+anom1ScoreCol+"_"+anom2ScoreCol, 
			meanUDF(col(anom1ScoreCol), col(anom2ScoreCol))).drop(anom1ScoreCol, anom2ScoreCol)
	}
}