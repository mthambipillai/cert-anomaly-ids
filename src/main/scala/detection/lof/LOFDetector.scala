package lof
import org.apache.spark.sql.DataFrame
import detection.Detector
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

class LOFDetector(spark: SparkSession, data: DataFrame, k: Int, maxScore: Double) extends Detector{

	private val assembled = {
		println("\nStarting to build LOF model...\n")
		val featuresCols = data.columns.filter(_.contains("scaled"))
		val assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("features")
		assembler.transform(data)
	}

	private val lofModel = new LOFtest(spark, k)

	override def detect(threshold: Double):DataFrame = {
		mapToScores(lofModel.transform(assembled).drop("features"), maxScore, threshold)
	}

	private def scoreUDF(maxScore: Double) = udf((x:Double) => {
		val temp = x/maxScore
		if(temp<1.0) temp else 1.0
	})
	private def mapToScores(lofs: DataFrame, maxScore: Double, threshold: Double):DataFrame = {
		//val lofThreshold = threshold*maxScore
		//println("new threshold : "+lofThreshold)
		val minMax = lofs.agg(min("lof"),max("lof")).head
		val minLOF = minMax.getDouble(0)
		val maxLOF = minMax.getDouble(1)
		println(minLOF+" "+maxLOF)
		val lofThreshold = threshold*maxLOF
		
		lofs.filter(col("lof").geq(lofThreshold))
		.withColumn("score_lof", scoreUDF(maxLOF)(col("lof"))).drop("lof")
	}
}