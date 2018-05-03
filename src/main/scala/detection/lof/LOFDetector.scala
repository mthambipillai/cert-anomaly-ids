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
		featuresCols.foreach(println(_))
		val assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("features")
		assembler.transform(data.limit(100000))
	}

	private val lofModel = new LOFtest(spark)

	override def detect(threshold: Double):DataFrame = {
		val lofs = mapToScores(lofModel.transform(assembled).drop("index"), maxScore, threshold)
		val res = assembled.join(lofs, assembled.col("features") === lofs.col("vector"), "inner")
		.drop("features").drop("vector")
		res.printSchema
		res
	}

	private def scoreUDF(maxScore: Double) = udf((x:Double) => {
		val temp = x/maxScore
		if(temp<1.0) temp else 1.0
	})
	private def mapToScores(lofs: DataFrame, maxScore: Double, threshold: Double):DataFrame = {
		val lofThreshold = threshold*maxScore
		println("new threshold : "+lofThreshold)
		val minMax = lofs.agg(min("lof"),max("lof")).head
		val minTime = minMax.getDouble(0)
		val maxTime = minMax.getDouble(1)
		println(minTime+" "+maxTime)

		lofs.filter(col("lof").geq(lofThreshold))
		.withColumn("score_lof",scoreUDF(maxScore)(col("lof"))).drop("lof")
	}
}