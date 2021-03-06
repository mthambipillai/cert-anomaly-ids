/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package lof
import org.apache.spark.sql.DataFrame
import detection.Detector
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

/*
This detector implements the LOF algorithm and maps the scores to a new scale.
*/
class LOFDetector(spark: SparkSession, data: DataFrame, k: Int, hashNbDigits: Int,
	hashNbVects: Int, knownMaxScore: Double) extends Detector{

	private val assembled = {
		println("\nStarting to build Local Outlier Factor model...\n")
		val featuresCols = data.columns.filter(_.contains("scaled"))
		val assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("features")
		assembler.transform(data)
	}

	private val lofModel = new LOF(spark, k, hashNbDigits, hashNbVects)

	override def detect(threshold: Double):DataFrame = {
		println("\nStarting to detect with Local Outlier Factor...\n")
		mapToScoresAndFilter(lofModel.transform(assembled).drop("features"), threshold)
	}

	private def scoreUDF(maxScore: Double) = udf((x:Double) => {
		val temp = x/maxScore
		if(temp<1.0) temp else 1.0
	})

	/*
	The LOF scores are on an open scale so they need to be mapped between 0.0 and 1.0
	From the maximum score and the threshold, the threshold score can be computed to filter
	and keep only the anomalies. The new scores are added in a new 'lof_score' column.
	*/
	private def mapToScoresAndFilter(lofs: DataFrame, threshold: Double):DataFrame = {
		val maxScore = if(knownMaxScore == -1){
			val minMax = lofs.agg(min("lof"),max("lof")).head
			val maxS = minMax.getDouble(1)
			println("Max LOF score is : "+maxS)
			maxS
		}else{
			knownMaxScore
		}
		val lofThreshold = threshold*maxScore

		lofs.filter(col("lof").geq(lofThreshold))
		.withColumn("lof_score", scoreUDF(maxScore)(col("lof"))).drop("lof")
	}
}