package lof
import org.apache.spark.ml.outlier.LOF
import org.apache.spark.sql.DataFrame
import detection.Detector
import org.apache.spark.ml.feature.VectorAssembler

class LOFDetector(data: DataFrame) extends Detector{

	private val assembled = {
		println("\nStarting to build LOF model...\n")
		val featuresCols = data.columns.filter(_.contains("scaled"))
		val assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("features")
		assembler.transform(data)
	}

	private val lofModel = new LOF().setMinPts(5)

	override def detect(threshold: Double):DataFrame = {
		lofModel.transform(assembled).withColumnRenamed("lof", "lof_score")
	}
}