package kmeans
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import detectors.Detector
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

class KMeansDetector(spark: SparkSession, data: DataFrame) extends Detector{

	override def detect(threshold: Double = 0.5):DataFrame = {
		val featuresCols = data.columns.filter(_.contains("scaled"))
		val assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("features")
		val assembled = assembler.transform(data)
		val km = new KMeans().setK(4).setSeed(1L).setFeaturesCol("features").setPredictionCol("cluster")
		val model = getOptimizedModel(km, assembled.sample(true, 0.1))
		val withClusters = model.transform(assembled).drop("features")
		val sizes = model.summary.clusterSizes.toList
		val withScores = mapToScores(withClusters, sizes)
		withScores.filter(withScores("score").leq(lit(1-threshold)))
	}

	private def mapToScores(df: DataFrame, sizes: List[Long]):DataFrame = {
		val maxSize = sizes.max.toDouble
		val minSize = sizes.min.toDouble
		val diff = (maxSize - minSize).toDouble
		val scaledSizes = sizes.zipWithIndex.map{case (s,cIndex) => (cIndex,(s - minSize)/diff)}.toMap
		val scaledSizesB = spark.sparkContext.broadcast(scaledSizes)
		val clusterColIndexB = spark.sparkContext.broadcast(df.columns.size - 1)
		println("Found the following cluster sizes : "+sizes)
		val encoder = RowEncoder(df.schema)
		println("Computing scores...")
		val res = df.mapPartitions{iter =>
			val f = scaledSizesB.value
			val clusterColIndex = clusterColIndexB.value
			iter.map{r => 
				val seq = r.toSeq
				val clusterId = seq(clusterColIndex).asInstanceOf[Int]
				Row.fromSeq(seq :+ f(clusterId))
			}
		}(encoder)
		res.withColumnRenamed("cluster","score")
	}

	private def getOptimizedModel(km: KMeans, train: DataFrame):KMeansModel = {
		var nbK = 4
		var model = km.setK(nbK).fit(train)
		var prevWSSSE = model.computeCost(train)
		nbK = nbK + 1
		model = km.setK(nbK).fit(train)
		var newWSSSE = model.computeCost(train)
		var oldDiff = prevWSSSE - newWSSSE
		var newDiff = 0.0
		var ratio = 0.3
		prevWSSSE = newWSSSE
		while(ratio >= 0.3 && ratio < 1.0 && nbK<50){
			nbK = nbK + 1
			model = km.setK(nbK).fit(train)
			newWSSSE = model.computeCost(train)
			newDiff = prevWSSSE - newWSSSE
			ratio = newDiff/oldDiff
			oldDiff = newDiff
			prevWSSSE = newWSSSE
			println(ratio+" "+newDiff)
		}
		model
	}
}