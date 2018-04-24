package kmeans
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.clustering.KMeansModel
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.DataFrame
import detection.Detector
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
/*
This detector implements the KMeans clustering algorithm. Smaller clusters tend to be anomalies.
*/
class KMeansDetector(spark: SparkSession, data: DataFrame, trainRatio: Double, minNbK: Int,
	maxNbK: Int, elbowRatio: Double, nbK: Int, lowerBoundSize: Long, upperBoundSize: Long) extends Detector{

	private val assembled = {
		println("\nStarting to build KMeans model...\n")
		val featuresCols = data.columns.filter(_.contains("scaled"))
		val assembler = new VectorAssembler().setInputCols(featuresCols).setOutputCol("features")
		assembler.transform(data)
	}

	private val model = {
		val km = new KMeans().setK(minNbK).setSeed(1L).setFeaturesCol("features").setPredictionCol("cluster")
		val trainSet = assembled.sample(true, trainRatio)
		if(nbK == -1){
			println("Computing number of clusters with the elbow technique...")
			println("Computing clusters from trainset...")
			getOptimizedModel(km, trainSet)
		}else{
			println("Computing clusters from trainset...")
			km.setK(nbK).fit(trainSet)
		}
	}

	override def detect(threshold: Double):DataFrame = {
		println("\nStarting to detect with KMeans...\n")
		println("Computing cluster sizes...")
		val withClusters = model.transform(assembled).drop("features")
		val sizes = model.summary.clusterSizes.toList
		val withScores = mapToScores(withClusters, sizes, lowerBoundSize, upperBoundSize)
		println("Returning anomalies with score above "+threshold+"...")
		withScores.filter(withScores("km_score").geq(lit(threshold)))
	}

	/*
	Returns a new DataFrame with a score column given 'df' which has a column with assigned cluster id. 'sizes'
	contains the different cluster sizes. Every size below 'lowerBoundSize' maps to score 1.0, every size above
	'upperBoundSize' maps to 1.0 and other sizes in between follow a linear mapping from 0.0 to 1.0.
	*/
	private def mapToScores(df: DataFrame, sizes: List[Long], lowerBoundSize: Long, upperBoundSize: Long):DataFrame = {
		val maxSize = scala.math.min(sizes.max.toDouble, upperBoundSize)
		val minSize = scala.math.max(sizes.min.toDouble, lowerBoundSize)
		val diff = (maxSize - minSize).toDouble
		val scaledSizes = sizes.zipWithIndex.map{case (s,cIndex) =>
			val scaled = if(s <= lowerBoundSize){
				0.0
			}else if(s >= upperBoundSize){
				1.0
			}else{
				(s.toDouble - minSize)/diff
			}
			(cIndex,scaled)
		}.toMap

		val scaledSizesB = spark.sparkContext.broadcast(scaledSizes)
		val clusterColIndexB = spark.sparkContext.broadcast(df.columns.size - 1)
		println("Found the following cluster sizes : "+sizes.mkString(", "))
		val newSchema = StructType(df.schema :+ StructField("km_score", DoubleType, true))
		val encoder = RowEncoder(newSchema)
		println("Computing scores from cluster sizes...")
		val res = df.mapPartitions{iter =>
			val f = scaledSizesB.value
			val clusterColIndex = clusterColIndexB.value
			iter.map{r => 
				val seq = r.toSeq
				val clusterId = seq(clusterColIndex).asInstanceOf[Int]
				val score = 1.0 - f(clusterId)
				Row.fromSeq(seq :+ score)
			}
		}(encoder)
		res.drop("cluster")
	}

	/*
	Returns an optimized model according to the elbow technique to find the
	optimal number of clusters.
	*/
	private def getOptimizedModel(km: KMeans, train: DataFrame):KMeansModel = {
		println("Finding optimal model...")
		var nbK = minNbK
		var model = km.setK(nbK).fit(train)
		var prevWSSE = model.computeCost(train)
		nbK = nbK + 1
		model = km.setK(nbK).fit(train)
		var newWSSE = model.computeCost(train)
		var oldDiff = prevWSSE - newWSSE
		var newDiff = 0.0
		var ratio = elbowRatio
		prevWSSE = newWSSE
		while(ratio >= elbowRatio && ratio < 1.0 && nbK < maxNbK){
			nbK = nbK + 1
			model = km.setK(nbK).fit(train)
			newWSSE = model.computeCost(train)
			newDiff = prevWSSE - newWSSE
			ratio = newDiff/oldDiff
			oldDiff = newDiff
			prevWSSE = newWSSE
			println(ratio+" "+newDiff)
		}
		model
	}
}