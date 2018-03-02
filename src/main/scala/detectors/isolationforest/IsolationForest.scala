package isolationforest
import scala.math.{log,ceil,pow,abs}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import detectors.Detector

class IsolationForest(spark: SparkSession, data: DataFrame, nbTrees: Int, trainSize: Int) extends Detector{
	private val limit = ceil(log(trainSize)/log(2.0)).toInt
	private val c:Double = {
		val size = data.count()
		val h = log(size-1)+0.5772156649
		2*h - (2*(size-1)/size)
	}
	private val trees = (1 to nbTrees).map(i => {
		val train = data.sample(true, 0.1).limit(trainSize)
		IsolationTree(train, limit)
	})

	private def scoreUDF(tl: Int, c: Double) = udf((x:Int) => {
		val mean = abs(x/tl)
		pow(2.0,(-1.0)*mean/c)
	})

	def computeScores(): DataFrame = {
		println("Computing path lengths...")
		val init = data.withColumn("pathlengthacc", lit(0))
		val sumDF = trees.foldLeft(init)((df, isolationTree) => accPathLength(df, isolationTree))
		println("Computing scores...")
		val l = trees.length
		sumDF.withColumn("score", scoreUDF(l,c)(col("pathlengthacc"))).drop("pathlengthacc")
	}

	def detect(threshold: Double = 0.5):DataFrame = {
		val scores = computeScores()
		scores.filter(col("score") >= threshold)
	}

	private val sumUDF = udf((x:Int,y:Int) => x+y)
	private def accPathLength(df: DataFrame, it: IsolationTree):DataFrame = {
		println("Accumulating...")
		val sumDF = it.pathLength(df, 0).withColumn("sum", sumUDF(col("pathlength"), col("pathlengthacc")))
		sumDF.drop("pathlength").drop("pathlengthacc").withColumnRenamed("sum", "pathlengthacc")
	}
}