package isolationforest
import scala.math.{log,ceil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import detectors.Detector

/*
This detector implements the algorithm described in "Liu, Ting and Zhou. Isolation Forest" (1).
*/
class IsolationForest(spark: SparkSession, data: DataFrame, nbTrees: Int, trainSize: Int) extends Detector with Serializable{
	private val limit = ceil(log(trainSize)/log(2.0)).toInt
	private val schema = data.dtypes
	private val c:Double = {//average path length as given in section 2 of (1)
		val size = data.count()
		val h = log(size-1)+0.5772156649
		2*h - (2*(size-1)/size)
	}
	private val trees = (1 to nbTrees).map(i => {
		val train = data.sample(true, 0.1).limit(trainSize)
		IsolationTree(train, limit)
	})

	/*
	Maps to a score in range [0,1] as explained in section 2 of (1).
	*/
	private def scoreUDF(tlc: Double) = udf((x:Int) => {
		val mean = (-1.0)*scala.math.abs(x/tlc)
		scala.math.pow(2.0,mean)
	})

	override def detect(threshold: Double = 0.5):DataFrame = {
		val lc = (-1.0)*trees.length * c
		val pathLengthThreshold = lc*(log(threshold)/log(2.0))
		println("Computing path lengths...")
		val mappedRDD = data.rdd.mapPartitions(sumPathLength)
		val newSchema = data.schema.add(StructField("pathlengthacc", DoubleType, true))
		val sumDF = data.sqlContext.createDataFrame(mappedRDD , newSchema)
		println("Finding anomalies and their scores...")
		val anomalies = sumDF.filter(col("pathlengthacc").leq(lit(pathLengthThreshold)))
		anomalies.withColumn("score", scoreUDF(lc)(col("pathlengthacc"))).drop("pathlengthacc")
	}

	/*
	Computes for every Row in 'rows' the sum of the path lengths from every IsolationTree in this IsolationForest. 
	*/
	private def sumPathLength(rows: Iterator[Row]):Iterator[Row] = {
		if(rows.isEmpty) return rows
		val rowsSeq = rows.toSeq
		val allLengths = trees.map(t => t.pathLength(schema, rowsSeq, 0)).filter(seq => !seq.isEmpty)
		val sumLengths = allLengths.tail.foldLeft(allLengths.head)((lens1,lens2) => lens1.zip(lens2).map{case (d1, d2) => d1+d2})
		rowsSeq.zip(sumLengths).map{case (row, sum) => Row.fromSeq(row.toSeq ++ Array[Any](sum))}.toIterator
	}
}