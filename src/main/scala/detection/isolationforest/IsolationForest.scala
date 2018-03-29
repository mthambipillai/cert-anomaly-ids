package isolationforest
import scala.math.{log,ceil}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import detection.Detector
import org.apache.spark.sql.catalyst.encoders.RowEncoder

/*
This detector implements the algorithm described in "Liu, Ting and Zhou. Isolation Forest" (1).
*/
class IsolationForest(spark: SparkSession, data: DataFrame, dataSize: Long, nbTrees: Int, trainSize: Int) extends Detector with Serializable{
	private val limit = ceil(log(trainSize)/log(2.0)).toInt
	private val schema = data.dtypes.zipWithIndex.flatMap{case ((colName, colType),index) => 
		if(colName=="srcentity" || colName=="dstentity" || colName=="timeinterval"){
			None
		}else{
			Some((colName, colType, index))
		}
	}
	private val c:Double = {//average path length as given in section 2 of (1)
		println("\nStarting to build isolationForest model...\n")
		println("nb samples : "+dataSize)
		val h = log(dataSize-1)+0.5772156649
		val res = 2*h - (2*(dataSize-1)/dataSize)
		println("c value for IsolationForest : "+res)
		res
	}
	private val samples = (1 to nbTrees).map(i => (i, data.sample(true, 0.1).limit(trainSize)))
	private val trees = samples.map{case (i,s) => {
		val train = s.collect()
		val columnsHelpers = schema.map{ case (colName, colType, index) => 
			val (minVal, maxVal) = colType match{
				case "LongType" => {
					val trainVals = train.map(r => r.getLong(index))
					(trainVals.min, trainVals.max)
				}
				case "DoubleType" => {
					val trainVals = train.map(r => r.getDouble(index))
					(trainVals.min, trainVals.max)
				}
			}
			ColumnHelper(colName, colType, index, minVal, maxVal)
		}.toList
		println("Building iTree nb "+i+"...")
		IsolationTree(columnsHelpers, train, limit)
	}}

	/*
	Maps to a score in range [0,1] as explained in section 2 of (1).
	*/
	private def scoreUDF(tlc: Double) = udf((x:Int) => {
		val mean = x/tlc
		scala.math.pow(2.0, mean)
	})

	override def detect(threshold: Double = 0.5):DataFrame = {
		println("\nStarting to detect with isolationForest...\n")
		val lc = (-1.0)*trees.length * c
		val pathLengthThreshold = lc*(log(threshold)/log(2.0))
		println("Computing path lengths...")
		val newSchema = data.schema.add(StructField("pathlengthacc", DoubleType, true))
		val encoder = RowEncoder(newSchema)
		val sumDF = data.mapPartitions(sumPathLength)(encoder)
		println("Computing scores from path lengths...")
		val anomalies = sumDF.filter(col("pathlengthacc").leq(lit(pathLengthThreshold)))
		val res = anomalies.withColumn("if_score", scoreUDF(lc)(col("pathlengthacc"))).drop("pathlengthacc")
		println("Returning anomalies with score above "+threshold+"...")
		res.filter(col("if_score").geq(threshold))
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