package evaluation
import features.Feature
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import scala.util.Random
import scala.math.abs
import org.apache.spark.sql.functions._

case class IntrusionKind(
	val name: String,
	val description: String,
	val requiredColumns: List[String],
	private val injectAux: (DataFrame, String, Long, Long) => DataFrame
){
	def inject(df: DataFrame, columns: List[String], src: String, minTimestamp: Long, maxTimestamp: Long): DataFrame = {
		if(requiredColumns.forall(c => columns.contains(c))){
			val newRows = injectAux(df, src, minTimestamp, maxTimestamp).select(columns.head, columns.tail: _*)
			df.union(newRows)
		}else{
			throw new Exception("Not all required columns are present.")
		}
	}

	def getIntrusion(minTimestamp: Long, maxTimestamp: Long):Intrusion = {
		val src = "dummySrc"+IntrusionKind.r.nextInt(10000)
		Intrusion(this, src, minTimestamp, maxTimestamp)
	}
}
object IntrusionKind{
	private val r = new Random(System.currentTimeMillis())

	private def timeUDF(timestamp: Long) = udf((t: Long) => timestamp)

	val tooManyAuthAttempts = IntrusionKind("tooManyAuthAttempts", "The src entity tried to authenticate an unusual number of times to the same dst",
		List("auth_attempts", "srcentity"), (df, src, minTimestamp, maxTimestamp) => {
			val nbAttempts = 50000
			df.sample(true, 0.1).limit(500)
				.withColumn("timestamp2", lit(minTimestamp)).drop("timestamp").withColumnRenamed("timestamp2", "timestamp")
				.withColumn("srcentity2", lit(src)).drop("srcentity").withColumnRenamed("srcentity2", "srcentity")
				.withColumn("auth_attempts2", lit(nbAttempts)).drop("auth_attempts").withColumnRenamed("auth_attempts2", "auth_attempts")
	})

	val allKinds = List(tooManyAuthAttempts)
}