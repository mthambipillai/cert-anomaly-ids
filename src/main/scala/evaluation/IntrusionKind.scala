package evaluation
import features.Feature
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import scala.util.Random
import scala.math.abs
import org.apache.spark.sql.functions._
import scalaz._
import Scalaz._
import org.apache.spark.sql.SaveMode

case class IntrusionKind(
	val name: String,
	val description: String,
	val requiredColumns: List[String],
	private val injectAux: (DataFrame, String, Long, Long) => DataFrame
){
	def inject(df: DataFrame, columns: List[String], minTimestamp: Long, maxTimestamp: Long): String\/(DataFrame,Intrusion) = {
		if(requiredColumns.forall(c => columns.contains(c))){
			val src = IntrusionKind.getNextSrc()
			val newRows = injectAux(df, src, minTimestamp, maxTimestamp)
			val selectedNewRows = newRows.select(columns.head, columns.tail: _*)
			selectedNewRows.sort(asc("timestamp")).write.mode(SaveMode.Overwrite).parquet("../intrusionslogs/"+src+".parquet")
			val intrusion = Intrusion(this, src, minTimestamp, maxTimestamp, Signer.getSignature(selectedNewRows))
			(df.union(selectedNewRows), intrusion).right
		}else{
			("Not all required columns are present. Need : "+requiredColumns.mkString(", ")+".").left
		}
	}
}
object IntrusionKind{
	private val r = new Random(System.currentTimeMillis())
	private var intrusionCounter = 0

	def getNextSrc():String = {
		intrusionCounter = intrusionCounter + 1
		"dummySrc"+intrusionCounter
	}

	private def timeUDF(r: Random, min: Long, max: Long) = udf((t: Long) => (r.nextDouble()*(max-min)).toLong+min)

	val tooManyAuthAttempts = IntrusionKind("tooManyAuthAttempts", "The src entity tried to authenticate an unusual number of times to the same dst",
		List("auth_attempts", "srchost"), (df, src, minTimestamp, maxTimestamp) => {
			val nbAttempts = 200
			val res = df.sample(true, 0.01).limit(50)
				.withColumn("timestamp2", timeUDF(r,minTimestamp, maxTimestamp)(df("timestamp")))
				.drop("timestamp").withColumnRenamed("timestamp2", "timestamp")
				.withColumn("srchost2", lit(src)).drop("srchost").withColumnRenamed("srchost2", "srchost")
				.withColumn("auth_attempts2", lit(nbAttempts))
				.drop("auth_attempts").withColumnRenamed("auth_attempts2", "auth_attempts")
			res
	})

	val allKinds = List(tooManyAuthAttempts)
}