package evaluation
import features.Feature
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import scala.util.Random
import scala.math.abs
import org.apache.spark.sql.functions._

@SerialVersionUID(100L)
case class Intrusion(
	val kind: IntrusionKind,
	val src: String,
	val beginTimestamp: Long,
	val endTimestamp: Long
) extends Serializable{
	def inject(df: DataFrame, columns: List[String]): DataFrame = {
		kind.inject(df, columns, src, beginTimestamp, endTimestamp)
	}
}