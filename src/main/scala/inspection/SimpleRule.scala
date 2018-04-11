package inspection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.DoubleAccumulator

case class SimpleRule(
	private val flagAux: (StructType, List[Row]) => (Boolean, List[String])
) extends Rule((s: StructType, r: List[Row], a: Option[DoubleAccumulator]) => flagAux(s,r)){
	@Override
	def initAcc(spark: SparkSession):Option[DoubleAccumulator] = None
}