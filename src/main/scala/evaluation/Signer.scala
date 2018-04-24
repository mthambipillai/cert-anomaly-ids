package evaluation
import org.apache.spark.sql.DataFrame

object Signer{
	val acc = new AccumulatorSign()
	def getSignature(df: DataFrame):String = {
		acc.reset()
		df.foreach(r => acc.add(r))
		acc.value
	}
}