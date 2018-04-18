package evaluation
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.sql.DataFrame
import java.math.BigInteger
import java.security.MessageDigest
import org.apache.spark.sql.Row

object Signer{
	val acc = new AccumulatorSign()
	def getSignature(df: DataFrame):String = {
		acc.reset()
		df.foreach(r => acc.add(r))
		acc.value
	}
}