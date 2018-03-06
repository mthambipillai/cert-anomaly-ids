package features
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.ml.feature.StringIndexer

import java.math.BigInteger
import java.net.{UnknownHostException, InetAddress}

/*
Each basic 'Feature' is taken from a field in the bro_conn logs and must
provide a way to convert its data type to integers or doubles (so that they can
be used later by machine learning techniques).
*/
case class Feature(
	val name: String,
	val description: String,
	private val parseColAux: (DataFrame,String) => DataFrame,
	private val aggregateAux: Option[String => Column]
){
	/*
	Parses the column 'name' of 'df' to an appropriate type (int or double).
	*/
	def parseCol(df: DataFrame):DataFrame = parseColAux(df,name)

	def aggregate(): Option[Column] = aggregateAux match{
		case Some(f) => Some(f(name))
		case None => None
	}
}

object Feature{

	private def countDistinctF(x: String): Column = countDistinct(x)
	val countDistinctOpt = Some(countDistinctF(_))

	private def meanF(x: String): Column = mean(x)
	val meanOpt = Some(meanF(_))

	private def sumF(x: String): Column = sum(x)
	val sumOpt = Some(sumF(_))

	/*
	Returns the standard list of features used for the IDS from BroConn.
	*/
	def getConnFeatures(): List[Feature] = {
		//TODO : read this from some conf file?
		List(Feature("timestamp","Timestamp",parseNumericalCol,None),
			Feature("proto","Transport layer protocol",parseStringCol,countDistinctOpt),
			Feature("service","Application layer protocol",parseStringCol,countDistinctOpt),
			Feature("duration","Duration of the connection",parseNumericalCol,meanOpt),
			Feature("orig_bytes","Number of bytes from src",parseNumericalCol,sumOpt),
			Feature("resp_bytes","Number of bytes from dst",parseNumericalCol,sumOpt),
			Feature("conn_state","Connection state at the end",parseStringCol,countDistinctOpt),
			Feature("srcip","Source IP address",parseStringCol,countDistinctOpt),
			Feature("dstip","Destination IP address",parseStringCol,countDistinctOpt),
			Feature("srcport","Source port",parseNumericalCol,countDistinctOpt),
			Feature("dstport","Destination port",parseNumericalCol,countDistinctOpt),
			Feature("srchost","DNS host resolution of the src ip",parseHostCol,countDistinctOpt),
			Feature("dsthost","DNS host resolution of the dst ip",parseHostCol,countDistinctOpt)
			//Feature("srcip_country","Country from GeoIP resolution of the src ip",parseStringCol,countDistinctOpt),
			//Feature("srcip_org","Organization from GeoIP resolution of the src ip",parseStringCol,countDistinctOpt),
			//Feature("dstip_country","Country from GeoIP resolution of the dst ip",parseStringCol,countDistinctOpt),
			//Feature("dstip_org","Organization from GeoIP resolution of the dst ip",parseStringCol,countDistinctOpt)
		)
	}

	/*
	Returns the standard list of features used for the IDS from BroSSH.
	*/
	def getSSHFeatures(): List[Feature] = {
		//TODO : read this from some conf file?
		List(Feature("timestamp","Timestamp",parseNumericalCol,None),
			Feature("auth_attempts","Number of authentication attempts",parseNumericalCol,sumOpt),
			Feature("cipher_alg","Cipher algorithm used",parseStringCol,countDistinctOpt),
			Feature("compression_alg","Compression algorithm used",parseStringCol,countDistinctOpt),
			Feature("mac_alg","MAC algorithm used",parseStringCol,countDistinctOpt),
			Feature("kex_alg","Key exchange algorithm used",parseStringCol,countDistinctOpt),
			Feature("srcport","Source port",parseNumericalCol,countDistinctOpt),
			Feature("dstport","Destination port",parseNumericalCol,countDistinctOpt),
			Feature("srchost","DNS host resolution of the src ip",parseHostCol,countDistinctOpt),
			Feature("dsthost","DNS host resolution of the dst ip",parseHostCol,countDistinctOpt),
			Feature("srcip","Source IP address",parseStringCol,countDistinctOpt),
			Feature("dstip","Destination IP address",parseStringCol,countDistinctOpt))
	}

	/*
	Fills the column 'null' values with 0.
	*/
	def parseNumericalCol(df: DataFrame, columnName: String): DataFrame = {
		df.na.fill(0, columnName :: Nil)
	}

	private val ipv4ToDouble = udf((ip: String) => ip.split("\\.").reverse.zipWithIndex.map(a=>a._1.toInt*math.pow(256,a._2).toInt).sum )
	/*
	Converts a column of IPv4 addresses to Double.
	*/
	def parseIPCol(df: DataFrame, columnName: String): DataFrame = {
		df.withColumn(columnName+"2", ipv4ToDouble(df(columnName))).drop(columnName).withColumnRenamed(columnName+"2",columnName)
	}

	/*
	Converts a column of categorical string values to integers.
	*/
	def parseStringCol(df: DataFrame, columnName: String): DataFrame = {
		val indexer = new StringIndexer().setInputCol(columnName).setOutputCol(columnName+"2").setHandleInvalid("keep")
		val df2 = df.na.fill("NULLFEATUREVALUE", columnName :: Nil)
		indexer.fit(df2).transform(df2).drop(columnName).withColumnRenamed(columnName+"2",columnName)
	}

	/*
	Converts a column of src/dst host string values to integers. This is not the same as parsing simple
	string values because "null" and "NOT_RESOLVED" are actually the same so first they must be mapped to the same string.
	*/
	def parseHostCol(df: DataFrame, columnName: String): DataFrame = {
		val df2 = df.na.fill("NOT_RESOLVED", columnName :: Nil)
		val indexer = new StringIndexer().setInputCol(columnName).setOutputCol(columnName+"2").setHandleInvalid("keep")
		indexer.fit(df2).transform(df2).drop(columnName).withColumnRenamed(columnName+"2",columnName)
	}

	/*
	Converts a column of src/dst entities string values to integers. This is not the same as parsing simple
	string values because we need to keep the previous column to be later reversed.
	*/
	def parseEntityCol(df: DataFrame, columnName: String): DataFrame = {
		val df2 = df.na.fill("NOT_RESOLVED", columnName :: Nil)
		val indexer = new StringIndexer().setInputCol(columnName).setOutputCol(columnName+"Index").setHandleInvalid("keep")
		indexer.fit(df2).transform(df2)
	}
}