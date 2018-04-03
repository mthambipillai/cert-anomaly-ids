package features
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.ml.feature.StringIndexer
import java.util.Date
import java.text.SimpleDateFormat
import java.math.BigInteger
import java.net.{UnknownHostException, InetAddress}
import java.util.Calendar;  
import java.util.TimeZone;

/*
Each basic 'Feature' is taken from a field in the bro_conn logs and must
provide a way to convert its data type to integers or doubles (so that they can
be used later by machine learning techniques).
*/
case class Feature(
	val name: String,
	val parent: Option[String],
	val description: String,
	private val parseColAux: (DataFrame,String) => DataFrame,
	private val aggregateAux: List[String => Column]
){
	/*
	Parses the column 'name' of 'df' to an appropriate type (int or double).
	*/
	def parseCol(df: DataFrame):DataFrame = parseColAux(df,name)

	def aggregate(): List[Column] = aggregateAux.map(f => f(name))
}

object Feature{
	val mostCommonValue = new MostCommonValueUDAF
	
	def mostCommonValueF(colName: String): Column = mostCommonValue(col(colName))
	def countDistinctF(colName: String): Column = countDistinct(colName)
	def meanF(colName: String): Column = mean(colName)
	def sumF(colName: String): Column = sum(colName)
	def maxF(colName: String): Column = max(colName)
	def minF(colName: String): Column = min(colName)

	val countDistinctOnly = List(countDistinctF(_))
	val meanOnly = List(meanF(_))
	val sumOnly = List(sumF(_))

	private val boolToDouble = udf((x: Boolean) => {
		val r = if(x==true) 1.0 else 0.0
		if(r==null) 0.0 else r
	})
	/*
	Fills the column 'null' values with 0 and converts to Double.
	*/
	def parseBooleanCol(df: DataFrame, columnName: String): DataFrame = {
		val df2 = df.withColumn(columnName+"2", boolToDouble(df(columnName))).drop(columnName).withColumnRenamed(columnName+"2",columnName)
		df2.na.fill(0.0, columnName :: Nil)
	}

	private val intToDouble = udf((x: Int) => x.toDouble)
	/*
	Fills the column 'null' values with 0 and converts to Double.
	*/
	def parseIntCol(df: DataFrame, columnName: String): DataFrame = {
		val filled = df.na.fill(0, columnName :: Nil)
		filled.withColumn(columnName+"2", intToDouble(filled(columnName))).drop(columnName).withColumnRenamed(columnName+"2",columnName)
	}

	private val longToDouble = udf((x: Long) => x.toDouble)
	/*
	Fills the column 'null' values with 0L and converts to Double.
	*/
	def parseLongCol(df: DataFrame, columnName: String): DataFrame = {
		val filled = df.na.fill(0L, columnName :: Nil)
		filled.withColumn(columnName+"2", longToDouble(filled(columnName))).drop(columnName).withColumnRenamed(columnName+"2",columnName)
	}

	private val timeToDouble = udf((t: Long) => t.toDouble/1000.0)
	/*
	Fills the column 'null' values with 0L, converts to Double and maps from milliseconds to seconds.
	*/
	def parseTimeCol(df: DataFrame, columnName: String): DataFrame = {
		val filled = df.na.fill(0L, columnName :: Nil)
		filled.withColumn(columnName+"2", timeToDouble(filled(columnName))).drop(columnName).withColumnRenamed(columnName+"2",columnName)
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
	string values because we need to keep the previous column.
	*/
	def parseEntityCol(df: DataFrame, columnName: String): DataFrame = {
		val df2 = df.na.fill("NOT_RESOLVED", columnName :: Nil)
		val indexer = new StringIndexer().setInputCol(columnName).setOutputCol(columnName+"Index").setHandleInvalid("keep")
		indexer.fit(df2).transform(df2)
	}

	private val hourFormatter = new SimpleDateFormat("HH");
	private def toHour(df: SimpleDateFormat) = udf((t: Long) => df.format(new Date(t)).toDouble)
	def parseHourCol(df: DataFrame, columnName: String): DataFrame = {
		df.withColumn("hour", toHour(hourFormatter)(df("timestamp")))
	}

	private val cal = Calendar.getInstance(TimeZone.getDefault());
	private def toDay(cal: Calendar) = udf((t: Long) => {
		cal.setTimeInMillis(t)
		cal.get(Calendar.DAY_OF_WEEK).toDouble
	})
	def parseDayCol(df: DataFrame, columnName: String): DataFrame = {
		df.withColumn("day", toDay(cal)(df("timestamp")))
	}
}
