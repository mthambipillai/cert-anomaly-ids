/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package features
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar;  
import java.util.TimeZone;

/*
Each basic 'Feature' is taken from a field in the logs and must
provide a way to parse its data type to doubles. It also has a list
of aggregation functions. More details about feature extraction can be
found in FeaturesExtractor and in the wiki.
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
	def parseCol(df: DataFrame):DataFrame = parent match{
		case Some(p) => parseColAux(df,p)
		case None => parseColAux(df,name)
	}

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

	/*
	Fills the column 'null' values with 0.0.
	*/
	def parseDoubleCol(df: DataFrame, columnName: String): DataFrame = {
		df.na.fill(0.0, columnName :: Nil)
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
	def parseStringCol(spark: SparkSession)(df: DataFrame, columnName: String): DataFrame = {
		val indexer = new HashStringIndexer(spark, columnName, columnName+"2")
		val df2 = df.na.fill("NULLFEATUREVALUE", columnName :: Nil)
		indexer.transform(df2).drop(columnName).withColumnRenamed(columnName+"2",columnName)
	}

	/*
	Converts a column of src/dst host string values to integers. This is not the same as parsing simple
	string values because "null" and "NOT_RESOLVED" are actually the same so first they must be mapped to the same string.
	*/
	def parseHostCol(spark: SparkSession)(df: DataFrame, columnName: String): DataFrame = {
		val df2 = df.na.fill("NOT_RESOLVED", columnName :: Nil)
		val indexer = new HashStringIndexer(spark, columnName, columnName+"2")
		indexer.transform(df2).drop(columnName).withColumnRenamed(columnName+"2",columnName)
	}

	/*
	Converts a column of src/dst entities string values to integers. This is not the same as parsing simple
	string values because we need to keep the previous column.
	*/
	def parseEntityCol(spark: SparkSession)(df: DataFrame, columnName: String): DataFrame = {
		val df2 = df.na.fill("NOT_RESOLVED", columnName :: Nil)
		val indexer = new HashStringIndexer(spark, columnName, columnName+"Index")
		indexer.transform(df2)
	}

	def parseIntEntityCol(df: DataFrame, columnName: String): DataFrame = {
		val filled = df.na.fill(0, columnName :: Nil)
		filled.withColumn(columnName+"Index", intToDouble(filled(columnName)))
	}

	private val hourFormatter = new SimpleDateFormat("HH")
	private def toHour(sdf: SimpleDateFormat) = udf((t: Long) => sdf.format(new Date(t)).toDouble)
	/*
	Converts a column of timestamps in milliseconds as Longs to hours of the day as Doubles.
	*/
	def parseHourCol(df: DataFrame, columnName: String): DataFrame = {
		df.withColumn("hour", toHour(hourFormatter)(df(columnName)))
	}

	private val cal = Calendar.getInstance(TimeZone.getDefault())
	private def toDay(cal: Calendar) = udf((t: Long) => {
		cal.setTimeInMillis(t)
		cal.get(Calendar.DAY_OF_WEEK).toDouble
	})
	/*
	Converts a column of timestamps in milliseconds as Longs to day of the week as Doubles.
	*/
	def parseDayCol(df: DataFrame, columnName: String): DataFrame = {
		df.withColumn("day", toDay(cal)(df(columnName)))
	}

	private val lengthUDF = udf((x:String) => x.length.toDouble)
	def parseLengthCol(newColName: String)(df: DataFrame, columnName: String): DataFrame = {
		df.withColumn(newColName, lengthUDF(df(columnName)))
	}

	private val headUDF = udf((x:String) => if(x==null) "NULL" else {
		val arr = x.split(" ")
		if(arr.length==0) "NULL" else arr(0)
	})
	def parseHeadCol(spark: SparkSession, newColName: String)(df: DataFrame, columnName: String): DataFrame = {
		val withHead = df.withColumn(newColName, headUDF(df(columnName)))
		val indexer = new HashStringIndexer(spark, newColName, newColName+"2")
		indexer.transform(withHead).drop(newColName).withColumnRenamed(newColName+"2",newColName)
	}
}