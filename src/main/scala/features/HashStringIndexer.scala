/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package features
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.SparkSession
/*
Provides a way to convert a column of Strings to Doubles using locality-sensitive hashing.
*/
class HashStringIndexer(spark: SparkSession, inputCol: String="", outputCol: String="") extends Serializable{

	def setInputCol(value: String):HashStringIndexer = new HashStringIndexer(spark, value, outputCol)

	def setOutputCol(value: String):HashStringIndexer = new HashStringIndexer(spark, inputCol, value)

	def transform(df: DataFrame):DataFrame = {
		val inputIndex = df.columns.indexOf(inputCol)
		val inputIndexB = spark.sparkContext.broadcast(inputIndex)
		val newSchema = df.schema.add(StructField(outputCol, DoubleType, true))
		val encoder = RowEncoder(newSchema)
		df.mapPartitions(iter => {
			val inputIndex = inputIndexB.value
			iter.map(r => withHash(inputIndex, r))
		})(encoder)
	}

	def withHash(inputIndex: Int, row: Row):Row = {
		val input = row.getString(inputIndex)
		val hash = getHash(input)
		Row.fromSeq(row.toSeq :+ hash)
	}

	def getHash(value: String):Double = {
		var hash = 5381
		value.foreach(c => {
			hash = ((hash << 5) + hash) + c.toInt
		})
		hash.toDouble
	}
}