/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
*/
package inspection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.DoubleAccumulator

/*
A Rule defines a way to analyze logs of an anomaly and determine if they match some condition that
implies that the detected anomaly is a true positive. It has 2 subclasses : SimpleRule and ComplexRule.
*/
abstract class Rule(
	private val flagAux: (StructType, List[Row], Option[DoubleAccumulator]) => (Boolean, List[String])
) extends Serializable {
	def initAcc(spark: SparkSession): Option[DoubleAccumulator]

	/*
	* Applies the rule to a list of Row by extending the schema with anomalytag and comments. Returns
	* a Boolean true if the rule found a match and the extended list of Row.
	* @param rows  The list of Row to flag.
	* @param acc  None if it's a SimpleRule, contains an accumulator for aggregation if it's a ComplexRule.
	* @param schema  Extended schema for the new rows.
	* @param tagIndex  Index of the anomalytag in the new schema.
	* @param commentIndex  Index of the comments in the new schema.
	*/
	def flag(rows: List[Row], acc: Option[DoubleAccumulator], schema: StructType, tagIndex: Int,
		commentIndex: Int):(Boolean, List[Row]) = {
		if(rows.size==0){
			return (false,rows)
		}
		val firstSeq = rows(0).toSeq
		val tag = firstSeq(tagIndex).asInstanceOf[String]
		val (isAnomaly, comments) = flagAux(schema, rows, acc)
		val newTag = if(isAnomaly) "yes" else {if(tag=="") "?" else tag}
		val firstComment = firstSeq(commentIndex)
		val newFirstRow = Row.fromSeq(firstSeq.dropRight(2) ++ Seq(newTag, firstComment))
		val newRows = (newFirstRow::rows.tail).zip(comments).map{case (row, comment) =>
			val prevComment = row.getString(commentIndex)
			val newComment = List(prevComment, comment).filter(_!="").mkString(" + ")
			Row.fromSeq(row.toSeq.dropRight(1) :+ newComment)
		}
		(isAnomaly, newRows)
	}
}