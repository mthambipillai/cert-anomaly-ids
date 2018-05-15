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
import scala.io.Source
import scala.util.Try
import scalaz._
import Scalaz._

/*
A SimpleRule is a Rule that considers each log entry of the anomaly individually, if at least
one of them matches the condition, the anomaly is considered a true positive.
*/
case class SimpleRule(
	private val flagAux: (StructType, List[Row]) => (Boolean, List[String])
) extends Rule((s: StructType, r: List[Row], a: Option[DoubleAccumulator]) => flagAux(s,r)){
	@Override
	def initAcc(spark: SparkSession):Option[DoubleAccumulator] = None
}

object SimpleRule{

	/*
	* Creates a SimpleRule based on different parameters that should follow a very simple
	* behaviour : it applies a boolean function on a single field for each row. The type of
	* the field must match the generic type T.
	* @param fieldName  name of the field in the row schema used by the rule.
	* @param nullFallBack  default value of the field in case it is null.
	* @param rowF  function to retrieve the field value from a row and the field index
	* @param checkF  boolean function applied on the field value. Returns true if it matches the rule.
	* @param commentText  text to append to the comments field in case the row matched the rule.
	*/
	def makeSimpleRule[T](fieldName: String, nullFallBack: T, rowF: (Row, Int) => T,
		checkF: T => Boolean, commentText: String):SimpleRule = SimpleRule((schema, rows) => {
		val index = schema.fieldIndex(fieldName)
		val tags = rows.map(r => {
			val v = if(r.isNullAt(index)) nullFallBack else rowF(r, index)
			val check = checkF(v)
			val comment = if(check) commentText else ""
			(check, List(comment))
		})
		tags.tail.foldLeft(tags.head){case ((tag1,comment1),(tag2, comment2)) => 
			(tag1 || tag2, comment1:::comment2)
		}
	})

	/*
	Returns a SimpleRule using the makeSimpleRule method where the boolean function checks if the field
	value is present in a file. The pattern of each line in the line should be : value|||count. More details
	about this can be found in the wiki.
	*/
	def makeFileContainsRule(fileName: String, fieldName: String, nullFallBack: String,
		commentText: String):String\/SimpleRule = Try(Source.fromFile(fileName)) match{
		case scala.util.Success(_) => SimpleRule.makeSimpleRule[String](fieldName, nullFallBack, _.getString(_), {
			val file = Source.fromFile(fileName).getLines.toList.map(_.split("""\|\|\|""")(0))
			file.contains(_)}, commentText).right
		case scala.util.Failure(e) => ("Could not read '"+fileName+"' because of "+e.getMessage).left
	}

	/*
	Returns a SimpleRule using the makeSimpleRule method where the boolean function checks if the field
	value is NOT present in a file. The pattern of each line in the line should be : value|||count. More details
	about this can be found in the wiki.
	*/
	def makeFileNotContainsRule(fileName: String, fieldName: String, nullFallBack: String,
		commentText: String):String\/SimpleRule = Try(Source.fromFile(fileName)) match{
		case scala.util.Success(_) => SimpleRule.makeSimpleRule[String](fieldName, nullFallBack, _.getString(_), {
			val file = Source.fromFile(fileName).getLines.toList.map(_.split("""\|\|\|""")(0))
			!file.contains(_)}, commentText).right
		case scala.util.Failure(e) => ("Could not read '"+fileName+"' because of "+e.getMessage).left
	}
}