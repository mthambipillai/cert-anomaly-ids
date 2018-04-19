package inspection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.DoubleAccumulator
import scala.io.Source
import scala.util.Try
import scalaz._
import Scalaz._

case class SimpleRule(
	private val flagAux: (StructType, List[Row]) => (Boolean, List[String])
) extends Rule((s: StructType, r: List[Row], a: Option[DoubleAccumulator]) => flagAux(s,r)){
	@Override
	def initAcc(spark: SparkSession):Option[DoubleAccumulator] = None
}

object SimpleRule{

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

	def makeFileContainsRule(fileName: String, fieldName: String, nullFallBack: String,
		commentText: String):String\/SimpleRule = Try(Source.fromFile(fileName)) match{
		case scala.util.Success(_) => SimpleRule.makeSimpleRule[String](fieldName, nullFallBack, _.getString(_), {
			val file = Source.fromFile(fileName).getLines.toList.map(_.split("""\|\|\|""")(0))
			file.contains(_)}, commentText).right
		case scala.util.Failure(e) => ("Could not read '"+fileName+"' because of "+e.getMessage).left
	}

	def makeFileNotContainsRule(fileName: String, fieldName: String, nullFallBack: String,
		commentText: String):String\/SimpleRule = Try(Source.fromFile(fileName)) match{
		case scala.util.Success(_) => SimpleRule.makeSimpleRule[String](fieldName, nullFallBack, _.getString(_), {
			val file = Source.fromFile(fileName).getLines.toList.map(_.split("""\|\|\|""")(0))
			!file.contains(_)}, commentText).right
		case scala.util.Failure(e) => ("Could not read '"+fileName+"' because of "+e.getMessage).left
	}
}