package inspection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import scala.io.Source
import org.apache.spark.util.DoubleAccumulator

abstract class Rule(
	private val flagAux: (StructType, List[Row], Option[DoubleAccumulator]) => (Boolean, List[String])
) extends Serializable {
	def initAcc(spark: SparkSession): Option[DoubleAccumulator]
	def flag(rows: List[Row], acc: Option[DoubleAccumulator], schema: StructType, tagIndex: Int, commentIndex: Int):(Boolean, List[Row]) = {
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