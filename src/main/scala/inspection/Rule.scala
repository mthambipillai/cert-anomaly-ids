package inspection
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import scala.io.Source

case class Rule(
	private val flagAux: (StructType, List[Row]) => (Boolean, List[String])
){

	def flag(rows: List[Row], schema: StructType, tagIndex: Int, commentIndex: Int):List[Row] = {
		val firstSeq = rows(0).toSeq
		val tag = firstSeq(tagIndex).asInstanceOf[String]
		val (isAnomaly, comments) = flagAux(schema, rows)
		val newTag = if(isAnomaly) "yes" else {if(tag=="") "?" else tag}
		val firstComment = firstSeq(commentIndex)
		val newFirstRow = Row.fromSeq(firstSeq.dropRight(2) ++ Seq(newTag, firstComment))
		(newFirstRow::rows.tail).zip(comments).map{case (row, comment) =>
			val prevComment = row.getString(commentIndex)
			val newComment = List(prevComment, comment).filter(_!="").mkString(" + ")
			Row.fromSeq(row.toSeq.dropRight(1) :+ newComment)
		}
	}
}

object Rule{

	val nbAttemptsSSH = Rule((schema, rows) => {
		val nbAttemptsIndex = schema.fieldIndex("auth_attempts")
		val tags = rows.map(r => {
			val nb = if(r.isNullAt(nbAttemptsIndex)) 4 else r.getInt(nbAttemptsIndex)
			val comment = if(nb>=4) "high/unknown nb attempts" else ""
			(nb>=4, List(comment))
		})
		tags.tail.foldLeft(tags.head){case ((tag1,comment1),(tag2, comment2)) => 
			(tag1 || tag2, comment1:::comment2)
		}
	})

	val dstPortSSH = Rule((schema, rows) => {
		val dstPortIndex = schema.fieldIndex("dstport")
		val tags = rows.map(r => {
			val port = if(r.isNullAt(dstPortIndex)) -1 else r.getInt(dstPortIndex)
			val comment = if(port!=22) "dst port not 22" else ""
			(port!=22, List(comment))
		})
		tags.tail.foldLeft(tags.head){case ((tag1,comment1),(tag2, comment2)) => 
			(tag1 || tag2, comment1:::comment2)
		}
	})

	val versionSSH = Rule((schema, rows) => {
		val versionIndex = schema.fieldIndex("version")
		val tags = rows.map(r => {
			val version = if(r.isNullAt(versionIndex)) -1 else r.getInt(versionIndex)
			val comment = if(version<2) "version less than 2.x" else ""
			(version<2, List(comment))
		})
		tags.tail.foldLeft(tags.head){case ((tag1,comment1),(tag2, comment2)) => 
			(tag1 || tag2, comment1:::comment2)
		}
	})

	val unusualDstHostSSH = Rule((schema, rows) => {
		val dstHostIndex = schema.fieldIndex("dsthost")
		val tags = rows.map(r => {
			val dstHost = if(r.isNullAt(dstHostIndex)) "" else r.getString(dstHostIndex)
			val comment = if(!dstHost.contains("lxplus")) "unusual dst host" else ""
			(!dstHost.contains("lxplus"), List(comment))
		})
		tags.tail.foldLeft(tags.head){case ((tag1,comment1),(tag2, comment2)) => 
			(tag1 || tag2, comment1:::comment2)
		}
	})

	val maliciousSrcIP = Rule((schema, rows) => {
		val srcIPIndex = schema.fieldIndex("srcip")
		val filename = "knownips.txt"
		val ips = Source.fromFile(filename).getLines.toList
		val tags = rows.map(r => {
			val srcIP = if(r.isNullAt(srcIPIndex)) "" else r.getString(srcIPIndex)
			val comment = if(ips.contains(srcIP)) "known malicious ip" else ""
			(ips.contains(srcIP), List(comment))
		})
		tags.tail.foldLeft(tags.head){case ((tag1,comment1),(tag2, comment2)) => 
			(tag1 || tag2, comment1:::comment2)
		}
	})

	val BroSSHRules = List(nbAttemptsSSH, dstPortSSH, versionSSH, unusualDstHostSSH, maliciousSrcIP)
}