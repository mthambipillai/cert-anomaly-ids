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

case class SimpleRule(
	private val flagAux: (StructType, List[Row]) => (Boolean, List[String])
) extends Rule((s: StructType, r: List[Row], a: Option[DoubleAccumulator]) => flagAux(s,r)){
	@Override
	def initAcc(spark: SparkSession):Option[DoubleAccumulator] = None
}

case class ComplexRule(
	name: String,
	private val flagAux: (StructType, List[Row], DoubleAccumulator) => (Boolean, List[String])
) extends Rule((s: StructType, r: List[Row], a: Option[DoubleAccumulator]) =>
	flagAux(s, r,ComplexRule.validateAcc(a, name))){
	@Override
	def initAcc(spark: SparkSession):Option[DoubleAccumulator] = Some(spark.sparkContext.doubleAccumulator(name))
}

object ComplexRule{
	def validateAcc(acc: Option[DoubleAccumulator], name: String):DoubleAccumulator = acc match{
		case Some(accumulator) => accumulator
		case None => throw new Exception("ComplexRule "+name+" must have an accumulator.")
	}
}

object Rule{

	private def makeRule[T](fieldName: String, nullFallBack: T,
		rowF: (Row, Int) => T, checkF: T => Boolean, commentText: String):SimpleRule = SimpleRule((schema, rows) => {
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

	val nbAttemptsSSH = makeRule[Int]("auth_attempts", 4, _.getInt(_), _>=4, "high/unknown nb attempts")
	val dstPortSSH = makeRule[Int]("dstport", -1, _.getInt(_), _!=22, "dst port not 22")
	val versionSSH = makeRule[Int]("version", -1, _.getInt(_), _<2, "version less than 2.x")
	private val maliciousIPs = Source.fromFile("knownips.txt").getLines.toList
	val maliciousSrcIP = makeRule[String]("srcip", "", _.getString(_), maliciousIPs.contains(_), "known malicious ip")
	private val dsthosts = Source.fromFile("dsthoststats.txt").getLines.toList.map(_.split("""\|\|\|""")(0))
	val unusualDstHostSSH = makeRule[String]("dsthost", "null", _.getString(_), !dsthosts.contains(_), "unusual dsthost")
	private val clients = Source.fromFile("clientstats.txt").getLines.toList.map(_.split("""\|\|\|""")(0))
	val unusualClientSSH = makeRule[String]("client", "null", _.getString(_), !clients.contains(_), "unusual client")
	private val servers = Source.fromFile("serverstats.txt").getLines.toList.map(_.split("""\|\|\|""")(0))
	val unusualServerSSH = makeRule[String]("server", "null", _.getString(_), !servers.contains(_), "unusual server")
	private val ciphers = Source.fromFile("cipher_algstats.txt").getLines.toList.map(_.split("""\|\|\|""")(0))
	val unusualCipherSSH = makeRule[String]("cipher_alg", "null", _.getString(_), !ciphers.contains(_), "unusual cipher")

	val totalNbAttemptsSSH = ComplexRule("totalNbAttemptsSSH", (schema, rows, acc) => {
		val index = schema.fieldIndex("auth_attempts")
		val comments = rows.map{ r =>
			val nbAttempts = if(r.isNullAt(index)) 0 else r.getInt(index)
			acc.add(nbAttempts)
			""
		}
		if(acc.value>=30){
			(true, "high total nb of attempts"::comments.tail)
		}else{
			(false, comments)
		}
	})

	val BroSSHRules = List(totalNbAttemptsSSH, nbAttemptsSSH, dstPortSSH, versionSSH,
		unusualDstHostSSH, maliciousSrcIP, unusualClientSSH, unusualServerSSH, unusualCipherSSH)
}