package evaluation
import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.functions._
import scala.util.Try
import scalaz._
import Scalaz._

object IntrusionsParser{

	def parse(fileName: String):String\/List[(IntrusionKind, Int)] = {
		for{
			json <- Try(scala.io.Source.fromFile(fileName)).toDisjunction.leftMap(e => e.getMessage)
			mapper = new ObjectMapper() with ScalaObjectMapper
			_ = mapper.registerModule(DefaultScalaModule)
			parsedJson <- Try(mapper.readValue[Map[String, Object]](json.reader()))
				.toDisjunction.leftMap(e => "Could not parse json '"+fileName+"'"+e.getMessage)
			rulesListMaps = parsedJson("intrusions").asInstanceOf[List[Map[String,Object]]]
			res <- rulesListMaps.traverseU{r =>
				val name = r("name").asInstanceOf[String]
				val doc = r("doc").asInstanceOf[String]
				val number = r("number").asInstanceOf[Int]
				IntrusionKind.getByName(name, doc).map(ik => (ik, number))
			}
		}yield res
	}
}