package features
import org.apache.spark.sql.DataFrame
import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.Column

object FeaturesParser{

	def parse(fileName: String):List[Feature] = {
		val json = Source.fromFile(fileName)
		val mapper = new ObjectMapper() with ScalaObjectMapper
		mapper.registerModule(DefaultScalaModule)
		val parsedJson = mapper.readValue[Map[String, Object]](json.reader())
		val featuresListMaps = parsedJson("features").asInstanceOf[List[Map[String,Object]]]
		featuresListMaps.map{f =>
			val name = f("name").asInstanceOf[String]
			val parent = getParent(f("parent").asInstanceOf[String])
			val parser = getParseFunction(f("type").asInstanceOf[String])
			val doc = f("doc").asInstanceOf[String]
			val aggs = f("aggs").asInstanceOf[List[String]]
			Feature(name, parent, doc, parser, aggs.map(getAggFunction))
		}
	}

	private def getParseFunction(fType: String):(DataFrame,String) => DataFrame = fType match {
		case "Boolean" => Feature.parseBooleanCol
		case "Int" => Feature.parseIntCol
		case "String" => Feature.parseStringCol
		case "Long" => Feature.parseLongCol
		case "Host" => Feature.parseHostCol
		case "Day" => Feature.parseDayCol
		case "Hour" => Feature.parseHourCol
	}

	private def getAggFunction(agg: String):String => Column = agg match {
		case "mostcommon" => Feature.mostCommonValueF
		case "countdistinct" => Feature.countDistinctF
		case "mean" => Feature.meanF
		case "sum" => Feature.sumF
		case "max" => Feature.maxF
		case "min" => Feature.minF
	}

	private def getParent(parent: String):Option[String] = parent match{
		case null => None
		case _ => Some(parent)
	}
}