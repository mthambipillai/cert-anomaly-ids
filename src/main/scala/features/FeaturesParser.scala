/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
*/
package features
import org.apache.spark.sql.DataFrame
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import org.apache.spark.sql.Column
import scala.util.Try
import scalaz._
import Scalaz._

object FeaturesParser{

	def parse(fileName: String):String\/List[Feature] = {
		for{
			json <- Try(scala.io.Source.fromFile(fileName)).toDisjunction.leftMap(e => e.getMessage)
        	mapper = new ObjectMapper() with ScalaObjectMapper
        	_ = mapper.registerModule(DefaultScalaModule)
        	parsedJson <- Try(mapper.readValue[Map[String, Object]](json.reader()))
        		.toDisjunction.leftMap(e => "Could not parse json from '"+fileName+"' : "+e.getMessage)
        	featuresListMaps = parsedJson("features").asInstanceOf[List[Map[String,Object]]]
        	res <- featuresListMaps.traverseU{f =>
				val name = f("name").asInstanceOf[String]
				val parent = getParent(f("parent").asInstanceOf[String])
				for{
					parser <- getParseFunction(f("type").asInstanceOf[String])
					doc = f("doc").asInstanceOf[String]
					aggs = f("aggs").asInstanceOf[List[String]]
					aggsF <- aggs.traverseU(getAggFunction)
				}yield Feature(name, parent, doc, parser, aggsF)
			}
		}yield res
	}

	private def getParseFunction(fType: String):String\/((DataFrame,String) => DataFrame) = fType match {
		case "Boolean" => (Feature.parseBooleanCol _).right
		case "Int" => (Feature.parseIntCol _).right
		case "String" => (Feature.parseStringCol _).right
		case "Long" => (Feature.parseLongCol _).right
		case "Host" => (Feature.parseHostCol _).right
		case "Day" => (Feature.parseDayCol _).right
		case "Hour" => (Feature.parseHourCol _).right
		case t => ("Unknown type '"+t+"' for parsing feature.").left
	}

	private def getAggFunction(agg: String):String\/(String => Column) = agg match {
		case "mostcommon" => (Feature.mostCommonValueF _).right
		case "countdistinct" => (Feature.countDistinctF _).right
		case "mean" => (Feature.meanF _).right
		case "sum" => (Feature.sumF _).right
		case "max" => (Feature.maxF _).right
		case "min" => (Feature.minF _).right
		case a => ("Unknown aggregation function '"+a+"' for parsing feature.").left
	}

	private def getParent(parent: String):Option[String] = parent match{
		case null => None
		case _ => Some(parent)
	}
}