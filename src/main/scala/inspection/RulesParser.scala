package inspection
import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper
import scala.util.Try
import scalaz._
import Scalaz._

object RulesParser{
	
	def parse(fileName: String):String\/List[Rule] = {
		for{
			json <- Try(scala.io.Source.fromFile(fileName)).toDisjunction.leftMap(e => e.getMessage)
			mapper = new ObjectMapper() with ScalaObjectMapper
			_ = mapper.registerModule(DefaultScalaModule)
			parsedJson <- Try(mapper.readValue[Map[String, Object]](json.reader()))
				.toDisjunction.leftMap(e => "Could not parse json '"+fileName+"'"+e.getMessage)
			rulesListMaps = parsedJson("rules").asInstanceOf[List[Map[String,Object]]]
		}yield{
			rulesListMaps.map{r =>
				val name = r("name").asInstanceOf[String]
				val params = r("params").asInstanceOf[List[String]]
				val text = r("text").asInstanceOf[String]
				getRule(name, params, text)
			}
		}
	}

	private def getRule(name: String, params: List[String], text: String): Rule = name match{
		case "ssh_auth_attempts" => {
			val nbAttempts = params.head.toInt
			SimpleRule.makeSimpleRule[Int]("auth_attempts", nbAttempts, _.getInt(_), _>=nbAttempts, text)
		}
		case "ssh_dstport" => {
			SimpleRule.makeSimpleRule[Int]("dstport", -1, _.getInt(_), _!=22, text)
		}
		case "ssh_version" => {
			val version = params.head.toInt
			SimpleRule.makeSimpleRule[Int]("version", -1, _.getInt(_), _< version, text)
		}
		case "ssh_srcip" => {
			val knownipsFileName = params.head
			SimpleRule.makeFileContainsRule(knownipsFileName, "srcip", "", text)
		}
		case "ssh_dsthost" => {
			val dsthostsFileName = params.head
			SimpleRule.makeFileNotContainsRule(dsthostsFileName, "dsthost", "null", text)
		}
		case "ssh_client" => {
			val clientsFileName = params.head
			SimpleRule.makeFileNotContainsRule(clientsFileName, "client", "null", text)
		}
		case "ssh_server" => {
			val serversFileName = params.head
			SimpleRule.makeFileNotContainsRule(serversFileName, "server", "null", text)
		}
		case "ssh_cipher" => {
			val ciphersFileName = params.head
			SimpleRule.makeFileNotContainsRule(ciphersFileName, "cipher_alg", "null", text)
		}
		case "ssh_total_auth_attempts" => {
			val totalNbAttempts = params.head.toInt
			ComplexRule(name, (schema, rows, acc) => {
				val index = schema.fieldIndex("auth_attempts")
				val comments = rows.map{ r =>
					val nbAttempts = if(r.isNullAt(index)) 0 else r.getInt(index)
					acc.add(nbAttempts)
					""
				}
				if(acc.value>=totalNbAttempts){
					(true, text::comments.tail)
				}else{
					(false, comments)
				}
			})
		}
	}
}