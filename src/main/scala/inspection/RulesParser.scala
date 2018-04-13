package inspection
import scala.io._
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.module.scala.experimental.ScalaObjectMapper

object RulesParser{
	
	def parse(fileName: String):List[Rule] = {
		val json = Source.fromFile(fileName)
		val mapper = new ObjectMapper() with ScalaObjectMapper
		mapper.registerModule(DefaultScalaModule)
		val parsedJson = mapper.readValue[Map[String, Object]](json.reader())
		val rulesListMaps = parsedJson("rules").asInstanceOf[List[Map[String,Object]]]
		rulesListMaps.map{r =>
			val name = r("name").asInstanceOf[String]
			val params = r("params").asInstanceOf[List[String]]
			val text = r("text").asInstanceOf[String]
			getRule(name, params, text)
		}
	}

	private def getRule(name: String, params: List[String], text: String): Rule = name match{
		case "ssh_auth_attempts" => {
			val nbAttempts = params.head.toInt
			Rule.makeRule[Int]("auth_attempts", nbAttempts, _.getInt(_), _>=nbAttempts, text)
		}
		case "ssh_dstport" => {
			Rule.makeRule[Int]("dstport", -1, _.getInt(_), _!=22, text)
		}
		case "ssh_version" => {
			val version = params.head.toInt
			Rule.makeRule[Int]("version", -1, _.getInt(_), _< version, text)
		}
		case "ssh_srcip" => {
			val knownipsFileName = params.head
			Rule.makeRule[String]("srcip", "", _.getString(_), {
				val maliciousIPs = Source.fromFile(knownipsFileName).getLines.toList
				maliciousIPs.contains(_)}, text)
		}
		case "ssh_dsthost" => {
			val dsthostsFileName = params.head
			Rule.makeRule[String]("dsthost", "null", _.getString(_), {
				val dsthosts = Source.fromFile(dsthostsFileName).getLines.toList.map(_.split("""\|\|\|""")(0))
				!dsthosts.contains(_)}, text)
		}
		case "ssh_client" => {
			val clientsFileName = params.head
			Rule.makeRule[String]("client", "null", _.getString(_), {
				val clients = Source.fromFile(clientsFileName).getLines.toList.map(_.split("""\|\|\|""")(0))
				!clients.contains(_)}, text)
		}
		case "ssh_server" => {
			val serversFileName = params.head
			Rule.makeRule[String]("server", "null", _.getString(_), {
				val servers = Source.fromFile(serversFileName).getLines.toList.map(_.split("""\|\|\|""")(0))
				!servers.contains(_)}, text)
		}
		case "ssh_cipher" => {
			val ciphersFileName = params.head
			Rule.makeRule[String]("cipher_alg", "null", _.getString(_), {
				val ciphers = Source.fromFile(ciphersFileName).getLines.toList.map(_.split("""\|\|\|""")(0))
				!ciphers.contains(_)}, text)
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