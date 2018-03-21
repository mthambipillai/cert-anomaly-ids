package features
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import scala.util.Try

/*
An 'Entity' represents either the originator or the responder of the connection.
An 'EntityExtractor' extracts new features from the existing data and features to
represents the 2 entities. The user can choose an EntityExtractor from the ones
implemented in the companion object.
*/
case class EntityExtractor(
	val name: String,
	val requiredColumns: List[String],
	private val extractAux: (DataFrame, List[Feature]) => (DataFrame, List[Feature]),
	private val reverseAux: String => (String, String)
)
{
	/*
	Returns the data frame 'df' and the features 'features' with 2 new columns/features : 'srcentity' and 'dstentity'.
	*/
	def extract(df: DataFrame, features : List[Feature]):(DataFrame, List[Feature]) = {
		val featuresNames = features.map(_.name)
		if(requiredColumns.forall(c => featuresNames.contains(c))){
			val (df2, features2) = extractAux(df, features)
			val features2Names = features2.map(_.name)
			val features2Ok = features2Names.contains("srcentity") && features2Names.contains("dstentity")
			val df2Ok = Try(df2("srcentity")).isSuccess && Try(df2("dstentity")).isSuccess
			if(features2Ok && df2Ok){
				(df2, features2)
			}else{
				throw new Exception("'srcentity' and 'dstentity' must both be present after extraction.")
			}
		}else{
			throw new Exception("Not all required columns are present.")
		}
	}

	def reverse(entityValue: String):(String, String) = {
		val (colType, value) = reverseAux(entityValue)
		if(requiredColumns.contains("src"+colType) || requiredColumns.contains("dst"+colType)){
			(colType, value)
		}else{
			throw new Exception("The original column must be a required column.")
		}
	}
}

object EntityExtractor{

	//entities are made only from the hostnames
	val hostsOnly = EntityExtractor("hostsOnly",List("srchost","dsthost"), (df, features) => {
		val df2 = df.withColumnRenamed("srchost","srcentity").withColumnRenamed("dsthost","dstentity").drop("srcip").drop("dstip")
		val filtered = features.filter(f => !(List("srchost","dsthost","srcip","dstip").contains(f.name)))
		(df2, addEntityFeatures(Feature.parseEntityCol, Feature.parseEntityCol, filtered))
	}, x=> ("host",x))

	//entities are made only from the ip addresses
	val ipOnly = EntityExtractor("ipOnly",List("srcip","dstip"), (df, features) => {
		val df2 = df.withColumnRenamed("srcip","srcentity").withColumnRenamed("dstip","dstentity").drop("srchost").drop("dsthost")
		val filtered = features.filter(f => !(List("srchost","dsthost","srcip","dstip").contains(f.name)))
		(df2, addEntityFeatures(Feature.parseEntityCol, Feature.parseEntityCol, filtered))
	}, x=> ("ip",x))

	private val fallbackUDF = udf((host: String, fallback: String) => if(host==null || host=="NOT_RESOLVED") "F"+fallback else host)

	//entities are made from hostname if it could be resolved, ip address otherwise
	val hostsWithIpFallback = EntityExtractor("hostsWithIpFallback",List("srchost","dsthost","srcip","dstip"), (df, features) => {
		hostsFallback(df, features, "srcip", "dstip")
	}, reverseFallback("ip"))

	//entities are made from hostname if it could be resolved, country otherwise
	val hostsWithCountryFallback = EntityExtractor("hostsWithCountryFallback",List("srchost","dsthost","srcip_country","dstip_country"), (df, features) => {
		hostsFallback(df, features, "srcip_country", "dstip_country")
	}, reverseFallback("ip_country"))

	//entities are made from hostname if it could be resolved, organization otherwise
	val hostsWithOrgFallback = EntityExtractor("hostsWithOrgFallback",List("srchost","dsthost","srcip_org","dstip_org"), (df, features) => {
		hostsFallback(df, features, "srcip_org", "dstip_org")
	}, reverseFallback("ip_org"))

	val extractors = List(hostsOnly, ipOnly, hostsWithIpFallback, hostsWithCountryFallback, hostsWithOrgFallback)
	val defaultExtractor = hostsWithIpFallback

	def getByName(name: String): EntityExtractor = extractors.find(e => e.name==name) match{
		case Some(ee) => ee
		case None => defaultExtractor
	}

	private def reverseFallback(fallbackCol: String)(value: String):(String, String)={
		if(value.head=='F') (fallbackCol, value.tail) else ("host",value)
	}

	/*
	Returns the dataframe 'df' and the features 'features' with the 2 additional features for entities made from hostname if
	it could be resolved, from 'srcCol' and 'dstCol' otherwise.
	*/
	private def hostsFallback(df: DataFrame, features : List[Feature], srcCol: String, dstCol: String):(DataFrame, List[Feature]) = {
		val df2 = df.withColumn("srcentity",fallbackUDF(df("srchost"),df(srcCol))).drop("srcip").drop("srchost")
		val df3 = df2.withColumn("dstentity",fallbackUDF(df("dsthost"),df(dstCol))).drop("dstip").drop("dsthost")

		/*val df2 = df.withColumn("srchost2",unresolvedToEmpty(df("srchost"))).drop("srchost")
		val df3 = df2.withColumn("dsthost2",unresolvedToEmpty(df2("dsthost"))).drop("dsthost")
		val df4 = df3.withColumn("srcentity",concat(df3("srchost2"),lit(""),df3(srcCol))).drop("srcip").drop("srchost2")
		val df5 = df4.withColumn("dstentity",concat(df4("dsthost2"),lit(""),df4(dstCol))).drop("dstip").drop("dsthost2")*/
		val filtered = features.filter(f => !(List("srchost","dsthost","srcip","dstip").contains(f.name)))
		(df3, addEntityFeatures(Feature.parseEntityCol, Feature.parseEntityCol, filtered))
	}

	private def addEntityFeatures(
		srcParser: (DataFrame,String) => DataFrame,
		dstParser: (DataFrame,String) => DataFrame,
		features: List[Feature]) : List[Feature] = {
		val srcFeature = Feature("srcentity", None, "Source Entity", srcParser, Feature.countDistinctOnly)
		val dstFeature = Feature("dstentity", None, "Destination Entity", dstParser, Feature.countDistinctOnly)
		srcFeature::(dstFeature::features)
	}
}