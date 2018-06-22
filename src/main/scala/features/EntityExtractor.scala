/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package features
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Try
import scalaz._
import Scalaz._

/*
An 'Entity' represents either the originator or the responder of the connection.
An 'EntityExtractor' extracts new features from the existing data and features to
represents the 2 entities.
*/
case class EntityExtractor(
	val name: String,
	val requiredColumns: List[String],
	private val extractAux: (DataFrame, List[Feature], String) => (DataFrame, List[Feature]),
	private val reverseAux: (String,String) => List[(String, String)]
)
{
	/*
	Returns the data frame 'df' and the features 'features' with 2 new columns/features : 'srcentity' and 'dstentity'.
	*/
	def extract(df: DataFrame, features : List[Feature], eType: String):String\/(DataFrame, List[Feature]) = {
		val featuresNames = features.map(_.name)
		if(requiredColumns.forall(c => featuresNames.contains(c))){
			val (df2, features2) = extractAux(df, features, eType)
			val features2Names = features2.map(_.name)
			val features2Ok = features2Names.contains("srcentity") && features2Names.contains("dstentity")
			val df2Ok = Try(df2("srcentity")).isSuccess && Try(df2("dstentity")).isSuccess
			if(features2Ok && df2Ok){
				(df2, features2).right
			}else{
				("'srcentity' and 'dstentity' must both be present after extraction.").left
			}
		}else{
			("Not all required columns are present. Need : "+requiredColumns.mkString(", ")+".").left
		}
	}

	/*
	Returns the SQL clause to fetch the original value that was converted to
	'entityValue' in the entity extraction phase.
	*/
	def reverse(eType: String, entityValue: String):String\/String = {
		val clausesDisj = reverseAux(eType, entityValue).traverseU{ case (colName, value) =>
			if(requiredColumns.contains(colName)){
				(colName+"='"+value+"'").right
			}else{
				("The original column must be a required column.").left
			}
		}
		clausesDisj.map(clauses => clauses.mkString(" AND "))
	}
}

object EntityExtractor{

	//entities are made only from the hostnames
	def hostsOnly(spark: SparkSession):EntityExtractor = EntityExtractor(
		"hostsOnly",List("srchost","dsthost"), (df, features, eType) => {
		val df2 = df.withColumnRenamed("srchost","srcentity").withColumnRenamed("dsthost","dstentity").drop("srcip").drop("dstip")
		val filtered = features.filter(f => !(List("srchost","dsthost","srcip","dstip").contains(f.name)))
		(df2, addEntityFeatures(Feature.parseEntityCol(spark), Feature.parseEntityCol(spark), filtered, eType))
	}, (eType,x) => List((eType+"host",x)))

	//entities are made only from the ip addresses
	def ipOnly(spark: SparkSession):EntityExtractor = EntityExtractor(
		"ipOnly",List("srcip","dstip"), (df, features, eType) => {
		val df2 = df.withColumnRenamed("srcip","srcentity").withColumnRenamed("dstip","dstentity").drop("srchost").drop("dsthost")
		val filtered = features.filter(f => !(List("srchost","dsthost","srcip","dstip").contains(f.name)))
		(df2, addEntityFeatures(Feature.parseEntityCol(spark), Feature.parseEntityCol(spark), filtered, eType))
	}, (eType,x) => List((eType+"ip",x)))

	private val fallbackUDF = udf((host: String, fallback: String) => if(host==null || host=="NOT_RESOLVED") "F"+fallback else host)

	//entities are made from hostname if it could be resolved, ip address otherwise
	def hostsWithIpFallback(spark: SparkSession):EntityExtractor = EntityExtractor(
		"hostsWithIpFallback",List("srchost","dsthost","srcip","dstip"), (df, features, eType) => {
		hostsFallback(spark)(df, features, "srcip", "dstip", eType)
	}, reverseFallback("ip"))

	//entities are made from hostname if it could be resolved, country otherwise
	def hostsWithCountryFallback(spark: SparkSession):EntityExtractor = EntityExtractor(
		"hostsWithCountryFallback",List("srchost","dsthost","srcip_country","dstip_country"), (df, features, eType) => {
		hostsFallback(spark)(df, features, "srcip_country", "dstip_country", eType)
	}, reverseFallback("ip_country"))

	//entities are made from hostname if it could be resolved, organization otherwise
	def hostsWithOrgFallback(spark: SparkSession):EntityExtractor = EntityExtractor(
		"hostsWithOrgFallback",List("srchost","dsthost","srcip_org","dstip_org"), (df, features, eType) => {
		hostsFallback(spark)(df, features, "srcip_org", "dstip_org", eType)
	}, reverseFallback("ip_org"))

	//entities are made from the user id if src or from host name if dst
	private val toStringUDF = udf((x: Int) => x+"")
	def uidOnlyAndHost(spark: SparkSession):EntityExtractor = EntityExtractor(
		"uidOnlyAndHost",List("uid","host"), (df, features, eType) => {
		val df2 = df.withColumn("srcentity", toStringUDF(col("uid"))).drop("uid").withColumnRenamed("host","dstentity").drop("host")
		val filtered = features.filter(f => !(List("uid","host").contains(f.name)))
		(df2, addEntityFeatures(Feature.parseEntityCol(spark), Feature.parseEntityCol(spark), filtered, eType))
	}, (eType,x) => if(eType=="src") List(("uid",x)) else List(("host",x)))

	//entities are made from the user id if src or from host name if dst
	private val srcUDF = udf((uid: Int, ppid:Int, host:String) => 
		if(uid==0){
			uid+","+ppid+","+host
		}else{
			if(uid<1000){
				uid+","+host
			}else{
				uid+""
			}
		}
	)
	def uid0AndHost(spark: SparkSession):EntityExtractor = EntityExtractor(
		"uid0AndHost",List("uid","ppid","host"), (df, features, eType) => {
		val df2 = df.withColumn("srcentity", srcUDF(df("uid"),df("ppid"),df("host"))).drop("uid").withColumnRenamed("host","dstentity").drop("host")
		val filtered = features.filter(f => !(List("uid","ppid","host").contains(f.name)))
		(df2, addEntityFeatures(Feature.parseEntityCol(spark), Feature.parseEntityCol(spark), filtered, eType))
	}, (eType,x) => if(eType=="src") parseUid(x) else List(("host",x)))

	private def parseUid(srcValue: String):List[(String,String)] = {
		val parts = srcValue.split(",")
		if(parts.size==1){
			List(("uid",srcValue))
		}else{
			if(parts.size==2){
				List(("uid",parts(0)), ("host",parts(1)))
			}else{
				List(("uid",parts(0)), ("ppid",parts(1)), ("host",parts(2)))
			}
		}
	}

	def extractors(spark: SparkSession):List[EntityExtractor] = List(
		hostsOnly(spark),
		ipOnly(spark),
		hostsWithIpFallback(spark),
		hostsWithCountryFallback(spark),
		hostsWithOrgFallback(spark),
		uidOnlyAndHost(spark),
		uid0AndHost(spark))

	def defaultExtractor(spark: SparkSession) = hostsWithIpFallback(spark)

	def getByName(spark: SparkSession, name: String): EntityExtractor = extractors(spark).find(e => e.name==name) match{
		case Some(ee) => ee
		case None => defaultExtractor(spark)
	}

	private def reverseFallback(fallbackCol: String)(eType: String, value: String):List[(String, String)]={
		if(value.head=='F') List((eType+fallbackCol, value.tail)) else List((eType+"host",value))
	}

	/*
	Returns the dataframe 'df' and the features 'features' with the 2 additional features for entities made from hostname if
	it could be resolved, from 'srcCol' and 'dstCol' otherwise.
	*/
	private def hostsFallback(spark: SparkSession)(df: DataFrame, features : List[Feature],
		srcCol: String, dstCol: String, eType: String):(DataFrame, List[Feature]) = {
		val df2 = df.withColumn("srcentity",fallbackUDF(df("srchost"),df(srcCol))).drop("srcip").drop("srchost")
		val df3 = df2.withColumn("dstentity",fallbackUDF(df("dsthost"),df(dstCol))).drop("dstip").drop("dsthost")
		val filtered = features.filter(f => !(List("srchost","dsthost","srcip","dstip").contains(f.name)))
		(df3, addEntityFeatures(Feature.parseEntityCol(spark), Feature.parseEntityCol(spark), filtered, eType))
	}

	private def addEntityFeatures(
		srcParser: (DataFrame,String) => DataFrame,
		dstParser: (DataFrame,String) => DataFrame,
		features: List[Feature], eType: String) : List[Feature] = {
		val srcAggs = eType match{
			case "srcentity" => Nil
			case "dstentity" => Feature.countDistinctOnly
		}
		val dstAggs = eType match{
			case "srcentity" => Feature.countDistinctOnly
			case "dstentity" => Nil
		}
		val srcFeature = Feature("srcentity", None, "Source Entity", srcParser, srcAggs)
		val dstFeature = Feature("dstentity", None, "Destination Entity", dstParser, dstAggs)
		srcFeature::(dstFeature::features)
	}
}