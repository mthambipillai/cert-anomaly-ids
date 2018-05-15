/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package inspection
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.util.DoubleAccumulator

/*
A ComplexRule is a Rule that considers all logs of the anomaly to determine if they
match the condition. It has an accumulator to compute a value accross all logs and then
evaluate that resulting value with the condition.
*/
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