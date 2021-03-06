/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package evaluation
import org.apache.spark.sql.DataFrame

object Signer{
	val acc = new AccumulatorSign()
	def getSignature(df: DataFrame):String = {
		acc.reset()
		df.foreach(r => acc.add(r))
		acc.value
	}
}