/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package config
import com.typesafe.config.Config
import scalaz._

case class KMeansConfig(
	val trainRatio: Double,
	val minNbK: Int,
	val maxNbK: Int,
	val elbowRatio: Double,
	val nbK: Int,
	val lowBound: Long,
	val upBound: Long
)

object KMeansConfig{

	def load(conf: Config):String\/KMeansConfig = {
		for{
			trainRatio <- IDSConfigParser.tryGet(conf.getDouble)("trainratio")
			minNbK <- IDSConfigParser.tryGet(conf.getInt)("minnbk")
			maxNbK <- IDSConfigParser.tryGet(conf.getInt)("maxnbk")
			elbowRatio <- IDSConfigParser.tryGet(conf.getDouble)("elbowratio")
			nbK <- IDSConfigParser.tryGet(conf.getInt)("nbk")
			lowBound <- IDSConfigParser.tryGet(conf.getLong)("lowbound")
			upBound <- IDSConfigParser.tryGet(conf.getLong)("upbound")
		}yield KMeansConfig(trainRatio, minNbK, maxNbK, elbowRatio, nbK, lowBound, upBound)
	}
}