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

case class LOFConfig(
	val k: Int,
	val hashNbDigits: Int,
	val hashNbVects: Int,
	val maxScore: Double
)

object LOFConfig{

	def load(conf: Config):String\/LOFConfig = {
		for{
			k <- IDSConfig.tryGet(conf.getInt)("k")
			hashNbDigits <- IDSConfig.tryGet(conf.getInt)("hashnbdigits")
			hashNbVects <- IDSConfig.tryGet(conf.getInt)("hashnbvects")
			maxScore <- IDSConfig.tryGet(conf.getInt)("maxscore")
		}yield LOFConfig(k, hashNbDigits, hashNbVects, maxScore)
	}
}