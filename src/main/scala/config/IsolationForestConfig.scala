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

case class IsolationForestConfig(
	val nbTrees: Int,
	val nbSamples: Int
)

object IsolationForestConfig{

	def load(conf: Config):String\/IsolationForestConfig = {
		for{
			nbTrees <- IDSConfig.tryGet(conf.getInt)("nbtrees")
			nbSamples <- IDSConfig.tryGet(conf.getInt)("nbsamples")
		}yield IsolationForestConfig(nbTrees, nbSamples)
	}
}