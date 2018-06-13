/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package config
import features.Feature
import scala.concurrent.duration._
import inspection.Rule
import evaluation.IntrusionKind

case class IDSConfig(
	//Global parameters
	val mode: String,
	val logLevel: String,
	val filePath: String,
	val featuresschema: List[Feature],
	val extractor: String,
	val interval: Duration,
	val trafficMode: String,
	val scaleMode: String,
	val ensembleMode: String,
	val featuresFile: String,
	val featuresStatsFile : String,
	val detectors: String,
	val threshold: Double,
	val topAnomalies: Int,
	val anomaliesFile: String,
	val rules: List[Rule],
	val inspectionResults: String,
	val recall: Boolean,
	val intrusions: List[(IntrusionKind,Int)],
	val intrusionsDir: String,
	val detectorToOpt: String,
	//IsolationForest parameters
	val isolationForest: IsolationForestConfig,
	//KMeans parameters
	val kMeans: KMeansConfig,
	//Local Outlier Factor parameters
	val lof: LOFConfig
)