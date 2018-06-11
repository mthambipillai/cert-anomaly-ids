/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package evaluation
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._
import org.apache.spark.sql.DataFrame
import scala.math.abs
import scala.util.Random
import org.apache.spark.sql.functions._
import java.io._
import scalaz._
import Scalaz._

/*
Contains methods related to the recall computation. In the `extract` phase, it injects fake
intrusions in the logs before the computation of the features and persists these intrusions.
In the `inspect` phase, it loads the intrusions and checks how many were detected.
*/
class Evaluator(spark: SparkSession) extends Serializable{
	private val r = new Random(System.currentTimeMillis())

	/*
	Returns a new DataFrame by unioning a DataFrame 'df' with fake injected logs computed by the
	'intrusionKinds'. All intrusions should happend after 'minTimestamp', before 'maxTimestamp' and last
	at most 'intrusionTime'. Intrusions will be persisted in 'directory'.
	*/
	def injectIntrusions(df: DataFrame, intrusionKinds: List[(IntrusionKind, Int)], 
		intrusionTime: Duration, directory: String):String\/DataFrame = {
		val minMax = df.agg(min("timestamp"),max("timestamp")).head
		val minTimestamp = minMax.getLong(0)
		val maxTimestamp = minMax.getLong(1)
		val maxBeginIntrusionTime = maxTimestamp - intrusionTime.toMillis
		val allKinds = intrusionKinds.flatMap{case (intrusionKind, nbOccurences) =>
			(1 to nbOccurences).map(i => intrusionKind)}

		val cols = df.columns.toList
		val init:String\/(DataFrame,List[Intrusion]) = (df,Nil).right
		for{
			(resDF, intrusions) <- allKinds.foldLeft(init){case (disjunction, intrusionKind) => 
				for{
					(previousdf,intrusions) <- disjunction
					(min, max) = getInterval(minTimestamp, maxBeginIntrusionTime, intrusionTime)
					(nextdf,intrusion) <- intrusionKind.inject(previousdf, cols, min, max, directory)
				}yield (nextdf, intrusion::intrusions)
			}
			_ = persistIntrusions(intrusions, directory)
		}yield resDF
	}

	/*
	Loads intrusions from 'directory' and checks for each DataFrame in 'logs' if it
	matches one of the intrusions. Recall is then printed in the console.
	*/
	def evaluateIntrusions(logs: List[DataFrame], directory: String):String\/Unit = {
		for{
			intrusions <- loadIntrusions(directory)
		}yield{
			val detected = checkIntrusions(intrusions, logs)
			val nbTotal = intrusions.size
			printRecall(detected, nbTotal)
		}
	}

	/*
	Randomly finds a timestamp interval of size 'intrusionTime' such that the beginning
	is between 'minTimestamp' and 'maxBeginIntrusionTime'.
	*/
	private def getInterval(minTimestamp: Long, maxBeginIntrusionTime: Long,
		intrusionTime: Duration):(Long, Long) = {
		val minT = (abs(r.nextLong())%(maxBeginIntrusionTime - minTimestamp)) + minTimestamp
		val maxT = minT + intrusionTime.toMillis
		(minT, maxT)
	}

	/*
	Returns the intrusions among 'intrusions' that match one of the DataFrame in 'logs'.
	*/
	private def checkIntrusions(intrusions: List[Intrusion], logs: List[DataFrame]):List[Intrusion] = {
		//TODO : check with signatures
		val srcHostIndex = logs.head.columns.indexOf("srchost")
		val srcs = logs.map(df => df.head.getString(srcHostIndex))
		intrusions.filter(i => srcs.contains(i.src))
	}

	/*
	Prints the recall from the list of detected intrusions 'detected' and the number 'nbTotal' of total
	intrusions that should have been detected. Each detected intrusion is also printed.
	*/
	private def printRecall(detected: List[Intrusion], nbTotal: Int):Unit = {
		if(nbTotal==0){
			println("No known intrusions to check.")
			return
		}
		val nbDetected = detected.size
		val recall = 100.0*(nbDetected.toDouble/nbTotal.toDouble)
		println("Known intrusions detected :")
		detected.foreach(println(_))
		println("Number of known intrusions detected (Recall) : "+nbDetected+"/"+nbTotal+" = "+recall+"%\n")
	}

	private def persistIntrusions(intrusions: List[Intrusion], directory: String):Unit={
		intrusions.zipWithIndex.map{case (intrusion, index) =>
			val oos = new ObjectOutputStream(new FileOutputStream(directory+"/intrusion"+index, false))
			oos.writeObject(intrusion)
			oos.close()
		}
	}

	private def loadIntrusions(directory: String):String\/List[Intrusion]={
		for{
			files <- getListOfFiles(directory)
			fileNames = files.map(_.getName())
		}yield fileNames.map{ name =>
			val ois = new ObjectInputStream(new FileInputStream(directory+"/"+name))
			val intrusion = ois.readObject.asInstanceOf[Intrusion]
			ois.close
			intrusion
		}
	}

	private def getListOfFiles(dir: String):String\/List[File] = {
		val d = new File(dir)
		if(d.exists && d.isDirectory){
			d.listFiles.filter(_.isFile).toList.right
		}else{
			("Cannot read files from '"+dir+"'").left
		}
	}
}