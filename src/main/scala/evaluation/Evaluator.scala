package evaluation
import org.apache.spark.sql.SparkSession
import scala.concurrent.duration._
import org.apache.spark.sql.DataFrame
import scala.math.abs
import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import java.io._
import au.com.bytecode.opencsv.CSVWriter
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.SaveMode
import scalaz._
import Scalaz._
import scala.util.Try
import org.apache.spark.sql.SaveMode

class Evaluator(spark: SparkSession) extends Serializable{
	private val r = new Random(System.currentTimeMillis())
	private var intrusions:List[Intrusion] = Nil

	def injectIntrusions(df: DataFrame, intrusionKinds: List[(IntrusionKind, Int)],
		minTimestamp: Long, maxTimestamp: Long, intrusionTime: Duration):String\/DataFrame = {
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
					(nextdf,intrusion) <- intrusionKind.inject(previousdf, cols, min, max)
				}yield (nextdf, intrusion::intrusions)
			}
			_ = persistIntrusions(intrusions)
		}yield resDF
	}

	def evaluateIntrusions(logs: List[DataFrame]):String\/Unit = {
		for{
			intrusions <- (loadIntrusions()).right
		}yield{
			val detected = checkIntrusions(intrusions, logs)
			val nbTotal = intrusions.size
			printRecall(detected, nbTotal)
		}
	}

	private def getInterval(minTimestamp: Long, maxBeginIntrusionTime: Long,
		intrusionTime: Duration):(Long, Long) = {
		val minT = (abs(r.nextLong())%(maxBeginIntrusionTime - minTimestamp)) + minTimestamp
		val maxT = minT + intrusionTime.toMillis
		(minT, maxT)
	}

	private def checkIntrusions(intrusions: List[Intrusion], logs: List[DataFrame]):List[Intrusion] = {
		//TODO : check with signatures
		val srcHostIndex = logs.head.columns.indexOf("srchost")
		val srcs = logs.map(df => df.head.getString(srcHostIndex))
		intrusions.filter(i => srcs.contains(i.src))
	}

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

	def persistIntrusions(intrusions: List[Intrusion]):Unit={
		intrusions.zipWithIndex.map{case (intrusion, index) =>
			val oos = new ObjectOutputStream(new FileOutputStream("../intrusions/intrusion"+index, false))
			oos.writeObject(intrusion)
			oos.close()
		}
	}

	def loadIntrusions():List[Intrusion]={
		val fileNames = getListOfFiles("../intrusions").map(_.getName())
		fileNames.map{ name =>
			val ois = new ObjectInputStream(new FileInputStream("../intrusions/"+name))
			val intrusion = ois.readObject.asInstanceOf[Intrusion]
			ois.close
			intrusion
		}
	}

	private def getListOfFiles(dir: String):List[File] = {
		val d = new File(dir)
		if(d.exists && d.isDirectory){
			d.listFiles.filter(_.isFile).toList
		}else{
			List[File]()
		}
	}
}