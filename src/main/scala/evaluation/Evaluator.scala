package evaluation
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

class Evaluator() extends Serializable{
	private val r = new Random(System.currentTimeMillis())
	private var intrusions:List[Intrusion] = Nil

	def injectIntrusions(df: DataFrame, intrusionKinds: List[(IntrusionKind, Int)],
		minTimestamp: Long, maxTimestamp: Long, intrusionTime: Duration):String\/DataFrame = {
		val maxBeginIntrusionTime = maxTimestamp - intrusionTime.toMillis
		val allIntrusions = intrusionKinds.flatMap{case (intrusionKind, nbOccurences) =>
			(1 to nbOccurences).map(i => 
				generateIntrusion(intrusionKind, minTimestamp, maxBeginIntrusionTime, intrusionTime))}
		this.intrusions = allIntrusions
		val cols = df.columns.toList
		val dfr:String\/DataFrame = df.right
		allIntrusions.foldLeft(dfr){ (previousdfDisj, intrusion) => 
			for{
				previousdf <- previousdfDisj
				nextdf <- intrusion.inject(previousdf, cols)
			}yield nextdf
		}
	}

	def evaluateResults(detected: DataFrame, trafficMode: String = "src", nbTop: Int, destFile: String):Unit={
		val eType = trafficMode+"entity"
		val distDetected = detected.dropDuplicates(Array(eType, "timeinterval"))
		println("Number of distinct "+trafficMode+" entities involved : "+distDetected.count)
		val otherCols = detected.columns.toList.filterNot(c => c==eType || c=="timeinterval")
		val newCols = eType::("timeinterval"::otherCols)
		val scoreCol = newCols.filter(_.contains("score")).head
		val top = distDetected.sort(desc(scoreCol)).limit(nbTop).select(newCols.head, newCols.tail:_*)
		println("Writing top "+nbTop+" intrusions detected to "+destFile+".")
		top.coalesce(1).write.mode(SaveMode.Overwrite).format("com.databricks.spark.csv").option("header", "true").save(destFile)

		val nbTotal = intrusions.size
		if(nbTotal==0){
			println("No known intrusions to check.")
			return
		}
		val entityIndex = distDetected.columns.indexOf(eType)
		val timeIndex = distDetected.columns.indexOf("timeinterval")
		val toCheck = distDetected.collect.zipWithIndex
		val init:List[Boolean] = Nil
		val (results, remaining) = intrusions.foldLeft((init, toCheck)){case ((results, prevCheck), intrusion) => 
			val (res, newCheck) = checkIntrusion(entityIndex, intrusion, prevCheck)
			(res::results, newCheck)
		}
		val nbDetected = results.filter(_==true).size
		val recall = 100.0*(nbDetected.toDouble/nbTotal.toDouble)
		println("Number of known intrusions detected (Recall) : "+nbDetected+"/"+nbTotal+" = "+recall+"%\n")
	}

	private def generateIntrusion(intrusionKind: IntrusionKind, minTimestamp: Long,
		maxBeginIntrusionTime: Long, intrusionTime: Duration):Intrusion = {
		val minT = (abs(r.nextLong())%(maxBeginIntrusionTime - minTimestamp)) + minTimestamp
		val maxT = minT + intrusionTime.toMillis
		intrusionKind.getIntrusion(minT, maxT)
	}

	private def checkIntrusion(entityIndex: Int, intrusion: Intrusion, detected: Array[(Row,Int)]):(Boolean, Array[(Row,Int)])  = {
		val index = detected.indexWhere{case (row, _) => row.getString(entityIndex) == intrusion.src}
		if(index != -1){
			val row = detected(index)._1
			println("Detected : "+row.mkString(" "))
			(true, detected.filterNot(d => d._2==index))
		}else{
			(false, detected)
		}
	}

	def persistIntrusions():Unit={
		this.intrusions.zipWithIndex.map{case (intrusion, index) =>
			val oos = new ObjectOutputStream(new FileOutputStream("intrusions/intrusion"+index, false))
			oos.writeObject(intrusion)
			oos.close()
		}
	}

	def loadIntrusions():Unit={
		val fileNames = getListOfFiles("intrusions").map(_.getName())
		this.intrusions = fileNames.map{ name =>
			val ois = new ObjectInputStream(new FileInputStream("intrusions/"+name))
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