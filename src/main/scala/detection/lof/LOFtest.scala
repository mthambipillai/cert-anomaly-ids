package lof
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.PriorityQueue
import org.apache.spark.broadcast.Broadcast

class LOFtest(spark: SparkSession, k: Int) extends Serializable{

	object KNNOrder extends Ordering[(Int,Double)] {
		def compare(x:(Int,Double), y:(Int,Double)) = x._2 compare y._2
	}

	def transform(data: DataFrame):DataFrame = {
		val featuresIndex = data.columns.indexOf("features")
		val featuresIndexB = spark.sparkContext.broadcast(featuresIndex)
		val kB = spark.sparkContext.broadcast(k)

		val lsh = getHashes(data, featuresIndexB).repartition(col("hash")).drop("hash")
		println("nb parts : "+lsh.rdd.partitions.size)
		val rdd = lsh.rdd.mapPartitionsWithIndex{ case (pIndex, iter) => {
			val featuresIndex = featuresIndexB.value
			val k = kB.value
			val rows = iter.toArray
			val withKDists = getKDistances(pIndex, rows, featuresIndex, k)
			val withReachDists = getReachabilityDistances(withKDists)
			val withLRDs = getLRDs(withReachDists, k)
			getLOFs(withLRDs, k).toIterator
		}}
		val lofField = StructField("lof", DoubleType, true)
		val newSchema = StructType(data.schema ++ Seq(lofField))
		val res = spark.sqlContext.createDataFrame(rdd, newSchema)
		res
	}

	private def getKDistances(pIndex: Int, rows: Array[Row], featuresIndex: Int, k: Int):List[Row] = {
		//println("Computing KNNs and their distances...")
		val distances = computeDistanceMatrix(rows, featuresIndex)
		val partID = ""+pIndex
		rows.toList.zipWithIndex.map{ case (r,index) => {
			val rID = (partID+index).toInt
			val (kDist, knns) = getKNN(index, distances(index), k, partID)
			val (ids, dists) = knns.unzip
			Row.fromSeq(rID +: (Seq(kDist, ids.toArray, dists.toArray) ++ r.toSeq))
		}}
		//schema: r => rID | kDist | knnIDs | knnDists | r
	}

	private def getReachabilityDistances(rows: List[Row]):List[Row] = {
		//println("Computing reachability distances...")
		rows.map(r => {
			val knnIDs = r.getAs[Array[Int]](2)
			val knnDists = r.getAs[Array[Double]](3)
			val (newIDs, reachDists) = knnIDs.zip(knnDists).flatMap{ case (id, dist) => 
				getReachDist(rows, id, dist).map(d => (id, d))}.unzip
			val seq = r.toSeq
			Row.fromSeq(Seq(seq.head, newIDs, reachDists) ++ seq.drop(4))
		})
		//schema: rID | kDist | knnIDs | knnDists | r => rID | knnIDs | knnReachDists | r
	}

	private def getLRDs(rows: List[Row], k: Int):List[Row] = {
		//println("Computing local reachability densities...")
		rows.map(r => {
			val reachDists = r.getAs[Array[Double]](2)
			val lrd = reachDists.sum / k.toDouble
			val seq = r.toSeq
			Row.fromSeq(seq.take(2) ++ (lrd +: seq.drop(3)))
		})
		//schema: rID | knnIDs | knnReachDists | r => rID | knnIDs | LRD | r
	}

	private def getLOFs(rows: List[Row], k: Int):List[Row] = {
		//println("Computing local outlier factors...")
		rows.map(r => {
			val knnIDs = r.getAs[Array[Int]](1)
			val knnLRDs = knnIDs.flatMap(id =>
				rows.find(r => r.getInt(0)==id).map(_.getDouble(2)))
			val lof = knnLRDs.sum / k.toDouble
			val seq = r.toSeq
			Row.fromSeq(seq.drop(3) :+ lof)

		})
		//schema: rID | knnIDs | LRD | r => r, LOF
	}

	private def getReachDist(rows: List[Row], rowID: Int, dist: Double):Option[Double] = {
		getRowKDist(rows, rowID).map(kd => math.max(kd, dist))
	}
	private def getRowKDist(rows: List[Row], rowID: Int):Option[Double] = {
		rows.find(r => r.getInt(0)==rowID).map(_.getDouble(1))
	}

	private def computeDistanceMatrix(rows: Array[Row], featuresIndex: Int):Array[Array[Double]] = {
		val distances = Array.ofDim[Double](rows.length,rows.length)
		for(i <- 0 to rows.length - 2 ){
			for(j <- i+1 to rows.length - 1 ){
				val d = distance(rows(i), rows(j), featuresIndex)
				distances(i)(j) = d
				distances(j)(i) = d
			}
		}
		distances
	}

	private def getKNN(sourceIndex: Int, distancesToOthers: Array[Double], k: Int,
		partID: String): (Double,List[(Int,Double)]) = {
		var count = 0
		var kDist = 0.0
		val maxHeap = PriorityQueue.empty(KNNOrder)

		def addToHeap(i: Int, d: Double):Unit = {
			maxHeap.enqueue(((partID+i).toInt,d))
			if(d > kDist){
				kDist = d
			}
		}

		for(i <- 0 to distancesToOthers.length - 1){
			if(i!=sourceIndex){
				val d = distancesToOthers(i)
				if(count < k){
					addToHeap(i, d)
				}else{
					val maxDist = maxHeap.head._2
					if(d < maxDist){
						maxHeap.dequeue()
						addToHeap(i, d)
					}
				}
				count = count + 1
			}
		}
		(kDist, maxHeap.toList)
	}

	private def distance(r1: Row, r2: Row, featuresIndex: Int):Double = {
		val v1 = r1.getAs[Vector](featuresIndex)
		val v2 = r2.getAs[Vector](featuresIndex)
		Vectors.sqdist(v1, v2)
	}

	private def localSensitiveHash(r: Row, as: List[List[Double]], featuresIndex: Int):Row = {
		val v = r.getAs[Vector](featuresIndex).toArray.toList
		var total = 0.0
		as.foreach(a => total+= a.zip(v).foldLeft(0.0){case (sum,(ai, vi)) => sum + ai*vi})
		val nbDigits = math.pow(10,8).toInt
		val hash = math.floor(total*nbDigits)/nbDigits
		Row.fromSeq(r.toSeq :+ hash)
	}

	private def getHashes(df: DataFrame, featuresIndexB: Broadcast[Int]):DataFrame = {
		println("Computing the hashes...")
		val newSchema = df.schema.add(StructField("hash", DoubleType, true))
		val encoder = RowEncoder(newSchema)
		val nbFeatures = df.columns.filter(c => c!="srcentity" && c!="dstentity"
		&& c!="timeinterval" && c!="features").size

		val primes = List(2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,101,103,107,
			109,113,127,131,137,139,149,151,157,163,167,173,179,181,191,193,197,199,211,223,227,229)
		val a = primes.take(nbFeatures).map(_.toDouble)
		val r = scala.util.Random
		val as = (1 to 10).map(i => r.shuffle(a)).toList
		val asB = spark.sparkContext.broadcast(as)
		df.mapPartitions(iter => {
			val featuresIndex = featuresIndexB.value
			val as = asB.value
			iter.map(r => localSensitiveHash(r, as, featuresIndex))
		})(encoder)
	}

	private def meanDistToOthers(rows: Array[Row], index: Int, featuresIndex: Int):Double = {
		if(rows.length==1) return 0.0
		(0 to rows.length - 1).foldLeft(0.0){case (prevSum, i) => {
			if(i!=index){
				prevSum + distance(rows(index), rows(i), featuresIndex)
			}else{
				prevSum
			}
		}}
	}

	private def meanDist(rows: Array[Row], featuresIndex: Int):Double = {
		if(rows.length==0) return 0.0
		val meanDists = (0 to rows.length - 1).map(i => meanDistToOthers(rows, i, featuresIndex))
		meanDists.sum / (rows.length*rows.length).toDouble
	}

	private def getMeanDist(df: DataFrame, featuresIndex: Int, accName: String):Double = {
		val counter = spark.sparkContext.doubleAccumulator(accName)
		val featuresIndexB = spark.sparkContext.broadcast(featuresIndex)
		df.foreachPartition(iter => {
			val featuresIndex = featuresIndexB.value
			var arr = iter.toArray
			val res = meanDist(arr, featuresIndex)
			counter.add(res)
		})
		counter.value
	}
}
