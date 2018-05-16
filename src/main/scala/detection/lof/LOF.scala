/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
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

/*
Implements the Local Outlier Factor algorithm with a custom LSH technique for in-partition computing.
More details can be found in the wiki.
*/
class LOF(spark: SparkSession, k: Int, hashNbDigits: Int, hashNbVects: Int) extends Serializable{

	object KNNOrder extends Ordering[(Int,Double)] {
		def compare(x:(Int,Double), y:(Int,Double)) = x._2 compare y._2
	}

	/*
	Appends a 'lof' column to 'data' with the LOF scores computed from the features. The data
	is first repartitioned according to LSH so that the computation can be done within each
	partition independantly.
	*/
	def transform(data: DataFrame):DataFrame = {
		val featuresIndex = data.columns.indexOf("features")
		val featuresIndexB = spark.sparkContext.broadcast(featuresIndex)
		val kB = spark.sparkContext.broadcast(k)

		val lsh = getHashes(data, featuresIndexB).repartition(col("hash")).drop("hash")
		println("Computing LOF for each hash partition...")
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

	/*
	Computes the k-distance and the distances to each kNN for each row in 'rows'. The IDs for each
	row are computed as well by using their index in the array and the partition index 'pIndex'.
	The schema is extended in the following way : r => rID | kDist | knnIDs | knnDists | r.
	*/
	private def getKDistances(pIndex: Int, rows: Array[Row], featuresIndex: Int, k: Int):List[Row] = {
		val distances = computeDistanceMatrix(rows, featuresIndex)
		val partID = ""+pIndex
		rows.toList.zipWithIndex.map{ case (r,index) => {
			val rID = (partID+index).toInt
			val (kDist, knns) = getKNNs(index, distances(index), k, partID)
			val (ids, dists) = knns.unzip
			Row.fromSeq(rID +: (Seq(kDist, ids.toArray, dists.toArray) ++ r.toSeq))
		}}
	}

	/*
	Computes for each row in 'rows' the reachability distances for each kNN from the
	k-distance and the distances to the kNNs. The schema is extended in the following way :
	rID | kDist | knnIDs | knnDists | r => rID | knnIDs | knnReachDists | r
	*/
	private def getReachabilityDistances(rows: List[Row]):List[Row] = {
		rows.map(r => {
			val knnIDs = r.getAs[Array[Int]](2)
			val knnDists = r.getAs[Array[Double]](3)
			val (newIDs, reachDists) = knnIDs.zip(knnDists).flatMap{ case (id, dist) => 
				getReachDist(rows, id, dist).map(d => (id, d))}.unzip
			val seq = r.toSeq
			Row.fromSeq(Seq(seq.head, newIDs, reachDists) ++ seq.drop(4))
		})
	}

	/*
	Computes for each row in 'rows' the local reachability density from the 
	reachability distances. The schema is extended in the following way :
	rID | knnIDs | knnReachDists | r => rID | knnIDs | LRD | r
	*/
	private def getLRDs(rows: List[Row], k: Int):List[Row] = {
		rows.map(r => {
			val reachDists = r.getAs[Array[Double]](2)
			val lrd = reachDists.sum / k.toDouble
			val seq = r.toSeq
			Row.fromSeq(seq.take(2) ++ (lrd +: seq.drop(3)))
		})
	}

	/*
	Computes for each row in 'rows' the local outlier factor from the local reachability
	density of the row and the ones of the kNNs. The schema is extended in the following way :
	rID | knnIDs | LRD | r => r, LOF
	*/
	private def getLOFs(rows: List[Row], k: Int):List[Row] = {
		rows.map(r => {
			val knnIDs = r.getAs[Array[Int]](1)
			val knnLRDs = knnIDs.flatMap(id =>
				rows.find(r => r.getInt(0)==id).map(_.getDouble(2)))
			val lof = knnLRDs.sum / k.toDouble
			val seq = r.toSeq
			Row.fromSeq(seq.drop(3) :+ lof)

		})
	}

	private def getReachDist(rows: List[Row], rowID: Int, dist: Double):Option[Double] = {
		getRowKDist(rows, rowID).map(kd => math.max(kd, dist))
	}
	private def getRowKDist(rows: List[Row], rowID: Int):Option[Double] = {
		rows.find(r => r.getInt(0)==rowID).map(_.getDouble(1))
	}

	/*
	Computes the distances between each row in 'rows' and return them as a 2 dimensional matrix.
	*/
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

	/*
	Finds the k-Nearest Neighbours (kNN) for the row with index 'sourceIndex' using
	the previously computed distances to other rows 'distancesToOthers'. Returns
	the k-distance (maximum of the distances to kNNs) and the list of kNNs IDs (using
	the partition index 'partID') and their distances to the considered row.
	*/
	private def getKNNs(sourceIndex: Int, distancesToOthers: Array[Double], k: Int,
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

	/*
	Computes the squared euclidean distance between r1 and r2.
	*/
	private def distance(r1: Row, r2: Row, featuresIndex: Int):Double = {
		val v1 = r1.getAs[Vector](featuresIndex)
		val v2 = r2.getAs[Vector](featuresIndex)
		Vectors.sqdist(v1, v2)
	}

	/*
	Computes the locality-sensitive hash for the row 'r' using the 'as' vectors as described
	in the documentation. It returns the row extended with a new field for the hash.
	*/
	private def localSensitiveHash(r: Row, as: List[List[Double]], featuresIndex: Int):Row = {
		val v = r.getAs[Vector](featuresIndex).toArray.toList
		val total = as.foldLeft(0.0){case (sum, a) => sum + dotProduct(a,v)}
		val avg = total / as.size.toDouble
		val nbDigitsPow = math.pow(10,hashNbDigits).toInt
		val hash = math.floor(total*nbDigitsPow)/nbDigitsPow
		Row.fromSeq(r.toSeq :+ hash)
	}

	private def dotProduct(a: List[Double], v: List[Double]):Double = {
		a.zip(v).foldLeft(0.0){case (sum,(ai, vi)) => sum + ai*vi}
	}

	/*
	Computes the locality-sensitive hashes for every row in 'df' and put them in a 
	new 'hash' column.
	*/
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
		val as = (1 to hashNbVects).map(i => r.shuffle(a)).toList
		val asB = spark.sparkContext.broadcast(as)
		df.mapPartitions(iter => {
			val featuresIndex = featuresIndexB.value
			val as = asB.value
			iter.map(r => localSensitiveHash(r, as, featuresIndex))
		})(encoder)
	}
}