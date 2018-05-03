package lof
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.PriorityQueue

class LOFtest(spark: SparkSession) extends Serializable{

	object KNNOrder extends Ordering[(Int,Double)] {
		def compare(x:(Int,Double), y:(Int,Double)) = y._2 compare x._2
	}

	def transform(data: DataFrame):DataFrame = {
		val featuresIndex = data.columns.indexOf("features")

		//println("before : "+getMeanDist(data.repartition(90).orderBy(rand()), featuresIndex, "c1"))

		val lsh = getHashes(data, featuresIndex).repartition(col("hash")).drop("hash")

		//println("nb parts : "+res.rdd.getNumPartitions)
		//println("nb hashes : "+res.select("hash").distinct.count)
		//res.groupBy("hash").count().orderBy(desc("count")).show()
		//res.groupBy("hash").count().orderBy(asc("count")).show()

		//println("after : "+getMeanDist(res, featuresIndex, "c2"))

		val knn = getKDistances(lsh, featuresIndex, 4)
		knn.show()

		System.exit(1)
		knn
	}

	private def getKDistances(data: DataFrame, featuresIndex: Int, k: Int):DataFrame = {
		val featuresIndexB = spark.sparkContext.broadcast(featuresIndex)
		val kB = spark.sparkContext.broadcast(k)
		val rdd = data.rdd.mapPartitionsWithIndex{ case (pIndex, iter) => {
			val rows = iter.toArray
			val featuresIndex = featuresIndexB.value
			val k = kB.value
			val distances = computeDistanceMatrix(rows, featuresIndex)
			val partID = ""+pIndex
			rows.toList.zipWithIndex.map{ case (r,index) => {
				val rID = (partID+index).toInt
				val (kDist, knns) = getKNN(index, distances(index), k, partID)
				val (ids, dists) = knns.unzip
				Row.fromSeq(rID +: (r.toSeq ++ Seq(kDist, ids, dists)))
			}}.toIterator
		}}
		val idField = StructField("rid", IntegerType, true)
		val kDistField = StructField("kdist", DoubleType, true)
		val knnIdsField = StructField("knnids", ArrayType(IntegerType), true)
		val knnDistsField = StructField("knndists", ArrayType(DoubleType), true)
		val newSchema = StructType(Seq(idField, kDistField, knnIdsField, knnDistsField) ++ data.schema)
		spark.sqlContext.createDataFrame(rdd, newSchema)
		//should extend the schema with Vector[Double] of size k representing the distances between
		//the point and each of the knn, k distance is the max value in the vector
		//kdist = max(d(r,n1),d(r,n2),d(r,n3),...)
		//r => rid, r, kdist, ((n1id, d(r,n1)),(n2id, d(r,n2)),(n3id, d(r,n3)),...)
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

	private def getReachabilityDistances(withKDistances: DataFrame):DataFrame = {
		withKDistances.rdd.mapPartitionsWithIndex{ case (pIndex, iter) => {
			val rows = iter.toList
			rows.map(r => {
				val knnIDs = r.getAs[Array[Int]](2)
				val knnDists = r.getAs[Array[Double]](3)
				val (newIDs, reachDists) = knnIDs.zip(knnDists).flatMap{ case (id, dist) => 
					getReachDist(rows, id, dist).map(d => (id, d))}.unzip
				val seq = r.toSeq
				Row.fromSeq(Seq(seq.head, newIDs, reachDists) ++ seq.drop(4))
			}).toIterator
		}}
		???
		//updates distance with reachability distance for each knn, schema doesnt change
		//for each nid in ((n1id, d(r,n1)),(n2id, d(r,n2)),(n3id, d(r,n3)),...),
		//lookup row nid, n, ndist, ...
		//and have then rid, r, kdist, ((n1id, d(r,n1), n1dist),(n2id, d(r,n2), n2dist),(n3id, d(r,n3), n3dist),...)
		//apply max(d(r,n),ndist)
		//rid, r, kdist, ((n1id, n1reach),(n2id, n2reach),(n3id, n3reach),...)
	}

	private def getRowKDist(rows: List[Row], rowID: Int):Option[Double] = {
		rows.find(r => r.getInt(0)==rowID).map(_.getDouble(1))
	}
	private def getReachDist(rows: List[Row], rowID: Int, dist: Double):Option[Double] = {
		getRowKDist(rows, rowID).map(kd => math.max(kd, dist))
	}

	private def getLRDs(withReachabilities: DataFrame):DataFrame = {
		???
		//extend the schema with a Double for the local reachability density, computed
		//from the reachabilities.
		//rid, r, kdist, ((n1id, n1reach),(n2id, n2reach),(n3id, n3reach),...)
		//lrd = sum(n1reach, n2reach, ...)/k
		//=> rid, r, lrd, (n1id, n2id, n3id,...)
	}

	private def getLOFs(withLRDs: DataFrame):DataFrame = {
		???
		//for each n in (n1id, n2id, n3id,...), lookup lrd of n
		//=> rid, r, lrd, (n1lrd, n2lrd, n3lrd,...)
		//lof = (sum(n1lrd, n2lrd, n3lrd, ...)/lrd)/k
		//=> rid, r, lof
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
		//val dotProduct = a.zip(v).foldLeft(0.0){case (sum,(ai, vi)) => sum + ai*vi}
		val nbDigits = math.pow(10,7).toInt
		val hash = math.floor(total*nbDigits)/nbDigits
		Row.fromSeq(r.toSeq :+ hash)
	}

	private def getHashes(df: DataFrame, featuresIndex: Int):DataFrame = {
		println("Computing the hashes...")
		val newSchema = df.schema.add(StructField("hash", DoubleType, true))
		val encoder = RowEncoder(newSchema)
		val featuresIndexB = spark.sparkContext.broadcast(featuresIndex)
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
