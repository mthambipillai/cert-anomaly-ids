package isolationforest
import org.apache.spark.sql.DataFrame
import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.math.log
import org.apache.spark.sql.Row

sealed trait IsolationTree{
	def pathLength(test: DataFrame, pl: Int):DataFrame
}
case class ExNode(size: Int) extends IsolationTree{
	private val c:Double = {
		if(size==0) 0 else {
			val h = log(size-1)+0.5772156649
			2*h - (2*(size-1)/size)
		}
	}
	def pathLength(test: DataFrame, pl: Int):DataFrame = test.withColumn("pathlength",lit(pl + c))
}
case class InNode(
	left: IsolationTree,
	right: IsolationTree,
	splitCol: String,
	splitVal: Double
) extends IsolationTree{
	def pathLength(test: DataFrame, pl: Int):DataFrame = {
		val testLeft = test.filter(col(splitCol) <= splitVal)
		val leftNbParts = testLeft.rdd.partitions.size
		val leftPathLength = left.pathLength(testLeft.repartition(leftNbParts/2), pl+1)
		val testRight = test.filter(col(splitCol) > splitVal)
		val rightNbParts = testRight.rdd.partitions.size
		val rightPathLength = right.pathLength(testRight.repartition(rightNbParts/2), pl+1)
		leftPathLength.union(rightPathLength)
	}
}


object IsolationTree{
	val r = new Random(System.currentTimeMillis())

	def apply(train: DataFrame, limit: Int): IsolationTree = {
		println("Building iTree...")
		build2(train.dtypes, train.collect(), 0, limit)
	}

	val vectUDF = udf((x:Vector) => x.toArray)
	def getUDF(index: Int) = udf((x:Array[Double]) => x(index))

	private def build(train: DataFrame, currentHeight: Int, heightLimit: Int): IsolationTree = {
		if(currentHeight >= heightLimit || train.take(2).length<=1){
			ExNode(train.take(1).length)
		}else{
			val columnsNames = train.columns
			val colName = columnsNames(r.nextInt(columnsNames.length))

			val splitValue = getSplitValue(colName, train)
			val trainLeft = train.filter(col(colName) <= splitValue)
			val trainRight = train.filter(col(colName) > splitValue)
			val leftTree = build(trainLeft, currentHeight+1,heightLimit)
			trainLeft.unpersist()
			val rightTree = build(trainRight, currentHeight+1,heightLimit)
			trainRight.unpersist()
			InNode(leftTree, rightTree, colName, splitValue)
		}
	}

	private def getSplitValue(colName: String, data: DataFrame):Double = {
		val minMax = data.agg(min(colName),max(colName)).head
		try{
			val minVal = minMax.getDouble(0)
			val maxVal = minMax.getDouble(1)
			minVal + r.nextDouble()*(maxVal-minVal)
		}catch{
			case _:ClassCastException => {
				val minVal = minMax.getLong(0)
				val maxVal = minMax.getLong(1)
				minVal + r.nextDouble()*(maxVal-minVal)
			}
		}
	}

	private def build2(columnsNamesTypes: Array[(String,String)], train: Array[Row], currentHeight: Int, heightLimit: Int): IsolationTree = {
		if(currentHeight >= heightLimit || train.length<=1){
			ExNode(train.length)
		}else{
			val (colName,colType) = columnsNamesTypes(r.nextInt(columnsNamesTypes.length))
			val index = train(0).fieldIndex(colName)
			val (splitLeft, splitRight, splitValue) = colType match {
				case "LongType" => {
					val trainVals = train.map(r => r.getLong(index))
					val minVal = trainVals.min
					val maxVal = trainVals.max
					val splitValue = minVal + r.nextDouble()*(maxVal-minVal)
					def splitLeft(r:Row):Boolean = {r.getLong(index) <= splitValue}
					def splitRight(r:Row):Boolean = {r.getLong(index) > splitValue}
					(splitLeft _, splitRight _, splitValue)
				}
				case "DoubleType" => {
					val trainVals = train.map(r => r.getDouble(index))
					val minVal = trainVals.min
					val maxVal = trainVals.max
					val splitValue = minVal + r.nextDouble()*(maxVal-minVal)
					def splitLeft(r:Row):Boolean = {r.getDouble(index) <= splitValue}
					def splitRight(r:Row):Boolean = {r.getDouble(index) > splitValue}
					(splitLeft _, splitRight _, splitValue)
				}
				case _ => throw new Exception("Unable to parse the column data type : "+colType)
			}

			val trainLeft = train.filter(splitLeft)
			val trainRight = train.filter(splitRight)
			
			val leftTree = build2(columnsNamesTypes, trainLeft, currentHeight+1,heightLimit)
			val rightTree = build2(columnsNamesTypes, trainRight, currentHeight+1,heightLimit)
			InNode(leftTree, rightTree, colName, splitValue)
		}
	}
}