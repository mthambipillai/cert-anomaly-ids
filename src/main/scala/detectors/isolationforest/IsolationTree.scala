package isolationforest
import org.apache.spark.sql.DataFrame
import scala.util.Random
import org.apache.spark.sql.functions._
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import scala.math.log
import org.apache.spark.sql.Row

/*
Element of the IsolationForest detector class. More details in "Liu, Ting and Zhou. Isolation Forest" (1).
*/
sealed trait IsolationTree{
	/*
	Computes the lengths in the tree of each row in 'rows' starting with length 'pl'. The
	schema is provided by 'columnsNamesTypes'.
	*/
	def pathLength(columnsNamesTypes: Array[(String,String)], rows: Seq[Row], pl:Int):Seq[Double]
}
/*
Leaf node, as explained in (1).
*/
case class ExNode(size: Int) extends IsolationTree{
	private val c:Double = {//average path length as given in section 2 of (1)
		if(size==0) 0 else {
			val h = log(size-1)+0.5772156649
			2*h - (2*(size-1)/size)
		}
	}

	override def pathLength(columnsNamesTypes: Array[(String,String)], rows: Seq[Row], pl:Int):Seq[Double] = {
		rows.map(r => pl+c)
	}
}
/*
Intermediate node, as explained in (1).
*/
case class InNode(
	left: IsolationTree,
	right: IsolationTree,
	splitCol: String,
	splitVal: Double
) extends IsolationTree{

	override def pathLength(columnsNamesTypes: Array[(String,String)], rows: Seq[Row], pl:Int):Seq[Double] = {
		val (colName, colType) = columnsNamesTypes.filter(x => x._1==splitCol).head
		if(rows.isEmpty) return Seq.empty[Double]
		val index = rows.head.fieldIndex(colName)
		val (leftRows, rightRows) = colType match{
			case "LongType" => {
				val leftSplit = rows.filter(r => r.getLong(index) <= splitVal)
				val rightSplit = rows.filter(r => r.getLong(index) > splitVal)
				(leftSplit, rightSplit)
			}
			case "DoubleType" => {
				val leftSplit = rows.filter(r => r.getDouble(index) <= splitVal)
				val rightSplit = rows.filter(r => r.getDouble(index) > splitVal)
				(leftSplit, rightSplit)
			}
		}
		val leftLengths = left.pathLength(columnsNamesTypes, leftRows, pl+1)
		val rightLengths = right.pathLength(columnsNamesTypes, rightRows, pl+1)
		leftLengths ++ rightLengths
	}
}


object IsolationTree{
	val r = new Random(System.currentTimeMillis())

	def apply(train: DataFrame, limit: Int): IsolationTree = {
		println("Building iTree...")
		build(train.dtypes, train.collect(), 0, limit)
	}

	/*
	Builds an IsolationTree from the samples in 'train' starting at height 'currentHeight' with the limit 'heightLimit'.
	The schema is provided by 'columnsNamesTypes'
	*/
	private def build(columnsNamesTypes: Array[(String,String)], train: Array[Row], currentHeight: Int, heightLimit: Int): IsolationTree = {
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
			
			val leftTree = build(columnsNamesTypes, trainLeft, currentHeight+1,heightLimit)
			val rightTree = build(columnsNamesTypes, trainRight, currentHeight+1,heightLimit)
			InNode(leftTree, rightTree, colName, splitValue)
		}
	}
}