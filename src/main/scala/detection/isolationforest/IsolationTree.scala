/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package isolationforest
import scala.util.Random
import org.apache.spark.sql.functions._
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
	def pathLength(columns: Array[(String,String,Int)], rows: Seq[Row], pl:Int):Seq[Double]
}
/*
Leaf node, as explained in (1).
*/
case class ExNode(size: Int) extends IsolationTree{
	private val c:Double = {//average path length as given in section 2 of (1)
		if(size<=1) 0 else {
			val h = log(size-1)+0.5772156649
			2*h - (2*(size-1)/size)
		}
	}

	override def pathLength(columns: Array[(String,String,Int)], rows: Seq[Row], pl:Int):Seq[Double] = {
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

	override def pathLength(columns: Array[(String,String,Int)], rows: Seq[Row], pl:Int):Seq[Double] = {
		val (colName, colType, index) = columns.filter(x => x._1==splitCol).head
		if(rows.isEmpty) return Seq.empty[Double]
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
		val leftLengths = left.pathLength(columns, leftRows, pl+1)
		val rightLengths = right.pathLength(columns, rightRows, pl+1)
		leftLengths ++ rightLengths
	}
}


object IsolationTree{
	private val r = new Random(System.currentTimeMillis())

	def apply(columns: List[ColumnHelper], train: Array[Row], limit: Int): IsolationTree = {
		val selectedColumns = scala.util.Random.shuffle(columns).take(limit)
		build(selectedColumns, train, 0, limit)
	}

	/*
	Builds an IsolationTree from the samples in 'train' starting at height 'currentHeight' with the limit 'heightLimit'.
	The schema is provided by 'columns'
	*/
	private def build(columns: List[ColumnHelper], train: Array[Row], currentHeight: Int, heightLimit: Int): IsolationTree = {
		if(currentHeight >= heightLimit || train.length<=1){
			ExNode(train.length)
		}else{
			val col = columns.head
			val index = col.index
			val (splitLeft, splitRight, splitValue) = col.dtype match {
				case "LongType" => {
					val minVal = col.minVal.asInstanceOf[Long]
					val maxVal = col.maxVal.asInstanceOf[Long]
					val splitValue = minVal + r.nextDouble()*(maxVal-minVal)
					def splitLeft(r:Row):Boolean = {r.getLong(index) <= splitValue}
					def splitRight(r:Row):Boolean = {r.getLong(index) > splitValue}
					(splitLeft _, splitRight _, splitValue)
				}
				case "DoubleType" => {
					val minVal = col.minVal.asInstanceOf[Double]
					val maxVal = col.maxVal.asInstanceOf[Double]
					val splitValue = minVal + r.nextDouble()*(maxVal-minVal)
					def splitLeft(r:Row):Boolean = {r.getDouble(index) <= splitValue}
					def splitRight(r:Row):Boolean = {r.getDouble(index) > splitValue}
					(splitLeft _, splitRight _, splitValue)
				}
				case _ => throw new Exception("Unable to parse the column data type : "+col.dtype)
			}

			val trainLeft = train.filter(splitLeft)
			val trainRight = train.filter(splitRight)
			
			val leftTree = build(columns.tail, trainLeft, currentHeight+1,heightLimit)
			val rightTree = build(columns.tail, trainRight, currentHeight+1,heightLimit)
			InNode(leftTree, rightTree, col.name, splitValue)
		}
	}
}