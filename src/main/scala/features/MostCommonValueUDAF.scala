/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
*/
package features
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MostCommonValueUDAF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)

  override def bufferSchema: StructType = StructType(
    StructField("frequencyMap", DataTypes.createMapType(DoubleType, LongType)) :: Nil
  )

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for the buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map[Double, Long]()
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    val inp = input.getAs[Double](0)
    val existingMap = buffer.getAs[Map[Double, Long]](0)
    buffer(0) = existingMap + (if (existingMap.contains(inp)) inp -> (existingMap(inp) + 1) else inp -> 1L)
  }

  // This is how you merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val map1 = buffer1.getAs[Map[Double, Long]](0)
    val map2 = buffer2.getAs[Map[Double, Long]](0)
    buffer1(0) =  map1 ++ map2.map{ case (k,v) => k -> (v + map1.getOrElse(k,0L)) }
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Double = {
    buffer.getAs[Map[Double, Long]](0).maxBy(_._2)._1
  }
}