package features
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import scala.concurrent.duration._
import org.apache.spark.sql.functions._
import scala.language.postfixOps

class PpidBinaryResolver(spark: SparkSession){


	def splitTimeIntervals(df: DataFrame, interval: Duration):List[DataFrame] = {
		val minMax = df.agg(min("timestamp"),max("timestamp")).head
		val minTime = minMax.getLong(0)
		val maxTime = minMax.getLong(1)
		val nbIntervals = (((maxTime-minTime)/interval.toMillis)+1).toInt
		val newNbPartitions = df.rdd.getNumPartitions * 20
		(1 to nbIntervals).toList.map(i => {
			val lowB = spark.sparkContext.broadcast(minTime.toDouble + (i-1)*interval.toMillis)
			val highB = spark.sparkContext.broadcast(lowB.value + interval.toMillis)
			df.filter(col("timestamp") >= lowB.value && col("timestamp") < highB.value)
				.repartition(newNbPartitions)
		})
	}

	def joinPpidBinary(subdf: DataFrame):DataFrame = {
		val ppidBinaries = subdf.select("pid","ppid","file","dstentity").withColumnRenamed("pid","ppid2")
		.withColumnRenamed("file","ppid_file").withColumnRenamed("ppid","gppid").withColumnRenamed("dstentity","dstentity2")
		subdf.join(ppidBinaries, subdf.col("ppid") === ppidBinaries.col("ppid2")
		&& subdf.col("dstentity") === ppidBinaries.col("dstentity2")).drop("ppid2").drop("dstentity2")
	}

	def resolve(df: DataFrame):DataFrame = {
		println("Begin to split...")
		val subdfs = splitTimeIntervals(df, 30 minutes)
		println("Begin to join...")
		val joined = subdfs.map(joinPpidBinary)
		joined.tail.foldLeft(joined.head){ case (df1, df2) => df1.union(df2)}
	}

}