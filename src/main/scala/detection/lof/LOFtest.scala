package lof
import org.apache.spark.sql.DataFrame
class LOF2(){

	def transform(data: DataFrame):DataFrame = {
		???
	}

	private def getKDistances(data: DataFrame):DataFrame = {
		???
		//should extend the schema with Vector[Double] of size k representing the distances between
		//the point and each of the knn, k distance is the max value in the vector
		//r => r,(d(r,n1),d(r,n2),d(r,n3),...)
	}

	private def getReachabilityDistances(withKDistances: DataFrame):DataFrame = {
		???
		//updates distance with reachability distance for each knn, schema doesnt change
		//kdist(r) = max(d(r,n1),d(r,n2),d(r,n3),...)
		//r,(d(r,n1),d(r,n2),d(r,n3),...) => r,(d(r,n1),d(r,n2),d(r,n3),...)
	}

	private def getLRDs(withReachabilities: DataFrame):DataFrame = {
		???
		//extend the schema with a Double for the local reachability density, computed
		//from the reachabilities.
	}
}