package detectors
import org.apache.spark.sql.DataFrame

abstract class Detector(){
	def detect(threshold: Double):DataFrame
}