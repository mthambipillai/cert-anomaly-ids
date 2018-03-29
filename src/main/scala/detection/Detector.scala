package detection
import org.apache.spark.sql.DataFrame

/*
Any intrusion detection algorithm must extend this class.
*/
abstract class Detector(){
	/*
	Computes a DataFrame of detected intrusions along with their scores. The
	original DataFrame must be defined by the constructor of the concrete class.
	*/
	def detect(threshold: Double):DataFrame
}