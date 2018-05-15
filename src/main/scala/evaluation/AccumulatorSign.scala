/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package evaluation
import org.apache.spark.util.AccumulatorV2
import java.math.BigInteger
import java.security.MessageDigest
import org.apache.spark.sql.Row

class AccumulatorSign extends AccumulatorV2[Row,String]{
	private var hashes: List[String] = Nil
	private var hashed = ""

	@Override
	def add(r: Row): Unit = {
		hashes = insertSorted(hash(r.mkString), hashes)
	}

	@Override
	def copy(): AccumulatorV2[Row,String] = {
		val cp = new AccumulatorSign()
		cp.hashes = hashes
		cp
	}

	@Override
	def isZero: Boolean = hashes.size==0

	@Override
	def merge(other: AccumulatorV2[Row,String]): Unit = {
		hashes = hashes:::List(other.value)
	}

	@Override
	def reset(): Unit = {
		hashes = Nil
	}

	@Override
	def value(): String = hash(hashes.mkString)

	private def hash(s: String): String = {
		String.format("%032x",
			new BigInteger(1, MessageDigest.getInstance("SHA-256").digest(s.getBytes("UTF-8"))))
	}

	private def insertSorted(hash: String, hashes: List[String]): List[String] = hashes match {
		case head::tail => {
			if(hash.compareTo(head)<1){
				List(hash, head):::tail
			}else{
				head::insertSorted(hash, tail)
			}
		}
		case Nil => List(hash)
	}
}