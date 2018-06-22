/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
* In applying this licence, CERN does not waive the privileges and immunities granted to it by virtue of its 
* status as an Intergovernmental Organization or submit itself to any jurisdiction.
*/
package evaluation
/*
An Intrusion is the result of the injection of fake logs by an IntrusionKind on some data.
The Intrusion is made by a fake source host, in a time interval and has a signature of
all the logs of the Intrusion. 
*/
@SerialVersionUID(100L)
case class Intrusion(
	val kind: IntrusionKind,
	val src: String,
	val beginTimestamp: Long,
	val endTimestamp: Long,
	val signature: String
) extends Serializable{

	def check(signature: String):Boolean = {
		this.signature == signature
	}

	def findMatch(detectedSignatures: List[String]):Int = {
		detectedSignatures.indexWhere(check(_))
	}

	override def toString():String = {
		src+" is involved in a "+kind.name+" intrusion between "+beginTimestamp+" and "+endTimestamp+"."
	}
}
