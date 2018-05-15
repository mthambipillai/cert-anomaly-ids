/*
* © Copyright 2018 CERN
* This software is distributed under the terms of the GNU General Public Licence version 3 (GPL  
* Version 3), copied verbatim in the file “COPYING”.
*/
package isolationforest

case class ColumnHelper(
	val name: String,
	val dtype: String,
	val index: Int,
	val minVal: AnyVal,
	val maxVal: AnyVal
)