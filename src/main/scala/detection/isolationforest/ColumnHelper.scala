package isolationforest

case class ColumnHelper(
	val name: String,
	val dtype: String,
	val index: Int,
	val minVal: AnyVal,
	val maxVal: AnyVal
)