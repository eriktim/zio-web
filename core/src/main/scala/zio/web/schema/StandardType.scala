package zio.web.schema

import zio.Chunk

sealed trait StandardType[A]

object StandardType {
  implicit object UnitType   extends StandardType[Unit]
  implicit object StringType extends StandardType[String]
  implicit object BoolType   extends StandardType[Boolean]
  implicit object ShortType  extends StandardType[Short]
  implicit object IntType    extends StandardType[Int]
  implicit object LongType   extends StandardType[Long]
  implicit object FloatType  extends StandardType[Float]
  implicit object DoubleType extends StandardType[Double]
  implicit object ByteType   extends StandardType[Chunk[Byte]]
  implicit object CharType   extends StandardType[Char]

  // TODO: Add java.time
}
