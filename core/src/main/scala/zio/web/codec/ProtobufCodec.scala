package zio.web.codec

import java.nio.charset.Charset

import zio.stream.ZTransducer
import zio.web.schema._
import zio.{ Chunk, ZIO, ZManaged }

import scala.collection.SortedMap

object ProtobufCodec extends Codec {
  override def encoder[A](schema: Schema[A]): ZTransducer[Any, Nothing, A, Byte] =
    ZTransducer(ZManaged.succeed(chunk => ZIO.succeed(Encoder.encodeChunk(schema, chunk))))

  override def decoder[A](schema: Schema[A]): ZTransducer[Any, String, Byte, A] =
    ZTransducer(ZManaged.succeed(chunk => ZIO.fail("Decoder not implemented: " + chunk)))

  sealed trait WireType {}

  case object VarInt                     extends WireType
  case object Bit64                      extends WireType
  case class LengthDelimited(width: Int) extends WireType
  case object StartGroup                 extends WireType
  case object EndGroup                   extends WireType
  case object Bit32                      extends WireType

  object Encoder {

    def encodeChunk[A](schema: Schema[A], chunk: Option[Chunk[A]]): Chunk[Byte] =
      chunk.map(_.flatMap(encode(None, schema, _))).getOrElse(Chunk.empty)

    private def encode[A](fieldNumber: Option[Int], schema: Schema[A], value: A): Chunk[Byte] = {
      System.out.println("ENCODE !!SCHEMA!! " + schema + "     !! VALUE !! " + value)
      (schema, value) match {
        case (Schema.Record(structure), v: SortedMap[String, _]) => encodeRecord(fieldNumber, structure, v)
        case (Schema.Sequence(element), v: Chunk[_])             => encodeSequence(fieldNumber, element, v)
        case (Schema.Enumeration(_), _)                          => Chunk.empty // TODO
        case (Schema.Transform(codec, _, g), _)                  => g(value).map(encode(fieldNumber, codec, _)).getOrElse(Chunk.empty)
        case (Schema.Primitive(standardType), v)                 => encodeStandardType(fieldNumber, standardType, v)
        case (Schema.Tuple(left, right), v @ (_, _))             => encodeTuple(fieldNumber, left, right, v)
        case (Schema.Optional(codec), v: Option[_])              => encodeOptional(fieldNumber, codec, v)
        case (_, _) => {
          System.out.println("skipping " + schema + " " + value)
          Chunk.empty
        }
      }
    }

    // TODO check: width only for non-root?
    private def encodeRecord(
      fieldNumber: Option[Int],
      structure: SortedMap[String, Schema[_]],
      data: SortedMap[String, _]
    ): Chunk[Byte] = {
      System.out.println("RECORD " + structure)
      System.out.println("RECORD " + data)
      System.out.println("RECORD " + fieldNumber)
      Chunk
        .fromIterable(structure.zipWithIndex.map {
          case ((field, schema), index) =>
            data
              .get(field)
              .map(value => (schema.asInstanceOf[Schema[Any]], value))
              .map {
                case (schema, value) =>
                  encode(Some(index + 1), schema, value)
              }
              .getOrElse(Chunk.empty)
        })
        .map(chunk => tag(LengthDelimited(chunk.size), fieldNumber) ++ chunk)
        .flatten
    }

    // TODO packed
    private def encodeSequence[A](
      fieldNumber: Option[Int],
      element: Schema[A],
      sequence: Chunk[A]
    ): Chunk[Byte] = {
      System.out.println("SEQ " + element)
      System.out.println("SEQ " + sequence)
      if (packed(element)) {
        val chunk = sequence.flatMap(value => encode(None, element, value))
        tag(LengthDelimited(chunk.size), fieldNumber) ++ chunk
      } else {
        sequence.flatMap(value => encode(fieldNumber, element, value))
      }
    }

    // TODO defaults
    // TODO varInts: 32, 64, unsigned, zigZag, fixed
    private def encodeStandardType[A](
      fieldNumber: Option[Int],
      standardType: StandardType[A],
      value: A
    ): Chunk[Byte] = {
      System.out.println("STANDARD " + value)
      (standardType, value) match {
        case (StandardType.UnitType, _) => Chunk.empty
        case (StandardType.StringType, str: String) => {
          val encoded = Chunk.fromArray(str.getBytes(Charset.forName("UTF-8"))) // TODO ZIO NIO?
          tag(LengthDelimited(encoded.size), fieldNumber) ++ encoded
        }
        case (StandardType.BoolType, b: Boolean)  => tag(VarInt, fieldNumber) ++ varInt(if (b) 1 else 0)
        case (StandardType.ShortType, v: Short)   => tag(VarInt, fieldNumber) ++ varInt(v.toLong)
        case (StandardType.IntType, v: Int)       => tag(VarInt, fieldNumber) ++ varInt(v)
        case (StandardType.LongType, v: Long)     => tag(VarInt, fieldNumber) ++ varInt(v)
        case (StandardType.FloatType, _: Float)   => Chunk.empty // TODO
        case (StandardType.DoubleType, _: Double) => Chunk.empty // TODO
        case (StandardType.ByteType, byte: Byte) =>
          tag(LengthDelimited(1), fieldNumber) :+ byte // TODO must be Chunk[Byte]?
        case (StandardType.CharType, c: Char) => encodeStandardType(fieldNumber, StandardType.StringType, c.toString)
        case (_, _) => {
          System.out.println("skipping " + standardType + " " + value)
          Chunk.empty
        }
      }
    }

    private def encodeTuple[A, B](
      fieldNumber: Option[Int],
      left: Schema[A],
      right: Schema[B],
      tuple: (A, B)
    ): Chunk[Byte] =
      encode(
        fieldNumber,
        Schema.record(SortedMap("left" -> left, "right" -> right)),
        SortedMap[String, Any]("left" -> tuple._1, "right" -> tuple._2)
      )

    private def encodeOptional[A](fieldNumber: Option[Int], schema: Schema[A], value: Option[A]): Chunk[Byte] =
      encode(
        fieldNumber,
        Schema.record(SortedMap("value" -> schema)),
        SortedMap("value" -> value)
      )

    private def varInt(value: Int): Chunk[Byte] =
      varInt(value.toLong)

    private def varInt(value: Long): Chunk[Byte] = {
      val base128    = value & 0x7F;
      val higherBits = value >>> 7;
      if (higherBits != 0x00) {
        (0x80 | base128).byteValue() +: varInt(higherBits)
      } else {
        Chunk(base128.byteValue())
      }
    }

    private def tag(wireType: WireType, fieldNumber: Option[Int]): Chunk[Byte] =
      fieldNumber.map { num =>
        val encodeTag = (base3: Int) => varInt(num << 3 | base3)
        wireType match {
          case VarInt                  => encodeTag(0)
          case Bit64                   => encodeTag(1)
          case LengthDelimited(length) => encodeTag(2) ++ varInt(length)
          case StartGroup              => encodeTag(3)
          case EndGroup                => encodeTag(4)
          case Bit32                   => encodeTag(5)
        }
      }.getOrElse(Chunk.empty)

    private def packed(schema: Schema[_]): Boolean = schema match {
      case _: Schema.Record               => false
      case Schema.Sequence(element)       => packed(element)
      case _: Schema.Enumeration          => false
      case Schema.Transform(codec, _, _)  => packed(codec)
      case Schema.Primitive(standardType) => packed(standardType)
      case _: Schema.Tuple[_, _]          => false
      case _: Schema.Optional[_]          => false
    }

    private def packed(standardType: StandardType[_]): Boolean = standardType match {
      case StandardType.UnitType   => false
      case StandardType.StringType => false
      case StandardType.BoolType   => true
      case StandardType.ShortType  => true
      case StandardType.IntType    => true
      case StandardType.LongType   => true
      case StandardType.FloatType  => true
      case StandardType.DoubleType => true
      case StandardType.ByteType   => false
      case StandardType.CharType   => true
    }

    // TODO remove: for debugging only
    def asHex(chunk: Chunk[Byte]): String =
      "0x" + chunk.toArray.map("%02X".format(_)).mkString
  }
}
