package zio.web.codec

import java.nio.charset.Charset
import java.nio.{ ByteBuffer, ByteOrder }

import zio.stream.ZTransducer
import zio.web.schema._
import zio.{ Chunk, ZIO, ZManaged }

import scala.collection.SortedMap

object ProtobufCodec extends Codec {
  override def encoder[A](schema: Schema[A]): ZTransducer[Any, Nothing, A, Byte] =
    ZTransducer(ZManaged.succeed(chunk => ZIO.succeed(Encoder.encodeChunk(schema, chunk))))

  override def decoder[A](schema: Schema[A]): ZTransducer[Any, String, Byte, A] =
    ZTransducer(ZManaged.succeed(chunk => ZIO.fromEither(Decoder.decodeChunk(schema, chunk))))

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

    private def encode[A](fieldNumber: Option[Int], schema: Schema[A], value: A): Chunk[Byte] =
      (schema, value) match {
        case (Schema.Record(structure), v: SortedMap[String, _]) => encodeRecord(fieldNumber, structure, v)
        case (Schema.Sequence(element), v: Chunk[_])             => encodeSequence(fieldNumber, element, v)
        case (Schema.Enumeration(_), _)                          => Chunk.empty // TODO: is this oneOf?
        case (Schema.Transform(codec, _, g), _)                  => g(value).map(encode(fieldNumber, codec, _)).getOrElse(Chunk.empty)
        case (Schema.Primitive(standardType), v)                 => encodePrimitive(fieldNumber, standardType, v)
        case (Schema.Tuple(left, right), v @ (_, _))             => encodeTuple(fieldNumber, left, right, v)
        case (Schema.Optional(codec), v: Option[_])              => encodeOptional(fieldNumber, codec, v)
        case (_, _)                                              => Chunk.empty
      }

    // TODO check: width only for non-root?
    private def encodeRecord(
      fieldNumber: Option[Int],
      structure: SortedMap[String, Schema[_]],
      data: SortedMap[String, _]
    ): Chunk[Byte] =
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

    // TODO packed
    private def encodeSequence[A](
      fieldNumber: Option[Int],
      element: Schema[A],
      sequence: Chunk[A]
    ): Chunk[Byte] =
      if (packed(element)) {
        val chunk = sequence.flatMap(value => encode(None, element, value))
        tag(LengthDelimited(chunk.size), fieldNumber) ++ chunk
      } else {
        sequence.flatMap(value => encode(fieldNumber, element, value))
      }

    // TODO defaults
    // TODO varInts: 32, 64, unsigned, zigZag, fixed
    @scala.annotation.tailrec
    private def encodePrimitive[A](
      fieldNumber: Option[Int],
      standardType: StandardType[A],
      value: A
    ): Chunk[Byte] =
      (standardType, value) match {
        case (StandardType.UnitType, _) => Chunk.empty
        case (StandardType.StringType, str: String) => {
          val encoded = Chunk.fromArray(str.getBytes(Charset.forName("UTF-8"))) // TODO ZIO NIO?
          tag(LengthDelimited(encoded.size), fieldNumber) ++ encoded
        }
        case (StandardType.BoolType, b: Boolean) => tag(VarInt, fieldNumber) ++ varInt(if (b) 1 else 0)
        case (StandardType.ShortType, v: Short)  => tag(VarInt, fieldNumber) ++ varInt(v.toLong)
        case (StandardType.IntType, v: Int)      => tag(VarInt, fieldNumber) ++ varInt(v)
        case (StandardType.LongType, v: Long)    => tag(VarInt, fieldNumber) ++ varInt(v)
        case (StandardType.FloatType, v: Float) => {
          val byteBuffer = ByteBuffer.allocate(4)
          byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
          byteBuffer.putFloat(v)
          tag(Bit32, fieldNumber) ++ Chunk.fromArray(byteBuffer.array)
        }
        case (StandardType.DoubleType, v: Double) => {
          val byteBuffer = ByteBuffer.allocate(8)
          byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
          byteBuffer.putDouble(v)
          tag(Bit64, fieldNumber) ++ Chunk.fromArray(byteBuffer.array)
        }
        case (StandardType.ByteType, byte: Byte) =>
          tag(LengthDelimited(1), fieldNumber) :+ byte // TODO must be Chunk[Byte]?
        case (StandardType.CharType, c: Char) => encodePrimitive(fieldNumber, StandardType.StringType, c.toString)
        case (_, _)                           => Chunk.empty
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
  }

  object Decoder {

    def decodeChunk[A](schema: Schema[A], chunk: Option[Chunk[Byte]]): Either[String, Chunk[A]] =
      chunk.map(decode(schema, _)).map(_.map(Chunk(_))).getOrElse(Left("Failed decoding empty bytes"))

    private def decode[A](schema: Schema[A], chunk: Chunk[Byte]): Either[String, A] = {
      System.out.println("DECODE " + schema)
      System.out.println("DECODE " + chunk)
      schema match {
        case Schema.Record(structure)       => decodeRecord(structure, chunk).asInstanceOf[Either[String, A]]
        case Schema.Sequence(element)       => decodeSequence(element, chunk).asInstanceOf[Either[String, A]]
        case Schema.Enumeration(_)          => Left("Enumeration is not yet supported")
        case Schema.Transform(codec, f, _)  => decodeTransform(codec, f, chunk)
        case Schema.Primitive(standardType) => decodePrimitive(standardType, chunk)
        case Schema.Tuple(left, right)      => decodeTuple(left, right, chunk).asInstanceOf[Either[String, A]]
        case Schema.Optional(codec)         => decodeOptional(codec, chunk).asInstanceOf[Either[String, A]]
      }
    }

    private def decodeRecord[A](
      schema: SortedMap[String, Schema[_]],
      chunk: Chunk[Byte]
    ): Either[String, SortedMap[String, _]] = {
      System.out.println("STANDARD " + schema)
      System.out.println("STANDARD " + chunk)
      Right(SortedMap())
    }

    private def decodeSequence[A](schema: Schema[A], chunk: Chunk[Byte]): Either[String, List[A]] = {
      System.out.println("STANDARD " + schema)
      System.out.println("STANDARD " + chunk)
      Right(List())
    }
    private def decodeTransform[A, B](
      schema: Schema[B],
      f: B => Either[String, A],
      chunk: Chunk[Byte]
    ): Either[String, A] =
      decode(schema, chunk).flatMap(f)

    private def decodePrimitive[A](standardType: StandardType[_], chunk: Chunk[Byte]): Either[String, A] = {
      System.out.println("STANDARD " + chunk)
      standardType match {
        case StandardType.UnitType   => Right(()).asInstanceOf[Either[String, A]]
        case StandardType.StringType => Right("").asInstanceOf[Either[String, A]]
        case StandardType.BoolType   => Right(false).asInstanceOf[Either[String, A]]
        case StandardType.ShortType  => Right(0).asInstanceOf[Either[String, A]]
        case StandardType.IntType    => Right(0).asInstanceOf[Either[String, A]]
        case StandardType.LongType   => Right(0).asInstanceOf[Either[String, A]]
        case StandardType.FloatType  => Right(0).asInstanceOf[Either[String, A]]
        case StandardType.DoubleType => Right(0).asInstanceOf[Either[String, A]]
        case StandardType.ByteType   => Right(0.byteValue()).asInstanceOf[Either[String, A]]
        case StandardType.CharType   => Right('a').asInstanceOf[Either[String, A]]
      }
    }

    private def decodeTuple[A, B](left: Schema[A], right: Schema[B], chunk: Chunk[Byte]): Either[String, (A, B)] = {
      System.out.println("STANDARD " + left)
      System.out.println("STANDARD " + right)
      System.out.println("STANDARD " + chunk)
      decode(Schema.record(SortedMap("left" -> left, "right" -> right)), chunk)
        .flatMap(
          record =>
            (record.get("left"), record.get("right")) match {
              case (Some(l), Some(r)) => Right((l.asInstanceOf[A], r.asInstanceOf[B]))
              case _                  => Left("Failed decoding tuple")
            }
        )
    }

    private def decodeOptional[A](schema: Schema[_], chunk: Chunk[Byte]): Either[String, Option[A]] = {
      System.out.println("STANDARD " + schema)
      System.out.println("STANDARD " + chunk)
      decode(Schema.record(SortedMap("value" -> schema)), chunk)
        .map(record => record.get("value").asInstanceOf[Option[A]])
    }
  }
}
