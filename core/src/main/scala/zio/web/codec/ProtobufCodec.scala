package zio.web.codec

import java.nio.charset.StandardCharsets
import java.nio.{ ByteBuffer, ByteOrder }

import zio.stream.ZTransducer
import zio.web.schema._
import zio.{ Chunk, ZIO, ZManaged }

import scala.collection.SortedMap

// TODO handle missing values (defaults)
// TODO decide on int default: 32, 64, unsigned, zigZag, fixed
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

    private def encodeSequence[A](
      fieldNumber: Option[Int],
      element: Schema[A],
      sequence: Chunk[A]
    ): Chunk[Byte] =
      if (canBePacked(element)) {
        val chunk = sequence.flatMap(value => encode(None, element, value))
        tag(LengthDelimited(chunk.size), fieldNumber) ++ chunk
      } else {
        sequence.flatMap(value => encode(fieldNumber, element, value))
      }

    @scala.annotation.tailrec
    private def encodePrimitive[A](
      fieldNumber: Option[Int],
      standardType: StandardType[A],
      value: A
    ): Chunk[Byte] =
      (standardType, value) match {
        case (StandardType.UnitType, _) => Chunk.empty
        case (StandardType.StringType, str: String) => {
          val encoded = Chunk.fromArray(str.getBytes(StandardCharsets.UTF_8))
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
      val base128    = value & 0x7F
      val higherBits = value >>> 7
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

    @scala.annotation.tailrec
    private def canBePacked(schema: Schema[_]): Boolean = schema match {
      case _: Schema.Record               => false
      case Schema.Sequence(element)       => canBePacked(element)
      case _: Schema.Enumeration          => false
      case Schema.Transform(codec, _, _)  => canBePacked(codec)
      case Schema.Primitive(standardType) => canBePacked(standardType)
      case _: Schema.Tuple[_, _]          => false
      case _: Schema.Optional[_]          => false
    }

    private def canBePacked(standardType: StandardType[_]): Boolean = standardType match {
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

    trait Decoder[A] { self =>
      def run(input: (Chunk[Byte], WireType)): Either[String, (Chunk[Byte], A)]

      def map[B](f: A => B): Decoder[B] =
        (input: (Chunk[Byte], WireType)) =>
          self.run(input).map {
            case (remainder, value) => (remainder, f(value))
          }

      def flatMap[B](f: A => Decoder[B]): Decoder[B] =
        (input: (Chunk[Byte], WireType)) =>
          self.run(input).flatMap {
            case (remainder, value) => f(value).run((remainder, input._2))
          }
    }

    def decodeChunk[A](schema: Schema[A], chunk: Option[Chunk[Byte]]): Either[String, Chunk[A]] =
      chunk
        .map(bs => decoder(schema).run((bs, LengthDelimited(bs.size))))
        .map(_.map(result => Chunk(result._2)))
        .getOrElse(Right(Chunk.empty))

    private def decoder[A](schema: Schema[A]): Decoder[A] =
      schema match {
        case Schema.Record(structure)       => recordDecoder(structure).asInstanceOf[Decoder[A]]
        case Schema.Sequence(element)       => sequenceDecoder(element).asInstanceOf[Decoder[A]]
        case Schema.Enumeration(_)          => _ => Left("Enumeration is not yet supported") // FIXME
        case Schema.Transform(codec, f, _)  => transformDecoder(codec, f)
        case Schema.Primitive(standardType) => primitiveDecoder(standardType)
        case Schema.Tuple(left, right)      => tupleDecoder(left, right).asInstanceOf[Decoder[A]]
        case Schema.Optional(codec)         => optionalDecoder(codec).asInstanceOf[Decoder[A]]
      }

    private def recordDecoder[A](structure: SortedMap[String, Schema[_]]): Decoder[SortedMap[String, _]] =
      recordLoopDecoder(structure, SortedMap())

    private def recordLoopDecoder(
      structure: SortedMap[String, Schema[_]],
      result: SortedMap[String, _]
    ): Decoder[SortedMap[String, _]] =
      input =>
        if (input._1.isEmpty) {
          Right((input._1, result))
        } else {
          recordLoopStepDecoder(structure, result).run(input)
        }

    private def recordLoopStepDecoder[A](
      structure: SortedMap[String, Schema[_]],
      result: SortedMap[String, _]
    ): Decoder[SortedMap[String, _]] =
      unTag.flatMap {
        case (wireType, fieldNumber) =>
          val field = structure.keys.toSeq(fieldNumber.toInt - 1)
          structure
            .get(field)
            .map { schema =>
              fieldDecoder(wireType, schema).flatMap { value =>
                val updatedResult = result.concat(Seq(field -> value))
                recordLoopDecoder(structure, updatedResult)
              }
            }
            .getOrElse(_ => Left("Invalid structure"))
      }

    private def fieldDecoder[A](wireType: WireType, schema: Schema[A]): Decoder[A] =
      (input: (Chunk[Byte], WireType)) => decoder(schema).run((input._1, wireType))

    private def sequenceDecoder[A](schema: Schema[A]): Decoder[List[A]] =
      input =>
        input._2 match {
          case LengthDelimited(_) => Left("Packed sequence is not yet supported: " + schema)     // FIXME
          case _                  => Left("Non-packed sequence is not yet supported: " + schema) // FIXME
        }

    private def transformDecoder[A, B](schema: Schema[B], f: B => Either[String, A]): Decoder[A] =
      decoder(schema).flatMap(a => input => f(a).map(b => (input._1, b)))

    private def primitiveDecoder[A](standardType: StandardType[_]): Decoder[A] =
      standardType match {
        case StandardType.UnitType   => ((chunk: Chunk[Byte]) => Right((chunk, ()))).asInstanceOf[Decoder[A]]
        case StandardType.StringType => stringDecoder.asInstanceOf[Decoder[A]]
        case StandardType.BoolType   => packedDecoder(VarInt, unVarInt).map(_ != 0).asInstanceOf[Decoder[A]]
        case StandardType.ShortType  => packedDecoder(VarInt, unVarInt).map(_.shortValue()).asInstanceOf[Decoder[A]]
        case StandardType.IntType    => packedDecoder(VarInt, unVarInt).map(_.intValue()).asInstanceOf[Decoder[A]]
        case StandardType.LongType   => packedDecoder(VarInt, unVarInt).asInstanceOf[Decoder[A]]
        case StandardType.FloatType  => floatDecoder.asInstanceOf[Decoder[A]]
        case StandardType.DoubleType => doubleDecoder.asInstanceOf[Decoder[A]]
        case StandardType.ByteType   => byteDecoder.asInstanceOf[Decoder[A]]
        case StandardType.CharType   => stringDecoder.map(_.charAt(0)).asInstanceOf[Decoder[A]]
      }

    private def tupleDecoder[A, B](left: Schema[A], right: Schema[B]): Decoder[(A, B)] =
      decoder(Schema.record(SortedMap("left" -> left, "right" -> right)))
        .flatMap(
          record =>
            input =>
              (record.get("left"), record.get("right")) match {
                case (Some(l), Some(r)) => Right((input._1, (l.asInstanceOf[A], r.asInstanceOf[B])))
                case _                  => Left("Failed decoding tuple")
              }
        )

    private def optionalDecoder[A](schema: Schema[_]): Decoder[Option[A]] =
      decoder(Schema.record(SortedMap("value" -> schema)))
        .map(record => record.get("value").asInstanceOf[Option[A]])

    private def stringDecoder: Decoder[String] = lengthDelimitedDecoder { length => input =>
      val (str, remainder) = input._1.splitAt(length)
      Right((remainder, new String(str.toArray, StandardCharsets.UTF_8)))
    }

    private def floatDecoder: Decoder[Float] =
      packedDecoder(
        Bit32,
        input => {
          val (float, remainder) = input._1.splitAt(4)
          val byteBuffer         = ByteBuffer.allocate(4)
          byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
          byteBuffer.put(float.toArray)
          Right((remainder, byteBuffer.getFloat()))
        }
      )

    private def doubleDecoder: Decoder[Double] =
      packedDecoder(
        Bit64,
        input => {
          val (double, remainder) = input._1.splitAt(8)
          val byteBuffer          = ByteBuffer.allocate(8)
          byteBuffer.order(ByteOrder.LITTLE_ENDIAN)
          byteBuffer.put(double.toArray)
          Right((remainder, byteBuffer.getDouble))
        }
      )

    private def byteDecoder: Decoder[Byte] = lengthDelimitedDecoder { _ => input =>
      val (byte, remainder) = input._1.splitAt(1)
      Right((remainder, byte(0)))
    }

    private def lengthDelimitedDecoder[A](decoder: Int => Decoder[A]): Decoder[A] =
      input =>
        input._2 match {
          case LengthDelimited(length) => decoder(length).run(input)
          case _                       => Left("Invalid wire type")
        }

    private def packedDecoder[A](decoderWireType: WireType, decoder: Decoder[A]): Decoder[A] =
      input =>
        input._2 match {
          case LengthDelimited(_)               => decoder.run(input)
          case _ if decoderWireType == input._2 => decoder.run(input)
          case _                                => Left("Invalid wire type")
        }

    private def unTag: Decoder[(WireType, Long)] =
      unVarInt.flatMap { tag => input =>
        val fieldNumber = tag >>> 3
        (tag & 0x07) match {
          case 0 => Right((input._1, (VarInt, fieldNumber)))
          case 1 => Right((input._1, (Bit64, fieldNumber)))
          case 2 => unVarInt.map(length => (LengthDelimited(length.toInt), fieldNumber)).run(input)
          case 3 => Right((input._1, (StartGroup, fieldNumber)))
          case 4 => Right((input._1, (EndGroup, fieldNumber)))
          case 5 => Right((input._1, (Bit32, fieldNumber)))
          case _ => Left("Failed decoding tag")
        }
      }

    private def unVarInt: Decoder[Long] =
      input =>
        if (input._1.isEmpty) {
          Left("Unexpected end of stream")
        } else {
          val index = input._1.indexWhere(b => (b.longValue() & 0x80) != 0x80) + 1
          val value = input._1.take(index).foldLeft(0L)((v, b) => (v << 7) + b)
          Right((input._1.drop(index), value))
        }
  }
}
