package zio.web.codec

import java.nio.charset.Charset

import zio.stream.ZTransducer
import zio.web.schema._
import zio.{ Chunk, ZIO, ZManaged }

object ProtobufCodec extends Codec {
  override def encoder[A](schema: Schema[A]): ZTransducer[Any, Nothing, A, Byte] =
    ZTransducer(ZManaged.succeed(chunk => ZIO.succeed(Encoder.encodeChunk(schema, chunk))))

  override def decoder[A](schema: Schema[A]): ZTransducer[Any, String, Byte, A] =
    ZTransducer(ZManaged.succeed(chunk => ZIO.fail("Decoder not implemented: " + chunk)))

  object Encoder {

    def encodeChunk[A](schema: Schema[A], chunk: Option[Chunk[A]]): Chunk[Byte] =
      chunk.map(_.flatMap(encode(schema, _))).getOrElse(Chunk.empty)

    private def encode[A](schema: Schema[A], value: A): Chunk[Byte] = {
      System.out.println("ENCODE !!SCHEMA!! " + schema + "     !! VALUE !! " + value)
      (schema, value) match {
        case (Schema.Record(structure), v: Map[String, _]) => encodeRecord(structure, v)
        case (Schema.Sequence(element), v: Chunk[_])       => encodeSequence(element, v)
        case (Schema.Enumeration(_), _)                    => Chunk(100.byteValue())
        case (Schema.Transform(codec, _, g), _)            => g(value).map(encode(codec, _)).getOrElse(Chunk.empty)
        case (Schema.Primitive(standardType), v)           => encodeStandardType(standardType, v)
        case (Schema.Tuple(left, right), v @ (_, _))       => encodeTuple(left, right, v)
        case (Schema.Optional(codec), v: Option[_])        => encodeOptional(codec, v)
        case (_, _) => {
          System.out.println("skipping " + schema + " " + value)
          Chunk.empty
        }
      }
    }

    type FieldCodec[A] = (Schema[A], A)

    private def encodeField(
      structure: Map[String, Schema[_]],
      data: Map[String, _],
      field: String
    ): Chunk[Byte] =
      structure
        .get(field)
        .flatMap(schema => data.get(field).map(value => (schema.asInstanceOf[Schema[Any]], value)))
        .map {
          case (schema, value) =>
            tag(schema, 1) ++ encode(schema, value)
        }
        .getOrElse(Chunk.empty)

    // TODO check: width only for non-root?
    private def encodeRecord(structure: Map[String, Schema[_]], data: Map[String, _]): Chunk[Byte] = {
      System.out.println("RECORD " + structure)
      System.out.println("RECORD " + data)
      val chunk = Chunk.fromIterable(data.keys.map(field => encodeField(structure, data, field))).flatten
      System.out.println("RECORD chunk " + asHex(chunk))
      varInt(chunk.size) ++ chunk
    }

    private def encodeSequence[A](element: Schema[A], chunk: Chunk[A]): Chunk[Byte] = {
      System.out.println("SEQ " + element)
      System.out.println("SEQ " + chunk)
      Chunk(100.byteValue()) // TODO
    }

    // TODO defaults
    // TODO varInts: 32, 64, unsigned, zigZag, fixed
    private def encodeStandardType[A](standardType: StandardType[A], value: A): Chunk[Byte] = {
      System.out.println("STANDARD " + value)
      (standardType, value) match {
        case (StandardType.UnitType, _) => Chunk.empty
        case (StandardType.StringType, str: String) => {
          val encoded = Chunk.fromArray(str.getBytes(Charset.forName("UTF-8"))) // TODO ZIO NIO?
          val result  = varInt(encoded.size) ++ encoded
          System.out.println("STRING " + str + " " + asHex(result))
          result
        }
        case (StandardType.BoolType, b: Boolean)  => varInt(if (b) 1 else 0)
        case (StandardType.ShortType, v: Short)   => varInt(v.toInt)
        case (StandardType.IntType, v: Int)       => varInt(v)
        case (StandardType.LongType, v: Long)     => varInt(v.toInt)
        case (StandardType.FloatType, _: Float)   => Chunk.empty // TODO
        case (StandardType.DoubleType, _: Double) => Chunk.empty // TODO
        case (StandardType.ByteType, byte: Byte)  => varInt(1) :+ byte // TODO must be Chunk[Byte]?
        case (StandardType.CharType, _: Char)     => Chunk.empty // TODO
        case (_, _) => {
          System.out.println("skipping " + standardType + " " + value)
          Chunk.empty
        }
      }
    }

    private def encodeTuple[A, B](left: Schema[A], right: Schema[B], tuple: (A, B)): Chunk[Byte] =
      encode(
        Schema.record(Map("left" -> left, "right" -> right)),
        Map[String, Any]("left" -> tuple._1, "right" -> tuple._2)
      )

    private def encodeOptional[A](schema: Schema[A], value: Option[A]): Chunk[Byte] =
      encode(
        Schema.record(Map("value" -> schema)),
        Map("value" -> value)
      )

    private def varInt(value: Int): Chunk[Byte] = {
      val base128    = value & 0x7F;
      val higherBits = value >>> 7;
      if (higherBits != 0x00) {
        (0x80 | base128).byteValue() +: varInt(higherBits)
      } else {
        Chunk(base128.byteValue())
      }
    }

    private def tag(schema: Schema[_], fieldNumber: Int): Chunk[Byte] =
      varInt(fieldNumber << 3 | wireType(schema))

    @scala.annotation.tailrec
    private def wireType(schema: Schema[_]): Int = schema match {
      case _: Schema.Record               => 2
      case _: Schema.Sequence[_]          => 2
      case _: Schema.Enumeration          => 2
      case Schema.Transform(codec, _, _)  => wireType(codec)
      case Schema.Primitive(standardType) => wireType(standardType)
      case _: Schema.Tuple[_, _]          => 2
      case _: Schema.Optional[_]          => 2
    }

    private def wireType(standardType: StandardType[_]): Int = standardType match {
      case StandardType.UnitType   => 0
      case StandardType.StringType => 0
      case StandardType.BoolType   => 0
      case StandardType.ShortType  => 0
      case StandardType.IntType    => 0
      case StandardType.LongType   => 0
      case StandardType.FloatType  => 5
      case StandardType.DoubleType => 1
      case StandardType.ByteType   => 2
      case StandardType.CharType   => 2
    }

    // TODO remove: for debugging only
    def asHex(chunk: Chunk[Byte]): String =
      "0x" + chunk.toArray.map("%02X".format(_)).mkString
  }
}
