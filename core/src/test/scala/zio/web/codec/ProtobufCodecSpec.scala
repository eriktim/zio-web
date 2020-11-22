package zio.web.codec

import zio.Chunk
import zio.stream.{ ZSink, ZStream }
import zio.test.Assertion._
import zio.test._
import zio.web.schema.Schema

object ProtobufCodecSpec extends DefaultRunnableSpec {

  def spec = suite("ProtobufCodec Spec")(
    suite("Toplevel ProtobufCodec Spec")(
      testM("Should correctly encode integers") {
        assertM(encode(schemaBasicInt, BasicInt(150)).map(asHex))(
          equalTo("0x089601")
        )
      },
      testM("Should correctly encode strings") {
        assertM(encode(schemaBasicString, BasicString("testing")).map(asHex))(
          equalTo("0x0A0774657374696E67")
        )
      },
      testM("Should correctly encode floats") {
        assertM(encode(schemaBasicFloat, BasicFloat(0.001f)).map(asHex))(
          equalTo("0x0D6F12833A")
        )
      },
      testM("Should correctly encode doubles") {
        assertM(encode(schemaBasicDouble, BasicDouble(0.001)).map(asHex))(
          equalTo("0x09FCA9F1D24D62503F")
        )
      },
      testM("Should correctly encode embedded messages") {
        assertM(encode(schemaEmbedded, Embedded(BasicInt(150))).map(asHex))(
          equalTo("0x0A03089601")
        )
      },
      testM("Should correctly encode packed lists") {
        assertM(encode(schemaPackedList, PackedList(List(3, 270, 86942))).map(asHex))(
          equalTo("0x0A06038E029EA705")
        )
      },
      testM("Should correctly encode unpacked lists") {
        assertM(encode(schemaUnpackedList, UnpackedList(List("foo", "bar", "baz"))).map(asHex))(
          equalTo("0x0A03666F6F0A036261720A0362617A")
        )
      },
      testM("Should correctly encode records") {
        assertM(encode(schemaRecord, Record("Foo", 123)).map(asHex))(
          equalTo("0x0A03466F6F107B")
        )
      },
      testM("Should encode and decode successfully") {
        assertM(encodeAndDecode(schema, message))(
          equalTo(Chunk(message))
        )
      }
    )
  )

  // some tests are based on https://developers.google.com/protocol-buffers/docs/encoding

  case class BasicInt(value: Int)

  val schemaBasicInt: Schema[BasicInt] = Schema.caseClassN(
    "value" -> Schema[Int]
  )(BasicInt, BasicInt.unapply)

  case class BasicString(value: String)

  val schemaBasicString: Schema[BasicString] = Schema.caseClassN(
    "value" -> Schema[String]
  )(BasicString, BasicString.unapply)

  case class BasicFloat(value: Float)

  val schemaBasicFloat: Schema[BasicFloat] = Schema.caseClassN(
    "value" -> Schema[Float]
  )(BasicFloat, BasicFloat.unapply)

  case class BasicDouble(value: Double)

  val schemaBasicDouble: Schema[BasicDouble] = Schema.caseClassN(
    "value" -> Schema[Double]
  )(BasicDouble, BasicDouble.unapply)

  case class Embedded(embedded: BasicInt)

  val schemaEmbedded: Schema[Embedded] = Schema.caseClassN(
    "embedded" -> schemaBasicInt
  )(Embedded, Embedded.unapply)

  case class PackedList(packed: List[Int])

  val schemaPackedList: Schema[PackedList] = Schema.caseClassN(
    "packed" -> Schema.list(Schema[Int])
  )(PackedList, PackedList.unapply)

  case class UnpackedList(items: List[String])

  val schemaUnpackedList: Schema[UnpackedList] = Schema.caseClassN(
    "items" -> Schema.list(Schema[String])
  )(UnpackedList, UnpackedList.unapply)

  case class Record(name: String, value: Int)

  val schemaRecord: Schema[Record] = Schema.caseClassN(
    "name"  -> Schema[String],
    "value" -> Schema[Int]
  )(Record, Record.unapply)

  // TODO: Generators instead of SearchRequest
  case class SearchRequest(query: String, pageNumber: Int, resultPerPage: Int)

  val schema: Schema[SearchRequest] = Schema.caseClassN(
    "query"         -> Schema[String],
    "pageNumber"    -> Schema[Int],
    "resultPerPage" -> Schema[Int]
  )(SearchRequest, SearchRequest.unapply)

  val message: SearchRequest = SearchRequest("bitcoins", 1, 100)

  def asHex(chunk: Chunk[Byte]): String =
    "0x" + chunk.toArray.map("%02X".format(_)).mkString

  def encode[A](schema: Schema[A], input: A) =
    ZStream
      .succeed(input)
      .transduce(ProtobufCodec.encoder(schema))
      .run(ZSink.collectAll)

  def encodeAndDecode[A](schema: Schema[A], input: A) =
    ZStream
      .succeed(input)
      .transduce(ProtobufCodec.encoder(schema))
      .transduce(ProtobufCodec.decoder(schema))
      .run(ZSink.collectAll)
}
