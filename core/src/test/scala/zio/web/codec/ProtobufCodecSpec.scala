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
      testM("Should encode and decode successfully") {
        assertM(encodeAndDecode(schema, message).fold(identity, _ => "SUCCESS"))(
          equalTo("TODO")
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

  // TODO Generators

  case class SearchRequest(query: String, pageNumber: Int, resultPerPage: Int)

  val schema: Schema[SearchRequest] = Schema.caseClassN(
    "query"         -> Schema[String],
    "pageNumber"    -> Schema[Int],
    "resultPerPage" -> Schema[Int]
  )(SearchRequest, SearchRequest.unapply)

  val message: SearchRequest = SearchRequest("bitcoins", 0, 100)

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
