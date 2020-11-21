package zio.web.codec

import zio.Chunk
import zio.stream.{ ZSink, ZStream }
import zio.test.Assertion._
import zio.test._
import zio.web.schema.Schema

object ProtobufCodecSpec extends DefaultRunnableSpec {

  def spec = suite("ProtobufCodec Spec")(
    suite("Toplevel ProtobufCodec Spec")(
      testM("Should encode Protobuf Test1 correctly") {
        assertM(encode(schemaTest1, Test1(150)).map(asHex))(
          equalTo("0x03089601")
        )
      },
      testM("Should encode Protobuf Test2 correctly") {
        assertM(encode(schemaTest2, Test2("testing")).map(asHex))(
          equalTo("0x090A0774657374696E67")
        )
      },
      testM("Should encode Protobuf Test3 correctly") {
        assertM(encode(schemaTest3, Test3(Test1(150))).map(asHex))(
          equalTo("0x050A03089601")
        )
      },
      testM("Should encode Protobuf Test4 correctly") {
        assertM(encode(schemaTest4, Test4(List(3, 270, 86942))).map(asHex))(
          equalTo("0x080A06038E029EA705")
        )
      },
      testM("Should encode and decode successfully") {
        assertM(encodeAndDecode(schema, message).fold(identity, _ => "SUCCESS"))(
          equalTo("TODO")
        )
      }
    )
  )

  // see https://developers.google.com/protocol-buffers/docs/encoding
  // note that unlike the protobuf test cases we always get field number 1

  case class Test1(a: Int)

  val schemaTest1: Schema[Test1] = Schema.caseClassN(
    "a" -> Schema[Int]
  )(Test1, Test1.unapply)

  case class Test2(b: String)

  val schemaTest2: Schema[Test2] = Schema.caseClassN(
    "b" -> Schema[String]
  )(Test2, Test2.unapply)

  case class Test3(c: Test1)

  val schemaTest3: Schema[Test3] = Schema.caseClassN(
    "c" -> schemaTest1
  )(Test3, Test3.unapply)

  case class Test4(d: List[Int])

  val schemaTest4: Schema[Test4] = Schema.caseClassN(
    "d" -> Schema.list(Schema[Int])
  )(Test4, Test4.unapply)

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
