package zio.web.codec

import zio.Chunk
import zio.stream.{ ZSink, ZStream }
import zio.test.Assertion._
import zio.test._
import zio.web.schema.Schema

object ProtobufCodecSpec extends DefaultRunnableSpec {
  case class Test1(a: Int)

  val schemaTest1: Schema[Test1] = Schema.caseClassN(
    "a" -> Schema[Int]
  )(Test1, Test1.unapply)

  val test1 = Test1(150)

  case class SearchRequest(query: String, pageNumber: Int, resultPerPage: Int)

  val schema: Schema[SearchRequest] = Schema.caseClassN(
    "query"         -> Schema[String],
    "pageNumber"    -> Schema[Int],
    "resultPerPage" -> Schema[Int]
  )(SearchRequest, SearchRequest.unapply)

  val message: SearchRequest = SearchRequest("bitcoins", 0, 100)

  def spec = suite("ProtobufCodec Spec")(
    suite("Toplevel ProtobufCodec Spec")(
      testM("Should encode correctly") {
        assertM(encode(schemaTest1, test1).map(asHex))(
          equalTo("0x03089601")
        )
      },
      testM("Should encode and decode successfully") {
        assertM(encodeAndDecode(schema, message).fold(identity, _ => "SUCCESS"))(
          equalTo("TODO")
        )
      }
    )
  )

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
