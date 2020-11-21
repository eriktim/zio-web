package zio.web.codec

import zio.Chunk
import zio.stream.{ ZSink, ZStream }
import zio.test.Assertion._
import zio.test._
import zio.web.schema.Schema

object ProtobufCodecSpec extends DefaultRunnableSpec {
  case class SearchRequest(query: String, pageNumber: Int, resultPerPage: Int)

  val schema: Schema[SearchRequest] = Schema.caseClassN(
    "query"         -> Schema[String],
    "pageNumber"    -> Schema[Int],
    "resultPerPage" -> Schema[Int]
  )(SearchRequest, SearchRequest.unapply)

  val message: SearchRequest = SearchRequest("bitcoins", 0, 100)

  def spec = suite("ProtobufCodec Spec")(
    suite("Toplevel ProtobufCodec Spec")(
      testM("Should encode and decode successfully") {
        assertM(encodeAndDecode(schema, message))(
          equalTo(Chunk(message))
        )
      }
    )
  )

  def encodeAndDecode[A](schema: Schema[A], input: A) =
    ZStream
      .succeed(input)
      .transduce(ProtobufCodec.encoder(schema))
      .transduce(ProtobufCodec.decoder(schema))
      .run(ZSink.collectAll)
}
