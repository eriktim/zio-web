package zio.web.codec

import zio.stream.ZTransducer
import zio.web.schema._
import zio.{ Chunk, ZIO, ZManaged }

object ProtobufCodec extends Codec {
  override def encoder[A](schema: Schema[A]): ZTransducer[Any, Nothing, A, Byte] =
    ZTransducer(ZManaged.succeed(_ => ZIO.succeed(Chunk(0.byteValue()))))

  override def decoder[A](schema: Schema[A]): ZTransducer[Any, String, Byte, A] =
    ZTransducer(ZManaged.succeed(_ => ZIO.fail("Decoder not implemented")))
}
