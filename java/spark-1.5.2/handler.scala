package sparklyr

@io.netty.channel.ChannelHandler.Sharable
class BackendHandler(
  close: () => Unit,
  logger: Logger,
  hostContext: String) extends io.netty.channel.SimpleChannelInboundHandler[Array[Byte]] {

  import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

  import scala.language.existentials

  import io.netty.channel.ChannelHandlerContext

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    val objId = Serializer.readString(dis)
    val isStatic = Serializer.readBoolean(dis)
    val methodName = Serializer.readString(dis)
    val numArgs = Serializer.readInt(dis)

    var reply: Array[Byte] = null

    if (objId == "Handler") {
      methodName match {
        case "stopBackend" =>
          Serializer.writeInt(dos, 0)
          Serializer.writeType(dos, "void")
          close()

          reply = bos.toByteArray
        case "terminateBackend" =>
          Serializer.writeInt(dos, 0)
          Serializer.writeType(dos, "void")
          close()

          System.exit(0)
        case _ =>
          reply = StreamHandler.read(msg, null, logger, hostContext)
      }
    } else {
      reply = StreamHandler.read(msg, null, logger, hostContext)
    }

    ctx.write(reply)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}
