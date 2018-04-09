//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

@_root_.io.netty.channel.ChannelHandler.Sharable
class BackendHandler(
  close: () => Unit,
  logger: Logger,
  hostContext: String,
  serializer: Serializer,
  tracker: JVMObjectTracker) extends _root_.io.netty.channel.SimpleChannelInboundHandler[Array[Byte]] {

  import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

  import scala.language.existentials

  import _root_.io.netty.channel.ChannelHandlerContext

  var streamHandler = new StreamHandler(serializer, tracker)

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    val objId = serializer.readString(dis)
    val isStatic = serializer.readBoolean(dis)
    val methodName = serializer.readString(dis)
    val numArgs = serializer.readInt(dis)

    var reply: Array[Byte] = null

    if (objId == "Handler") {
      methodName match {
        case "stopBackend" =>
          serializer.writeInt(dos, 0)
          serializer.writeType(dos, "void")
          close()

          reply = bos.toByteArray
        case "terminateBackend" =>
          serializer.writeInt(dos, 0)
          serializer.writeType(dos, "void")
          close()

          System.exit(0)
        case _ =>
          reply = streamHandler.read(msg, null, logger, hostContext)
      }
    } else {
      reply = streamHandler.read(msg, null, logger, hostContext)
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
