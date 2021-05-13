package sparklyr

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.language.existentials

import io.netty.channel.ChannelHandlerContext
import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.SimpleChannelInboundHandler

import util.control.Breaks._

@Sharable
class BackendHandler(
  close: () => Unit,
  logger: Logger,
  hostContext: String,
  serializer: Serializer,
  tracker: JVMObjectTracker) extends SimpleChannelInboundHandler[Array[Byte]] {

  var streamHandler = new StreamHandler(serializer, tracker)

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    val objId = Serializer.readString(dis)
    val isStatic = Serializer.readBoolean(dis)
    val returnJObjRef = Serializer.readBoolean(dis)
    val methodName = Serializer.readString(dis)
    val numArgs = Serializer.readInt(dis)

    var needsReply: Boolean = true
    var reply: Array[Byte] = null

    breakable {
      do {
        objId match {
          case "stopBackend" =>
              Serializer.writeInt(dos, 0)
              Serializer.writeType(dos, "void")
              close()

              reply = bos.toByteArray
              break
          case "terminateBackend" =>
              Serializer.writeInt(dos, 0)
              Serializer.writeType(dos, "void")
              close()

              System.exit(0)
          case "rm" =>
              needsReply = false
          case _ =>
        }
        reply = streamHandler.read(msg, null, logger, hostContext)
      } while (false)
    }

    if (needsReply) ctx.write(reply)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause.printStackTrace()
    ctx.close()
  }
}
