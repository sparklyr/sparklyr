package sparklyr

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.language.existentials

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.channel.ChannelHandler.Sharable

import sparklyr.Serializer._
import sparklyr.StreamHandler._

@Sharable
class BackendHandler(
  channel: BackendChannel,
  logger: Logger,
  hostContext: String)
extends SimpleChannelInboundHandler[Array[Byte]] {

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    val objId = readString(dis)
    val isStatic = readBoolean(dis)
    val methodName = readString(dis)
    val numArgs = readInt(dis)

    var reply: Array[Byte] = null

    if (objId == "Handler") {
      methodName match {
        case "stopBackend" =>
          writeInt(dos, 0)
          writeType(dos, "void")
          channel.close()

          reply = bos.toByteArray
        case "terminateBackend" =>
          writeInt(dos, 0)
          writeType(dos, "void")
          channel.close()

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
