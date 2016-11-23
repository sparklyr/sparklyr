package sparklyr

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.language.existentials

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.channel.ChannelHandler.Sharable

import sparklyr.Serializer._
import sparklyr.StreamHandler._

@Sharable
class BackendHandler(server: Backend)
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
          server.close()

          reply = bos.toByteArray
        case "terminateBackend" =>
          writeInt(dos, 0)
          writeType(dos, "void")
          server.close()

          System.exit(0)
        case _ =>
          reply = StreamHandler.read(msg, null)
      }
    } else {
      reply = StreamHandler.read(msg, null)
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
