package sparklyr

import java.io.{DataInputStream, DataOutputStream}
import java.io.{File, FileOutputStream, IOException}
import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
import java.util.concurrent.TimeUnit

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelFuture, ChannelInitializer, EventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}

import scala.util.Try

class BackendChannel(logger: Logger, terminate: () => Unit, serializer: Serializer, tracker: JVMObjectTracker) {

  private[this] var channelFuture: ChannelFuture = null
  private[this] var bootstrap: ServerBootstrap = null
  private[this] var bossGroup: EventLoopGroup = null
  private[this] var inetAddress: InetSocketAddress = null
  private[this] var hostContext: String = null

  def setHostContext(hostContextParam: String) {
    hostContext = hostContextParam
  }

  def init(remote: Boolean, port: Int, deterministicPort: Boolean, preCommandHooks: Option[Runnable]): Int = {
    if (remote) {
      val anyIpAddress = Array[Byte](0, 0, 0, 0)
      val anyInetAddress = InetAddress.getByAddress(anyIpAddress)

      val channelPort = if (deterministicPort) Utils.nextPort(port, anyInetAddress) else 0
      logger.log("is using port " + channelPort + " for backend channel")

      inetAddress = new InetSocketAddress(anyInetAddress, channelPort)
    }
    else {
      val channelPort = if (deterministicPort) Utils.nextPort(port, InetAddress.getLoopbackAddress()) else 0
      logger.log("is using port " + channelPort + " for backend channel")

      inetAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), channelPort)
    }

    bossGroup = new NioEventLoopGroup(BackendConf.getNumThreads)
    val workerGroup = bossGroup
    val handler = new BackendHandler(() => this.close(), logger, hostContext, serializer, tracker, preCommandHooks)

    bootstrap = new ServerBootstrap()
      .group(bossGroup, workerGroup)
      .channel(classOf[NioServerSocketChannel])

    bootstrap.childHandler(new ChannelInitializer[SocketChannel]() {
      def initChannel(ch: SocketChannel): Unit = {
        ch.pipeline()
          .addLast("encoder", new ByteArrayEncoder())
          .addLast("frameDecoder",
            new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4))
          .addLast("decoder", new ByteArrayDecoder())
          .addLast("handler", handler)
      }
    })

    channelFuture = bootstrap.bind(inetAddress)
    channelFuture.syncUninterruptibly()
    channelFuture.channel().localAddress().asInstanceOf[InetSocketAddress].getPort()
  }

  def run(): Unit = {
    channelFuture.channel.closeFuture().syncUninterruptibly()
  }

  def close(): Unit = {
    terminate()

    if (channelFuture != null) {
      // close is a local operation and should finish within milliseconds; timeout just to be safe
      channelFuture.channel().close().awaitUninterruptibly(10, TimeUnit.SECONDS)
      channelFuture = null
    }
    if (bootstrap != null && bootstrap.group() != null) {
      bootstrap.group().shutdownGracefully()
    }
    if (bootstrap != null && bootstrap.childGroup() != null) {
      bootstrap.childGroup().shutdownGracefully()
    }
    bootstrap = null
  }

}
