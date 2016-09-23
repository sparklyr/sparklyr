package sparklyr

import java.io.{DataOutputStream, File, FileOutputStream, IOException}
import java.net.{InetAddress, InetSocketAddress, ServerSocket}
import java.util.concurrent.TimeUnit

import io.netty.bootstrap.ServerBootstrap
import io.netty.channel.{ChannelFuture, ChannelInitializer, EventLoopGroup}
import io.netty.channel.nio.NioEventLoopGroup
import io.netty.channel.socket.SocketChannel
import io.netty.channel.socket.nio.NioServerSocketChannel
import io.netty.handler.codec.LengthFieldBasedFrameDecoder
import io.netty.handler.codec.bytes.{ByteArrayDecoder, ByteArrayEncoder}

import org.apache.spark.SparkConf

import sparklyr.Logging._

class Backend {

  private[this] var channelFuture: ChannelFuture = null
  private[this] var bootstrap: ServerBootstrap = null
  private[this] var bossGroup: EventLoopGroup = null

  def init(): Int = {
    val conf = new SparkConf()
    bossGroup = new NioEventLoopGroup(conf.getInt("sparklyr.backend.threads", 2))
    val workerGroup = bossGroup
    val handler = new Handler(this)

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

    channelFuture = bootstrap.bind(new InetSocketAddress("localhost", 0))
    channelFuture.syncUninterruptibly()
    channelFuture.channel().localAddress().asInstanceOf[InetSocketAddress].getPort()
  }

  def run(): Unit = {
    channelFuture.channel.closeFuture().syncUninterruptibly()
  }

  def close(): Unit = {
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

object Backend {
  private[this] var isService: Boolean = false
  private[this] var gatewayServerSocket: ServerSocket = null
  
  def main(args: Array[String]): Unit = {
    if (args.length > 1) {
      System.err.println("Usage: Backend [--service]")
      System.exit(-1)
    }
    
    isService = args.length > 0 && args(0) == "--service"
    
    try {
      gatewayServerSocket = new ServerSocket(8880, 1, InetAddress.getByName("localhost"))
      gatewayServerSocket.setSoTimeout(0)
      
      log("sparklyr listening in port 8880")
    
      while(true) {
        bind()
      }
    } catch {
      case e: IOException =>
        logError("Server shutting down: failed with exception ", e)
        System.exit(1)
    }
    
    System.exit(0)
  }
  
  def bind(): Unit = {
    
    val gatewaySocket = gatewayServerSocket.accept()
    
    log("sparklyr accepted new connection")

    // wait for the end of stdin, then exit
    new Thread("wait for socket to close") {
      setDaemon(true)
      override def run(): Unit = {
        try {
          val buf = new Array[Byte](1024)
          
          log("sparklyr creating backend")
          
          val backend = new Backend()
          val backendPort: Int = backend.init()
          backend.run()
          
          log("sparklyr backend created listening on port " + backendPort)
          
          try {
            val dos = new DataOutputStream(gatewaySocket.getOutputStream())
            dos.writeInt(backendPort)
            dos.close()
            
            gatewaySocket.close()
            
            // wait for the end of socket, closed if R process die
            gatewaySocket.getInputStream().read(buf)
          } finally {
            backend.close()
            log("sparklyr backend closed for connection")
            
            if (!isService) System.exit(0)
          }
        } catch {
          case e: IOException =>
            logError("Backend failed with exception ", e)
            
            if (!isService) System.exit(1)
        }
      }
    }.start() 
  }
}
