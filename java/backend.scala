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
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

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
  private[this] var port: Int = 0
  private[this] var sessionId: Int = 0
  
  private var hc: HiveContext = null
  
  def getOrCreateHiveContext(sc: SparkContext): HiveContext = {
    if (hc == null) {
      hc = new HiveContext(sc)
    }
    
    hc
  }
  
  def main(args: Array[String]): Unit = {
    if (args.length > 3 || args.length < 2) {
      System.err.println(
        "Usage: Backend port id [--service]\n" +
        "\n" +
        "  port:      port the gateway will listen to\n" +
        "  id:        arbitrary numeric identifier for this backend session\n" +
        "  --service: prevents closing the connection from closing the backend"
      )
      
      System.exit(-1)
    }
    
    port = args(0).toInt
    sessionId = args(1).toInt
    isService = args.length > 2 && args(2) == "--service"
    
    try {
      gatewayServerSocket = new ServerSocket(port, 1, InetAddress.getByName("localhost"))
      gatewayServerSocket.setSoTimeout(0)
    
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

    val buf = new Array[Byte](1024)
    
    val backend = new Backend()
    val backendPort: Int = backend.init()
          
    // wait for the end of stdin, then exit
    new Thread("wait for monitor to close") {
      setDaemon(true)
      override def run(): Unit = {
        try {
          val dos = new DataOutputStream(gatewaySocket.getOutputStream())
          dos.writeInt(sessionId)
          dos.writeInt(gatewaySocket.getLocalPort())
          dos.writeInt(backendPort)
          
          // wait for the end of socket, closed if R process die
          gatewaySocket.getInputStream().read(buf)          

          dos.close()
          gatewaySocket.close()
        } catch {
          case e: IOException =>
            logError("Backend failed with exception ", e)
            
          if (!isService) System.exit(1)
        } finally {
          backend.close()
          
          if (!isService) System.exit(0)
        }
      }
    }.start() 
    
    // wait for the end of stdin, then exit
    new Thread("run backend") {
      setDaemon(true)
      override def run(): Unit = {
        try {
          backend.run()
        }
        catch {
          case e: IOException =>
            logError("Backend failed with exception ", e)
            
          if (!isService) System.exit(1)
        }
      }
    }.start()
  }
}
