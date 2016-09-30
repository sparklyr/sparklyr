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

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext

import scala.util.Try

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
  
  private[this] var sc: SparkContext = null
  private[this] var hc: HiveContext = null
  
  private[this] var sessionsMap:Map[Int, Int] = Map()
  
  object GatewayOperattions extends Enumeration {
    val GetPorts, RegisterInstance = Value
  }
  
  def getOrCreateHiveContext(sc: SparkContext): HiveContext = {
    if (hc == null) {
      hc = new HiveContext(sc)
    }
    
    hc
  }
  
  def getSparkContext(): SparkContext = {
    sc
  }
  
  def setSparkContext(nsc: SparkContext): Unit = {
    sc = nsc
  }
  
  def portIsAvailable(port: Int) = {
    var ss: ServerSocket = null
    var available = false
    
    Try {
        ss = new ServerSocket(port, 1, InetAddress.getByName("localhost"))
        available = true
    }
    
    if (ss != null) {
        Try {
            ss.close();
        }
    }

    available
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
    
    log("sparklyr session " + sessionId + " ready on port " + port)
    
    try {
      if (portIsAvailable(port))
      {
        gatewayServerSocket = new ServerSocket(port, 1, InetAddress.getByName("localhost"))
      }
      else
      {
        gatewayServerSocket = new ServerSocket(0, 1, InetAddress.getByName("localhost"))
        val gatewayPort = port
        port = gatewayServerSocket.getLocalPort()
        
        val success = register(gatewayPort, sessionId, port)
        if (!success) {
          logError("Failed to register sparklyr session (" + sessionId + ") to gateway port (" + gatewayPort + ")")
          System.exit(1)
        }
      }
      
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
          val dis = new DataInputStream(gatewaySocket.getInputStream())
          val commandId = dis.readInt()
          
          log("sparklyr gateway received command identifier (" + commandId + ")")
          
          GatewayOperattions(commandId) match {
            case GatewayOperattions.GetPorts => {
              val requestedSessionId = dis.readInt()
              
              val dos = new DataOutputStream(gatewaySocket.getOutputStream())
              
              if (requestedSessionId == sessionId || requestedSessionId == 0)
              {
                log("sparklyr gateway found current session id (" + sessionId + ")")
                
                dos.writeInt(sessionId)
                dos.writeInt(gatewaySocket.getLocalPort())
                dos.writeInt(backendPort)
                
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
                
                // even if this backend instance did not start as a service
                // it will be considered one since other instances might use
                // this instance to map ports while this instance in running
                val wasService = isService
                isService = true
                
                // wait for the end of socket, closed if R process die
                gatewaySocket.getInputStream().read(buf)
                
                isService = wasService
              }
              else
              {
                log("sparklyr gateway searching for session id (" + requestedSessionId + ")")
                
                var portForSession = sessionsMap.get(requestedSessionId)
                
                var sessionMapRetries: Int = 100
                while (!portForSession.isDefined && sessionMapRetries > 0)
                {
                  portForSession = sessionsMap.get(requestedSessionId)
                  
                  Thread.sleep(10)
                  sessionMapRetries = sessionMapRetries - 1
                }
                
                if (portForSession.isDefined)
                {
                  log("sparklyr gateway found mapping for session id (" + requestedSessionId + ")")
                  
                  dos.writeInt(requestedSessionId)
                  dos.writeInt(portForSession.get)
                  dos.writeInt(0)
                }
                else
                {
                  log("sparklyr gateway found no mapping for session id (" + requestedSessionId + ")")
              
                  dos.writeInt(requestedSessionId)
                  dos.writeInt(0)
                  dos.writeInt(0)
                  
                  if (!isService) System.exit(1)
                }
              }
              
              dos.close()
            }
            case GatewayOperattions.RegisterInstance => {
              val registerSessionId = dis.readInt()
              val registerGatewayPort = dis.readInt()
              
              log("sparklyr gateway registering session id (" + registerSessionId + ") for port (" + registerGatewayPort + ")")
              
              val dos = new DataOutputStream(gatewaySocket.getOutputStream())
              dos.writeInt(0)
              dos.flush()
              dos.close()
              
              sessionsMap += (registerSessionId -> registerGatewayPort)
            }
          }
          
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
  }
  
  def register(gatewayPort: Int, sessionId: Int, port: Int): Boolean = {
    log("sparklyr registering session (" + sessionId + ") into gateway port (" + gatewayPort +  ")")
    
    val s = new Socket(InetAddress.getByName("localhost"), gatewayPort)
    
    val dos = new DataOutputStream(s.getOutputStream())
    dos.writeInt(GatewayOperattions.RegisterInstance.id)
    dos.writeInt(sessionId)
    dos.writeInt(port)
    
    log("sparklyr waiting for registration of (" + sessionId + ") into gateway port (" + gatewayPort +  ")")
    
    val dis = new DataInputStream(s.getInputStream())
    val status = dis.readInt()
    
    log("sparklyr fnished registration of session (" + sessionId + ") into gateway port (" + gatewayPort +  ") with status (" + status + ")")
    
    s.close()
    status == 0
  }
}
