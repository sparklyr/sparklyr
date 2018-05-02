//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

/*
 * The Backend class is launched from Spark through spark-submit with the following
 * paramters: port, session and service.
 *
 *   port: Defined the port the gateway should listen to.
 *   sessionid: An identifier to track each session and reuse sessions if needed.
 *   service: A flag to keep this service running until the client forces it to
 *            shut down by calling "terminateBackend" through the invoke interface.
 *   remote: A flag to enable the gateway and backend to accept remote connections.
 *
 * On launch, the Backend will open the gateway socket on the port specified by
 * the shell parameter on launch.
 *
 * If the port is already in use, the Backend will attempt to use the existing
 * service running in this port as a sparklyr gateway and register itself. Therefore,
 * the gateway socket serves not only as an interface to connect to the current
 * instance, but also as bridge to other sparklyr backend instances running in this
 * machine. This mechanism is the replacement of the ports file which used to
 * communicate ports information back to the sparklyr client, in this model, one
 * and only one gateway runs and provides the mapping between sessionids and ports.
 *
 * While running, the Backend loops under a while(true) loop and blocks under the
 * gateway socket accept() method waiting for clients to connect. Once a client
 * connects, it launches a thread to process the client requests and blocks again.
 *
 * In the gateway socket, the thread listens for commands: GetPorts,
 * RegisterInstance or UnregisterInstance.
 *
 * GetPorts provides a mapping to the gateway/backend ports. In a single-client/
 * single-backend scenario, the sessionid from the current instance and the
 * requested instance will match, a backend gets created and the backend port
 * communicated back to the client. In a multiple-backend scenario, GetPort
 * will look at the sessionid mapping table and return a redirect port if needed,
 * this enables the system to run multiple backends all using the same gateway
 * port but still support redirection to the correct sessionid backend. Finally,
 * if the sessionis is not found, a delay is introduced in case an existing
 * backend is launching an about to register.
 *
 * RegiterInstance provides a way to map sessionids to ports to other instances
 * of sparklyr running in this machines. During launch, if the gateway port is
 * already in use, the instance being launched will use this api to communicate
 * to the main gateway the port in which this instance will listen to.
 */

class Backend() {
  import java.io.{DataInputStream, DataOutputStream}
  import java.io.{File, FileOutputStream, IOException}
  import java.net.{InetAddress, InetSocketAddress, ServerSocket, Socket}
  import java.util.concurrent.TimeUnit

  import org.apache.spark.SparkConf
  import org.apache.spark.SparkContext
  import org.apache.spark.sql.hive.HiveContext

  import scala.util.Try

  private[this] var isService: Boolean = false
  private[this] var isRemote: Boolean = false
  private[this] var isWorker: Boolean = false

  private[this] var hostContext: String = null

  private[this] var isRunning: Boolean = true
  private[this] var isRegistered: Boolean = false
  private[this] var gatewayPort: Int = 0

  private[this] var gatewayServerSocket: ServerSocket = null
  private[this] var port: Int = 0
  private[this] var sessionId: Int = 0
  private[this] var connectionTimeout: Int = 60

  private[this] var sc: SparkContext = null
  private[this] var hc: HiveContext = null

  private[this] var sessionsMap: Map[Int, Int] = Map()

  private[this] var inetAddress: InetAddress = InetAddress.getLoopbackAddress()

  private[this] var logger: Logger = new Logger("Session", 0);

  private[this] var oneConnection: Boolean = false;

  private[this] var defaultTracker: Option[JVMObjectTracker] = None

  def setTracker(tracker: JVMObjectTracker): Unit = {
    defaultTracker = Option(tracker)
  }

  object GatewayOperattions extends Enumeration {
    val GetPorts, RegisterInstance, UnregisterInstance = Value
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

  def getPort(): Int = {
    port
  }

  def setSparkContext(nsc: SparkContext): Unit = {
    sc = nsc
  }

  def portIsAvailable(port: Int, inetAddress: InetAddress) = {
    var ss: ServerSocket = null
    var available = false

    Try {
        ss = new ServerSocket(port, 1, inetAddress)
        available = true
    }

    if (ss != null) {
        Try {
            ss.close();
        }
    }

    available
  }

  def setType(isServiceParam: Boolean,
              isRemoteParam: Boolean,
              isWorkerParam: Boolean) = {
    isService = isServiceParam
    isRemote = isRemoteParam
    isWorker = isWorkerParam
  }

  def setHostContext(hostContextParam: String) = {
    hostContext = hostContextParam
  }

  def init(portParam: Int,
           sessionIdParam: Int,
           connectionTimeoutParam: Int): Unit = {

    port = portParam
    sessionId = sessionIdParam
    connectionTimeout = connectionTimeoutParam

    logger = new Logger("Session", sessionId)

    logger.log("is starting under " +
        InetAddress.getLoopbackAddress().getHostAddress +
        " port " + port)

    if (isRemote) {
      logger.log("is configuring for remote connections")

      val anyIpAddress = Array[Byte](0, 0, 0, 0)
      inetAddress = InetAddress.getByAddress(anyIpAddress)
    }

    try {
      if (isWorker)
      {
        gatewayServerSocket = new ServerSocket(0, 1, inetAddress)
        port = gatewayServerSocket.getLocalPort()
      }
      else if (portIsAvailable(port, inetAddress))
      {
        logger.log("found port " + port + " is available")
        logger = new Logger("Gateway", sessionId)

        gatewayServerSocket = new ServerSocket(port, 100, inetAddress)
      }
      else
      {
        logger.log("found port " + port + " is not available")
        logger = new Logger("Backend", sessionId)
        if (isWorker) logger = new Logger("Worker", sessionId)

        gatewayServerSocket = new ServerSocket(0, 1, inetAddress)
        gatewayPort = port
        port = gatewayServerSocket.getLocalPort()

        val success = register(gatewayPort, sessionId, port)
        if (!success) {
          logger.logError("failed to register on gateway port " + gatewayPort)
          if (!isService) System.exit(1)
        }

        isRegistered = true
      }

      gatewayServerSocket.setSoTimeout(0)
    } catch {
      case e: IOException =>
        logger.logError("is shutting down from init() with exception ", e)
        if (!isService) System.exit(1)
    }

    // Delay load workers to retrieve ports from backend
    if (!isWorker) run()

    if (!isService) System.exit(0)
  }

  def run(): Unit = {
    try {
      initMonitor()
      while(isRunning) {
        bind()
      }
    } catch {
      case e: java.net.SocketException =>
        logger.log("is shutting down with expected SocketException", e)
        if (!isService) System.exit(1)
      case e: IOException =>
        logger.logError("is shutting down from run() with exception ", e)
        if (!isService) System.exit(1)
    }
  }

  def initMonitor(): Unit = {
    new Thread("starting init monitor thread") {
      override def run(): Unit = {
        Thread.sleep(connectionTimeout * 1000)
        if (!oneConnection && !isService) {
          logger.log("is terminating backend since no client has connected after " + connectionTimeout + " seconds")
          System.exit(1)
        }
      }
    }.start()
  }

  def bind(): Unit = {
    logger.log("is waiting for sparklyr client to connect to port " + port)
    val gatewaySocket = gatewayServerSocket.accept()

    oneConnection = true

    logger.log("accepted connection")
    val buf = new Array[Byte](1024)

    // wait for the end of stdin, then exit
    new Thread("wait for monitor to close") {
      setDaemon(true)
      override def run(): Unit = {
        try {
          val dis = new DataInputStream(gatewaySocket.getInputStream())
          val commandId = dis.readInt()

          logger.log("received command " + commandId)

          GatewayOperattions(commandId) match {
            case GatewayOperattions.GetPorts => {
              val requestedSessionId = dis.readInt()
              val startupTimeout = dis.readInt()

              val dos = new DataOutputStream(gatewaySocket.getOutputStream())

              if (requestedSessionId == sessionId)
              {
                logger.log("found requested session matches current session")
                logger.log("is creating backend and allocating system resources")

                val tracker = if (defaultTracker.isDefined) defaultTracker.get else new JVMObjectTracker();
                val serializer = new Serializer(tracker);
                val backendChannel = new BackendChannel(logger, terminate, serializer, tracker)
                backendChannel.setHostContext(hostContext)

                val backendPort: Int = backendChannel.init(isRemote)

                logger.log("created the backend")

                try {
                  // wait for the end of stdin, then exit
                  new Thread("run backend") {
                    setDaemon(true)
                    override def run(): Unit = {
                      try {
                        dos.writeInt(sessionId)
                        dos.writeInt(gatewaySocket.getLocalPort())
                        dos.writeInt(backendPort)

                        backendChannel.run()
                      }
                      catch {
                        case e: IOException =>
                          logger.logError("failed with exception ", e)

                        if (!isService) System.exit(1)

                        terminate()
                      }
                    }
                  }.start()

                  logger.log("is waiting for r process to end")

                  // wait for the end of socket, closed if R process die
                  gatewaySocket.getInputStream().read(buf)
                }
                finally {
                  backendChannel.close()

                  if (!isService) {
                    logger.log("is terminating backend")

                    gatewayServerSocket.close()
                    System.exit(0)
                  }

                  // workers should always terminate but without exceptions
                  if (isWorker) {
                    logger.log("is terminating backend")
                    isRunning = false
                    gatewayServerSocket.close()
                  }
                }
              }
              else
              {
                logger.log("is searching for session " + requestedSessionId)

                var portForSession = sessionsMap.get(requestedSessionId)

                var sessionMapRetries: Int = startupTimeout * 10
                while (!portForSession.isDefined && sessionMapRetries > 0)
                {
                  portForSession = sessionsMap.get(requestedSessionId)

                  Thread.sleep(100)
                  sessionMapRetries = sessionMapRetries - 1
                }

                if (portForSession.isDefined)
                {
                  logger.log("found mapping for session " + requestedSessionId)

                  dos.writeInt(requestedSessionId)
                  dos.writeInt(portForSession.get)
                  dos.writeInt(0)
                }
                else
                {
                  logger.log("found no mapping for session " + requestedSessionId)

                  dos.writeInt(requestedSessionId)
                  dos.writeInt(0)
                  dos.writeInt(0)
                }
              }

              dos.close()
            }
            case GatewayOperattions.RegisterInstance => {
              val registerSessionId = dis.readInt()
              val registerGatewayPort = dis.readInt()

              logger.log("received session " + registerSessionId + " registration request")

              sessionsMap += (registerSessionId -> registerGatewayPort)

              val dos = new DataOutputStream(gatewaySocket.getOutputStream())
              dos.writeInt(0)
              dos.flush()
              dos.close()
            }
            case GatewayOperattions.UnregisterInstance => {
              val unregisterSessionId = dis.readInt()

              logger.log("received session " + unregisterSessionId + " unregistration request")

              if (sessionsMap.contains(unregisterSessionId)) {
                logger.log("found session " + unregisterSessionId + " during unregistration request")
                sessionsMap -= unregisterSessionId
              }

              val dos = new DataOutputStream(gatewaySocket.getOutputStream())
              dos.writeInt(0)
              dos.flush()
              dos.close()
            }
          }

          gatewaySocket.close()
        } catch {
          case e: IOException =>
            logger.logError("failed with exception ", e)

          if (!isService) System.exit(1)
        }
      }
    }.start()
  }

  def register(gatewayPort: Int, sessionId: Int, port: Int): Boolean = {
    logger.log("is registering session in gateway")

    val s = new Socket(InetAddress.getLoopbackAddress(), gatewayPort)

    val dos = new DataOutputStream(s.getOutputStream())
    dos.writeInt(GatewayOperattions.RegisterInstance.id)
    dos.writeInt(sessionId)
    dos.writeInt(port)

    logger.log("is waiting for registration in gateway")

    val dis = new DataInputStream(s.getInputStream())
    val status = dis.readInt()

    logger.log("finished registration in gateway with status " + status)

    s.close()
    status == 0
  }

  def terminate() = {
    if (isRegistered && !isWorker) {
      val success = unregister(gatewayPort, sessionId)
      if (!success) {
        logger.logError("failed to unregister on gateway port " + gatewayPort)
        if (!isService) System.exit(1)
      }
    }

    if (!isService || isWorker) {
      isRunning = false
    }
  }

  def unregister(gatewayPort: Int, sessionId: Int): Boolean = {
    try {
      logger.log("is unregistering session in gateway")

      val s = new Socket(InetAddress.getLoopbackAddress(), gatewayPort)

      val dos = new DataOutputStream(s.getOutputStream())
      dos.writeInt(GatewayOperattions.UnregisterInstance.id)
      dos.writeInt(sessionId)

      logger.log("is waiting for unregistration in gateway")

      val dis = new DataInputStream(s.getInputStream())
      val status = dis.readInt()

      logger.log("finished unregistration in gateway with status " + status)

      s.close()
      status == 0
    } catch {
      case e: Exception =>
        logger.log("failed to unregister from gateway: " + e.toString)
        false
    }
  }
}

object Backend {
  /* Leaving this entry for backward compatibility with databricks */
  def main(args: Array[String]): Unit = {
    Shell.main(args)
  }
}
