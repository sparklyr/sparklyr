//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

class StreamHandler(serializer: Serializer, tracker: JVMObjectTracker) {

  import java.io._

  import scala.collection.mutable.HashMap
  import scala.language.existentials

  import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
  import io.netty.channel.ChannelHandler.Sharable

  val invoke = new Invoke()

  def classExists(name: String): Boolean = {
    scala.util.Try(Class.forName(name)).isSuccess
  }

  def read(
    msg: Array[Byte],
    classMap: Map[String, Object],
    logger: Logger,
    hostContext: String): Array[Byte] = {

    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    val objId = serializer.readString(dis)
    val isStatic = serializer.readBoolean(dis)
    val methodName = serializer.readString(dis)
    val numArgs = serializer.readInt(dis)

    if (objId == "Handler") {
      methodName match {
        case "echo" =>
          val args = readArgs(numArgs, dis)
          if (numArgs != 1) throw new IllegalArgumentException("echo should take a single argument")

          serializer.writeInt(dos, 0)
          serializer.writeObject(dos, args(0))
        case "rm" =>
          try {
            val t = serializer.readObjectType(dis)
            if (t != 'c') throw new IllegalArgumentException("object removal expects a string")
            val objToRemove = serializer.readString(dis)
            tracker.remove(objToRemove)
            serializer.writeInt(dos, 0)
            serializer.writeObject(dos, null)
          } catch {
            case e: Exception =>
              logger.logError(s"failed to remove $objId", e)
              serializer.writeInt(dos, -1)
              serializer.writeString(dos, s"Removing $objId failed: ${e.getMessage}")
          }
        case "getHostContext" =>
          serializer.writeInt(dos, 0)
          serializer.writeObject(dos, hostContext.asInstanceOf[AnyRef])
        case _ =>
          dos.writeInt(-1)
          serializer.writeString(dos, s"Error: unknown method $methodName")
      }
    } else {
      handleMethodCall(isStatic, objId, methodName, numArgs, dis, dos, classMap, logger)
    }

    bos.toByteArray
  }

  /**
   * Return a nice string representation of the exception. It will call "printStackTrace" to
   * recursively generate the stack trace including the exception and its causes.
   */
  def exceptionString(e: Throwable): String = {
    if (e == null) {
      "No exception information provided."
    } else {
      // Use e.printStackTrace here because e.getStackTrace doesn't include the cause
      val stringWriter = new StringWriter()
      e.printStackTrace(new PrintWriter(stringWriter))
      stringWriter.toString
    }
  }

  def handleMethodCall(
    isStatic: Boolean,
    objId: String,
    methodName: String,
    numArgs: Int,
    dis: DataInputStream,
    dos: DataOutputStream,
    classMap: Map[String, Object],
    logger: Logger): Unit = {
      var obj: Object = null
      try {
        val cls = if (isStatic) {
          if (classMap != null && classMap.contains(objId)) {
            obj = classMap(objId)
            classMap(objId).getClass.asInstanceOf[Class[_]]
          }
          else if (!classExists(objId) && classExists(objId + ".package$")) {
            val pkgCls = Class.forName(objId + ".package$")

            val mdlField = pkgCls.getField("MODULE$")
            obj = mdlField.get(null)
            obj.getClass
          }
          else {
            Class.forName(objId)
          }
        } else {
          tracker.get(objId) match {
            case None => throw new IllegalArgumentException("Object not found " + objId)
            case Some(o) =>
              obj = o
              o.getClass
          }
        }

        val args = readArgs(numArgs, dis)
        val res = invoke.invoke(cls, objId, obj, methodName, args, logger)

        serializer.writeInt(dos, 0)
        serializer.writeObject(dos, res.asInstanceOf[AnyRef])
      } catch {
        case e: Exception =>
          val cause = exceptionString(
            if (e.getCause == null) e else e.getCause
          )
          logger.logError(s"failed calling $methodName on $objId: " + cause)
          serializer.writeInt(dos, -1)
          serializer.writeString(dos, cause)
        case e: NoClassDefFoundError =>
          logger.logError(s"failed calling $methodName on $objId with no class found error")
          serializer.writeInt(dos, -1)
          serializer.writeString(dos, exceptionString(
            if (e.getCause == null) e else e.getCause
          ))
      }
    }

  // Read a number of arguments from the data input stream
  def readArgs(numArgs: Int, dis: DataInputStream): Array[java.lang.Object] = {
    (0 until numArgs).map { _ =>
      serializer.readObject(dis)
    }.toArray
  }
}
