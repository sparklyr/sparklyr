package sparklyr

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}

import scala.collection.mutable.HashMap
import scala.language.existentials

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.channel.ChannelHandler.Sharable

import sparklyr.Logging._
import sparklyr.Serializer._

@Sharable
class Handler(server: Backend)
extends SimpleChannelInboundHandler[Array[Byte]] {

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    // First bit is isStatic
    val isStatic = readBoolean(dis)
    val objId = readString(dis)
    val methodName = readString(dis)
    val numArgs = readInt(dis)

    if (objId == "Handler") {
      methodName match {
        // This function is for test-purpose only
        case "echo" =>
          val args = readArgs(numArgs, dis)
        assert(numArgs == 1)

        writeInt(dos, 0)
        writeObject(dos, args(0))
        case "stopBackend" =>
          writeInt(dos, 0)
          writeType(dos, "void")
          server.close()
        case "terminateBackend" =>
          writeInt(dos, 0)
          writeType(dos, "void")
          server.close()

          System.exit(0)
        case "rm" =>
          try {
            val t = readObjectType(dis)
            assert(t == 'c')
            val objToRemove = readString(dis)
            JVMObjectTracker.remove(objToRemove)
            writeInt(dos, 0)
            writeObject(dos, null)
          } catch {
            case e: Exception =>
              logError(s"Removing $objId failed", e)
            writeInt(dos, -1)
            writeString(dos, s"Removing $objId failed: ${e.getMessage}")
          }
        case _ =>
          dos.writeInt(-1)
        writeString(dos, s"Error: unknown method $methodName")
      }
    } else {
      handleMethodCall(isStatic, objId, methodName, numArgs, dis, dos)
    }

    val reply = bos.toByteArray
    ctx.write(reply)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // Close the connection when an exception is raised.
    cause.printStackTrace()
    ctx.close()
  }

  def handleMethodCall(
    isStatic: Boolean,
    objId: String,
    methodName: String,
    numArgs: Int,
    dis: DataInputStream,
    dos: DataOutputStream): Unit = {
      var obj: Object = null
      try {
        val cls = if (isStatic) {
          Class.forName(objId)
        } else {
          JVMObjectTracker.get(objId) match {
            case None => throw new IllegalArgumentException("Object not found " + objId)
            case Some(o) =>
              obj = o
            o.getClass
          }
        }

        val args = readArgs(numArgs, dis)

        val methods = cls.getMethods
        val selectedMethods = methods.filter(m => m.getName == methodName)
        if (selectedMethods.length > 0) {
          val index = InvokeUtils.findMatchedSignature(
            selectedMethods.map(_.getParameterTypes),
            args)

          if (index.isEmpty) {
            logWarning(s"cannot find matching method ${cls}.$methodName. "
                       + s"Candidates are:")
            selectedMethods.foreach { method =>
              logWarning(s"$methodName(${method.getParameterTypes.mkString(",")})")
            }
            throw new Exception(s"No matched method found for $cls.$methodName")
          }

          val ret = selectedMethods(index.get).invoke(obj, args : _*)

          // Write status bit
          writeInt(dos, 0)
          writeObject(dos, ret.asInstanceOf[AnyRef])
        } else if (methodName == "<init>") {
          // methodName should be "<init>" for constructor
          val ctors = cls.getConstructors
          val index = InvokeUtils.findMatchedSignature(
            ctors.map(_.getParameterTypes),
            args)

          if (index.isEmpty) {
            logWarning(s"cannot find matching constructor for ${cls}. "
                       + s"Candidates are:")
            ctors.foreach { ctor =>
              logWarning(s"$cls(${ctor.getParameterTypes.mkString(",")})")
            }
            throw new Exception(s"No matched constructor found for $cls")
          }

          val obj = ctors(index.get).newInstance(args : _*)

          writeInt(dos, 0)
          writeObject(dos, obj.asInstanceOf[AnyRef])
        } else {
          throw new IllegalArgumentException("invalid method " + methodName + " for object " + objId)
        }
      } catch {
        case e: Exception =>
          logError(s"$methodName on $objId failed")
        writeInt(dos, -1)
        // Writing the error message of the cause for the exception. This will be returned
        // to user in the R process.
        writeString(dos, Utils.exceptionString(e.getCause))
      }
    }

  // Read a number of arguments from the data input stream
  def readArgs(numArgs: Int, dis: DataInputStream): Array[java.lang.Object] = {
    (0 until numArgs).map { _ =>
      readObject(dis)
    }.toArray
  }
}

/**
 * Helper singleton that tracks JVM objects returned to R.
 * This is useful for referencing these objects in RPC calls.
*/
object JVMObjectTracker {

  private[this] val objMap = new HashMap[String, Object]

  private[this] var objCounter: Int = 0

  def getObject(id: String): Object = {
    objMap(id)
  }

  def get(id: String): Option[Object] = {
    objMap.get(id)
  }

  def put(obj: Object): String = {
    val objId = objCounter.toString
    objCounter = objCounter + 1
    objMap.put(objId, obj)
    objId
  }

  def remove(id: String): Option[Object] = {
    objMap.remove(id)
  }
}
