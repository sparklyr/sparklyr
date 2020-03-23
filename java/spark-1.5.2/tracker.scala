package sparklyr

import java.util.NoSuchElementException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import scala.language.existentials

class JVMObjectTracker {

  val objMap = new ConcurrentHashMap[String, Object]

  val objCounter = new AtomicInteger()

  def getObject(id: String): Object = {
    Option(objMap.get(id)) match {
      case Some(obj) => obj
      case None => throw new NoSuchElementException()
    }
  }

  def get(id: String): Option[Object] = {
    Option(objMap.get(id))
  }

  def put(obj: Object): String = {
    val objId = objCounter.getAndAdd(1).toString
    objMap.put(objId, obj)
    objId
  }

  def remove(id: String): Option[Object] = {
    Option(objMap.remove(id))
  }
}
