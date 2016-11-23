//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

import scala.collection.mutable.HashMap
import scala.language.existentials

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
