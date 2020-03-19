package sparklyr

import java.util.NoSuchElementException
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.Lock
import java.util.concurrent.locks.ReentrantReadWriteLock
import scala.language.existentials

class JVMObjectTracker {

  val objMap = new ConcurrentHashMap[String, Object]

  val objCounter = new AtomicInteger()
  var writerCnt: Int = 0
  val rwLock = new ReentrantReadWriteLock()

  def getObject(id: String): Object = {
    rwLock.readLock().lock()
    try {
      Option(objMap.get(id)) match {
        case Some(obj) => obj
        case None => throw new NoSuchElementException()
      }
    } finally {
      rwLock.readLock().unlock()
    }
  }

  def get(id: String): Option[Object] = {
    rwLock.readLock().lock()
    try {
      Option(objMap.get(id))
    } finally {
      rwLock.readLock().unlock()
    }
  }

  def put(obj: Object): String = {
    acquireWriteLock()
    try {
      val objId = objCounter.getAndAdd(1).toString
      objMap.put(objId, obj)
      objId
    } finally {
      releaseWriteLock()
    }
  }

  def remove(id: String): Option[Object] = {
    acquireWriteLock()
    try {
      Option(objMap.remove(id))
    } finally {
      releaseWriteLock()
    }
  }

  private[this] def acquireWriteLock(): Unit = this.synchronized {
    writerCnt += 1
    if (writerCnt == 1) {
      // first writer should lock objMap for write for itself and for any
      // subsequent writers
      rwLock.writeLock().lock()
    }
  }

  private[this] def releaseWriteLock(): Unit = this.synchronized {
    writerCnt -= 1
    if (writerCnt == 0) {
      // last writer should release objMap write lock for subsequent readers
      rwLock.writeLock().unlock()
    }
  }
}
