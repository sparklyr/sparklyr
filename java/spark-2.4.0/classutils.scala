package sparklyr

class ClassUtils {
  def getClassLoader: ClassLoader = {
    Option(Thread.currentThread().getContextClassLoader).getOrElse(getClass.getClassLoader)
  }

  def classForName(className: String): Class[_] = {
    Class.forName(className, true, getClassLoader)
  }

  def classExists(className: String): Boolean = {
    try {
      classForName(className)
      true
    } catch {
      case e: ClassNotFoundException => false
    }
  }
}
