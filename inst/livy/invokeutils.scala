//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

import scala.collection.mutable.HashMap
import scala.language.existentials

import Logging._

object InvokeUtils {
  // Find a matching method signature in an array of signatures of constructors
  // or methods of the same name according to the passed arguments. Arguments
  // may be converted in order to match a signature.
  //
  // Note that in Java reflection, constructors and normal methods are of different
  // classes, and share no parent class that provides methods for reflection uses.
  // There is no unified way to handle them in this function. So an array of signatures
  // is passed in instead of an array of candidate constructors or methods.
  //
  // Returns an Option[Int] which is the index of the matched signature in the array.
  def findMatchedSignature(
    parameterTypesOfMethods: Array[Array[Class[_]]],
    args: Array[Object]): Option[Int] = {
      val numArgs = args.length

      for (index <- 0 until parameterTypesOfMethods.length) {
        val parameterTypes = parameterTypesOfMethods(index)

        if (parameterTypes.length == numArgs) {
          var argMatched = true
          var i = 0
          while (i < numArgs && argMatched) {
            val parameterType = parameterTypes(i)

            if (parameterType == classOf[Seq[Any]] && args(i).getClass.isArray) {
              // The case that the parameter type is a Scala Seq and the argument
              // is a Java array is considered matching. The array will be converted
              // to a Seq later if this method is matched.
            } else {
              var parameterWrapperType = parameterType

              // Convert native parameters to Object types as args is Array[Object] here
              if (parameterType.isPrimitive) {
                parameterWrapperType = parameterType match {
                  case java.lang.Integer.TYPE => classOf[java.lang.Integer]
                  case java.lang.Long.TYPE => classOf[java.lang.Integer]
                  case java.lang.Double.TYPE => classOf[java.lang.Double]
                  case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
                  case _ => parameterType
                }
              }
              if ((parameterType.isPrimitive || args(i) != null) &&
                  !parameterWrapperType.isInstance(args(i))) {
                argMatched = false
              }
            }

            i = i + 1
          }

          if (argMatched) {
            // Convert args if needed
            val parameterTypes = parameterTypesOfMethods(index)

            (0 until numArgs).map { i =>
              if (parameterTypes(i) == classOf[Seq[Any]] && args(i).getClass.isArray) {
                // Convert a Java array to scala Seq
                args(i) = args(i).asInstanceOf[Array[_]].toSeq
              }
            }

            return Some(index)
          }
        }
      }
      None
    }

  def invoke(cls: Class[_], objId: String, obj: Object, methodName: String, args: Array[Object]): Object = {
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

      return selectedMethods(index.get).invoke(obj, args : _*)
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

      return ctors(index.get).newInstance(args : _*).asInstanceOf[Object]
    } else {
      throw new IllegalArgumentException("invalid method " + methodName + " for object " + objId)
    }
  }

  def invokeEx(obj: Object, method: String, args: Array[Object]): (String, Object) = {
    val cls = obj.getClass
    val classId = cls.getName

    val res = invoke(cls, classId, obj, method, args)

    (if (res != null) res.getClass.getName else null, res)
  }

  def invokeStaticEx(classId: String, method: String, args: Array[Object]): (String, Object) = {
    val cls = Class.forName(classId)
    val res = invoke(cls, classId, null, method, args)

    (res.getClass.getName, res)
  }

  def invokeNewEx(classId: String, args: Array[Object]): (String, Object) = {
    val cls = Class.forName(classId)
    val res = invoke(cls, classId, null, "<init>", args)

    (res.getClass.getName, res)
  }

  def invokeElemEx(obj: Object, index: Int): (String, Object) = {
    val res = obj.asInstanceOf[Array[Object]](index)

    (res.getClass.getName, res)
  }

  def invokeLengthEx(obj: Object) : (String, Object) = {
    val res: java.lang.Integer = obj.asInstanceOf[Array[Object]].length

    (res.getClass.getName, res)
  }
}
