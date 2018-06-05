//
// This file was automatically generated using livy_sources_refresh()
// Changes to this file will be reverted.
//

import scala.collection.mutable.HashMap
import scala.language.existentials

class Invoke {
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

            if (parameterType == classOf[Seq[Any]] &&
                args(i) != null && args(i).getClass.isArray) {
              // The case that the parameter type is a Scala Seq and the argument
              // is a Java array is considered matching. The array will be converted
              // to a Seq later if this method is matched.
            } else if (parameterType == classOf[Char] && args(i) != null &&
                       args(i).isInstanceOf[String]) {
              // Pparameter type is Char and argument is String.
              // Check that the string has length 1.
              if (args(i).asInstanceOf[String].length != 1) argMatched = false
            } else if (parameterType == classOf[Short] && args(i) != null &&
                       args(i).isInstanceOf[Integer]) {
              // Parameter type is Short and argument is Integer.
            } else if (parameterType == classOf[Long] && args(i) != null &&
                       args(i).isInstanceOf[Integer]) {
              // Parameter type is Long and argument is Integer.
              // This is done for backwards compatibility.
            } else {
              var parameterWrapperType = parameterType

              // Convert native parameters to Object types as args is Array[Object] here
              if (parameterType.isPrimitive) {
                parameterWrapperType = parameterType match {
                  case java.lang.Integer.TYPE => classOf[java.lang.Integer]
                  case java.lang.Long.TYPE => classOf[java.lang.Double]
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
              if (parameterTypes(i) == classOf[Seq[Any]] &&
                  args(i) != null && args(i).getClass.isArray) {
                // Convert a Java array to scala Seq
                args(i) = args(i).asInstanceOf[Array[_]].toSeq
              } else if (parameterTypes(i) == classOf[Char] &&
                         args(i) != null && args(i).isInstanceOf[String]) {
                // Convert String to Char
                args(i) = new java.lang.Character(args(i).asInstanceOf[String](0))
              } else if (parameterTypes(i) == classOf[Short] &&
                         args(i) != null && args(i).isInstanceOf[Integer]) {
                // Convert Integer to Short
                val argInt = args(i).asInstanceOf[Integer]
                if (argInt > Short.MaxValue || argInt < Short.MinValue) {
                  throw new Exception("Unable to cast integer to Short: out of range.")
                }
                args(i) = new java.lang.Short(argInt.toShort)
              } else if (parameterTypes(i) == classOf[Long] &&
                         args(i) != null && args(i).isInstanceOf[Double]) {
                // Try to convert Double to Long
                val argDouble = args(i).asInstanceOf[Double]
                if (argDouble > Long.MaxValue || argDouble < Long.MinValue) {
                  throw new Exception("Unable to cast numeric to Long: out of range.")
                }
                args(i) = new java.lang.Long(argDouble.toLong)
              } else if (parameterTypes(i) == classOf[Long] &&
                          args(i) != null && args(i).isInstanceOf[Integer]) {
                args(i) = new java.lang.Long(args(i).asInstanceOf[Integer].toLong)
              }
            }

            return Some(index)
          }
        }
      }
      None
    }

  def invoke(
    cls: Class[_],
    objId: String,
    obj: Object,
    methodName: String,
    args: Array[Object],
    logger: Logger): Object = {

    val methods = cls.getMethods
    val selectedMethods = methods.filter(m => m.getName == methodName)
    if (selectedMethods.length > 0) {
      val index = findMatchedSignature(
        selectedMethods.map(_.getParameterTypes),
        args)

      if (index.isEmpty) {
        logger.logWarning(
          s"cannot find matching method ${cls}.$methodName. " +
          s"Candidates are:")
        selectedMethods.foreach { method =>
          logger.logWarning(s"$methodName(${method.getParameterTypes.mkString(",")})")
        }
        throw new Exception(s"No matched method found for $cls.$methodName")
      }

      return selectedMethods(index.get).invoke(obj, args : _*)
    } else if (methodName == "<init>") {
      // methodName should be "<init>" for constructor
      val ctors = cls.getConstructors
      val index = findMatchedSignature(
        ctors.map(_.getParameterTypes),
        args)

      if (index.isEmpty) {
        logger.logWarning(
          s"cannot find matching constructor for ${cls}. " +
          s"Candidates are:")
        ctors.foreach { ctor =>
          logger.logWarning(s"$cls(${ctor.getParameterTypes.mkString(",")})")
        }
        throw new Exception(s"No matched constructor found for $cls")
      }

      return ctors(index.get).newInstance(args : _*).asInstanceOf[Object]
    } else {
      val fields = cls.getFields
      val selectedField = fields.filter(m => m.getName == methodName)
      if (selectedField.length == 1) {
        return selectedField(0).get(obj)
      }
      else {
        throw new IllegalArgumentException("invalid method " + methodName + " for object " + objId + "/" + cls.getName + " fields " + fields.length + " selected " + selectedField.length)
      }
    }
  }
}
