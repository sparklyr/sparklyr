#
# Client into the Spark API over sockets
#
#   See: https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/api/r/RBackend.scala
#
# Particular methods are defined on their specific clases, for instance, for "createSparkContext" see:
#
#   See: https://github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/api/r/RRDD.scala
#
spark_api <- function (backend, isStatic, objName, methodName, ...) 
{
  rc <- rawConnection(raw(), "r+")
  writeBoolean(rc, isStatic)
  writeString(rc, objName)
  writeString(rc, methodName)
  
  args <- list(...)
  writeInt(rc, length(args))
  writeArgs(rc, args)
  bytes <- rawConnectionValue(rc)
  close(rc)
  
  rc <- rawConnection(raw(0), "r+")
  writeInt(rc, length(bytes))
  writeBin(bytes, rc)
  con <- rawConnectionValue(rc)
  close(rc)
  
  writeBin(con, backend)
  returnStatus <- readInt(backend)
  
  if (returnStatus != 0) {
    stop(readString(backend))
  }
  
  readObject(backend)
}