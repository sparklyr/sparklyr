#
# Client into the Spark API over sockets
#
#   See: https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RBackend.scala
#
# Particular methods are defined on their specific clases, for instance, for "createSparkContext" see:
#
#   See: https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RRDD.scala
#
spark_api <- function (sparkCon, isStatic, objName, methodName, ...)
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

  writeBin(con, sparkCon$backend)
  returnStatus <- readInt(sparkCon$backend)

  if (returnStatus != 0) {
    stop(readString(sparkCon$backend))
  }

  readObject(sparkCon$backend)
}

# API into https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RRDD.scala
#
# def createSparkContext(
#   master: String,                               // The Spark master URL.
#   appName: String,                              // Application name to register with cluster manager
#   sparkHome: String,                            // Spark Home directory
#   jars: Array[String],                          // Character string vector of jar files to pass to the worker nodes.
#   sparkEnvirMap: JMap[Object, Object],          // Named list of environment variables to set on worker nodes.
#   sparkExecutorEnvMap: JMap[Object, Object])    // Named list of environment variables to be used when launching executors.
#   : JavaSparkContext
#
spark_api_create_context <- function(con, master, appName) {
  sparkHome <- as.character(normalizePath(Sys.getenv("SPARK_HOME"), mustWork = FALSE))

  spark_api(
    con,

    TRUE,
    "org.apache.spark.api.r.RRDD",
    "createSparkContext",

    master,
    appName,
    sparkHome,
    list(),
    new.env(),
    new.env()
  )
}

#
# API into https://github.com/apache/spark/blob/branch-1.6/sql/core/src/main/scala/org/apache/spark/sql/api/r/SQLUtils.scala
#
# def createSQLContext(jsc: JavaSparkContext): SQLContext
#
spark_api_create_sql_context <- function(con) {
  spark_api(
    con,

    TRUE,
    "org.apache.spark.sql.api.r.SQLUtils",
    "createSQLContext",

    con$sc
  )
}

spark_api_sql <- function(con, sql) {
  spark_api(
    con,

    FALSE,
    con$sql$id,
    "sql",

    sql
  )
}
