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

spark_api_start <- function(master, appName) {
  con <- start_shell()

  con$sc <- spark_api_create_context(con, master, appName)
  if (identical(con$sc, NULL)) {
    stop("Failed to create Spark context")
  }

  con$sql <- spark_api_create_sql_context(con)
  if (identical(con$sc, NULL)) {
    stop("Failed to create SQL context")
  }

  con
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
  result <- spark_api(
    con,

    FALSE,
    con$sql$id,
    "sql",

    sql
  )

  spark_api(
    con,

    FALSE,
    result$id,
    "printSchema"
  )

  result
}

spark_api_schema <- function(con, sqlResult) {
  spark_api(
    con,

    FALSE,
    sqlResult$id,
    "schema"
  )
}

spark_api_object_method <- function(con, object, property) {
  spark_api(
    con,

    FALSE,
    object$id,
    property
  )
}

spark_api_field <- function(con, field) {
  name <- spark_api_object_method(con, field, "name")
  dataType <- spark_api_object_method(con, field, "dataType")
  longType <- spark_api_object_method(con, dataType, "toString")
  shortType <- spark_api_object_method(con, dataType, "simpleString")

  list(
    name = name,
    longType = longType,
    shortType = shortType
  )
}

spark_api_schema_fields <- function(con, schemaResult) {
  lapply(
    spark_api(
      con,

      FALSE,
      schemaResult$id,
      "fields"
    ),
    function (field) {
      spark_api_field(con, field)
    }
  )
}

# See https://github.com/apache/spark/tree/branch-1.6/sql/catalyst/src/main/scala/org/apache/spark/sql/types
spark_api_data_frame_default_type <- function(field) {
  switch(field$shortType,
         tinyint = integer(),
         bigint = integer(),
         smallint = integer(),
         string = character(),
         double = double(),
         int = integer(),
         character())
}

# Retrives a typed column for the given dataframe
spark_api_data_frame_columns_typed <- function(col, stringData, fields, rows) {
  shortType <- fields[[col]]$shortType

  unlist(lapply(seq_len(rows), function(row) {
    raw <- stringData[[(col - 1) * rows + row]]

    switch(shortType,
           tinyint = as.integer(raw),
           bigint = as.integer(raw),
           smallint = as.integer(raw),
           string = raw,
           double = as.double(raw),
           int = as.integer(raw),
           raw)
  }))
}

spark_api_data_frame <- function(con, sqlResult) {
  schema <- spark_api_schema(con, sqlResult)
  fields <- spark_api_schema_fields(con, schema)

  df <- spark_api(
    con,

    TRUE,
    "org.apache.spark.sql.api.r.SQLUtils",
    "dfToCols",

    sqlResult
  )

  dfNames <- lapply(fields, function(x) x$name)
  rows <- length(df[[1]])

  # If this is a resultset with no rows...
  if (rows == 0) {
    dfEmpty <- lapply(fields, function(field) {
      spark_api_data_frame_default_type(field)
    })
    names(dfEmpty) <- dfNames
    df <- data.frame(dfEmpty, stringsAsFactors=FALSE)
  }
  else {
    stringData <- unlist(df)
    columns <- lapply(seq_along(fields),
                      spark_api_data_frame_columns_typed,
                      stringData,
                      fields,
                      rows)
    names(columns) <- dfNames

    df <- data.frame(columns, stringsAsFactors=FALSE)
  }

  df
}

spark_api_copy_data <- function(con, df, name) {
  tempfile <- tempfile(fileext = ".csv")
  write.csv(df, tempfile, row.names=FALSE)

  columns <- lapply(df, typeof)
  df <- spark_read_csv(con, tempfile, columns)

  spark_register_temp_table(con, df, name)
}

spark_register_temp_table <- function(con, table, name) {
  spark_api(con, FALSE, table$id, "registerTempTable", name)
}

spark_drop_temp_table <- function(con, name) {
  spark_api(con, FALSE, con$sql$id, "dropTempTable", name)
}
