#
# Client into the Spark API over sockets
#
#   See: https://github.com/apache/spark/blob/branch-1.6/core/src/main/scala/org/apache/spark/api/r/RBackend.scala
#
#
# API into https://github.com/apache/spark/blob/branch-1.6/sql/core/src/main/scala/org/apache/spark/sql/api/r/SQLUtils.scala
#
# def createSQLContext(jsc: JavaSparkContext): SQLContext
#
spark_api_create_sql_context <- function(scon) {
  ctx <- spark_context(scon)
  jsc <- invoke_static(
    scon,
    "org.apache.spark.api.java.JavaSparkContext",
    "fromSparkContext",
    ctx
  )

  invoke_static(
    scon,

    "org.apache.spark.sql.api.r.SQLUtils",
    "createSQLContext",

    jsc
  )
}

is_spark_v2 <- function(scon) {
  sparkVersion <- spark_connection_version(scon)
  if (!is.null(sparkVersion)) {
    version <- sub("-preview", "", sparkVersion)
    compared <- utils::compareVersion(version, "2.0.0")
    compared != -1
  } else {
    FALSE
  }
}

spark_api_create_hive_context <- function(scon) {

  if (is_spark_v2(scon))
    spark_api_create_hive_context_v2(scon)
  else
    spark_api_create_hive_context_v1(scon)
}

spark_api_create_hive_context_v2 <- function(scon) {

  # SparkSession.builder().enableHiveSupport()
  builder <- invoke_static(
    scon,
    "org.apache.spark.sql.SparkSession",
    "builder"
  )

  builder <- invoke(
    builder,
    "enableHiveSupport"
  )

  session <- invoke(
    builder,
    "getOrCreate"
  )

  conf <- invoke(session, "conf")

  params <- spark_config_params(scon$config, spark_connection_is_local(scon), "spark.session.")
  lapply(names(params), function(paramName) {
    configValue <- params[[paramName]]
    if (is.logical(configValue)) {
      configValue <- if (configValue) "true" else "false"
    }
    else {
      as.character(configValue)
    }

    invoke(
      conf,
      "set",
      paramName,
      configValue
    )
  })

  session

}

spark_api_create_hive_context_v1 <- function(scon) {
  invoke_new(
    scon,

    "org.apache.spark.sql.hive.HiveContext",

    spark_context(scon)
  )
}

spark_api_create <- function(scon) {
  list(
    scon = scon
  )
}

spark_sql_or_hive <- function(api) {
  sconInst <- spark_connection_get_inst(api$scon)

  if (!identical(sconInst$hive, NULL))
    sconInst$hive
  else
    sconInst$sql
}

spark_api_sql <- function(api, sql) {
  result <- invoke(
    spark_sql_or_hive(api),
    "sql",
    sql
  )

  result
}

spark_api_schema <- function(api, sqlResult) {
  invoke(
    sqlResult,
    "schema"
  )
}

spark_api_object_method <- function(api, object, property) {
  invoke(
    object,
    property
  )
}

spark_api_field <- function(api, field) {
  name <- spark_api_object_method(api, field, "name")
  dataType <- spark_api_object_method(api, field, "dataType")
  longType <- spark_api_object_method(api, dataType, "toString")
  shortType <- spark_api_object_method(api, dataType, "simpleString")

  list(
    name = name,
    longType = longType,
    shortType = shortType
  )
}

spark_api_schema_fields <- function(api, schemaResult) {
  lapply(
    invoke(
      schemaResult,
      "fields"
    ),
    function (field) {
      spark_api_field(api, field)
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
           bigint = as.numeric(raw),
           smallint = as.integer(raw),
           string = raw,
           double = as.double(raw),
           int = as.integer(raw),
           raw)
  }))
}

spark_api_data_frame <- function(api, sqlResult) {
  schema <- spark_api_schema(api, sqlResult)
  fields <- spark_api_schema_fields(api, schema)

  df <- invoke_static(
    api$scon,

    "org.apache.spark.sql.api.r.SQLUtils",
    "dfToCols",

    sqlResult
  )

  dfNames <- lapply(fields, function(x) x$name)
  rows <- if (length(df) > 0) length(df[[1]]) else 0

  # If this is a resultset with no rows...
  if (rows == 0) {
    # Remove invalid fields that have zero length
    fields <- Filter(function(e) nchar(e$name) > 0, fields)
    dfNames <- Filter(function(e) nchar(e) > 0, dfNames)
    
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

spark_api_build_types <- function(api, columns) {
  names <- names(columns)
  fields <- lapply(names, function(name) {
    invoke_static(api$scon, "org.apache.spark.sql.api.r.SQLUtils", "createStructField", name, columns[[name]], TRUE)
  })

  invoke_static(api$scon, "org.apache.spark.sql.api.r.SQLUtils", "createStructType", fields)
}

spark_api_copy_data <- function(api, df, name, repartition, local_file = TRUE) {
  if (!is.numeric(repartition)) {
    stop("The repartition parameter must be an integer")
  }

  # Escaping issues that used to work were broken in Spark 2.0.0-preview, fix:
  names(df) <- gsub("[^a-zA-Z0-9]", "_", names(df))

  columns <- lapply(df, function(e) {
    if (is.factor(e))
      "character"
    else
      typeof(e)
  })

  if (local_file) {
    tempfile <- tempfile(fileext = ".csv")
    write.csv(df, tempfile, row.names = FALSE, na = "")
    df <- spark_api_read_csv(api, tempfile, csvOptions = list(
      header = "true"
    ), columns = columns)

    if (repartition > 0) {
      df <- invoke(df, "repartition", as.integer(repartition))
    }
  } else {
    structType <- spark_api_build_types(api, columns)
    
    # Map date and time columns as standard doubles
    df <- as.data.frame(lapply(df, function(e) {
      if (is.time(e) || is.date(e))
        sapply(e, function(t) {
          class(t) <- NULL
          t
        })
      else
        e
    }))

    rows <- lapply(seq_len(NROW(df)), function(e) as.list(df[e,]))

    rdd <- invoke_static(
      api$scon,
      "utils",
      "createDataFrame",
      spark_context(api$scon),
      rows,
      as.integer(if (repartition <= 0) 1 else repartition)
    )

    df <- invoke(spark_sql_or_hive(api), "createDataFrame", rdd, structType)
  }

  spark_register_temp_table(df, name)
}

spark_register_temp_table <- function(table, name) {
  invoke(table, "registerTempTable", name)
}

spark_drop_temp_table <- function(api, name) {
  context <- spark_sql_or_hive(api)
  if (is_spark_v2(api$scon)) {
    context <- invoke(context, "wrapped")
  }
  
  invoke(context, "dropTempTable", name)
}

spark_print_schema <- function(api, tableName) {
  result <- spark_api_sql(
    api,
    paste("SELECT * FROM", tableName, "LIMIT 1")
  )

  invoke(
    result,
    "printSchema"
  )
}

spark_api_read_generic <- function(api, path, fileMethod) {
  read <- invoke(spark_sql_or_hive(api), "read")
  invoke(read, fileMethod, path)
}

spark_api_write_generic <- function(df, path, fileMethod) {
  write <- invoke(df, "write")
  invoke(write, fileMethod, path)

  invisible(TRUE)
}


spark_inspect <- function(jobj) {
  print(jobj)
  if (!spark_connection_is_open(spark_connection(jobj)))
    return(jobj)

  class <- invoke(jobj, "getClass")

  cat("Fields:\n")
  fields <- invoke(class, "getDeclaredFields")
  lapply(fields, function(field) { print(field) })

  cat("Methods:\n")
  methods <- invoke(class, "getDeclaredMethods")
  lapply(methods, function(method) { print(method) })

  jobj
}
