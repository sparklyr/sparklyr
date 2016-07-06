
spark_api_schema <- function(sqlResult) {
  invoke(
    sqlResult,
    "schema"
  )
}

spark_api_object_method <- function(object, property) {
  invoke(
    object,
    property
  )
}

spark_api_field <- function(field) {
  name <- spark_api_object_method(field, "name")
  dataType <- spark_api_object_method(field, "dataType")
  longType <- spark_api_object_method(dataType, "toString")
  shortType <- spark_api_object_method(dataType, "simpleString")

  list(
    name = name,
    longType = longType,
    shortType = shortType
  )
}

spark_api_schema_fields <- function(schemaResult) {
  lapply(
    invoke(
      schemaResult,
      "fields"
    ),
    function (field) {
      spark_api_field(field)
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

  result <- lapply(seq_len(rows), function(row) {
    raw <- stringData[[(col - 1) * rows + row]]

    switch(shortType,
           tinyint = as.integer(raw),
           bigint = as.numeric(raw),
           smallint = as.integer(raw),
           string = raw,
           double = as.double(raw),
           int = as.integer(raw),
           boolean = as.logical(raw),
           vector = invoke(raw, "toArray"),
           raw)
  })
  
  if (!shortType %in% c("vector")) unlist(result) else result
}

spark_api_data_frame <- function(sc, sqlResult) {
  schema <- spark_api_schema(sqlResult)
  fields <- spark_api_schema_fields(schema)

  df <- invoke_static(
    sc,

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
    df <- as.data.frame(seq_len(rows))
    
    lapply(seq_along(fields), function(col) {
      df[[dfNames[[col]]]] <<- spark_api_data_frame_columns_typed(
        col,
        stringData,
        fields,
        rows)
    })
    
    df[[1]] <- NULL
  }

  df
}

spark_api_build_types <- function(sc, columns) {
  names <- names(columns)
  fields <- lapply(names, function(name) {
    invoke_static(sc, "org.apache.spark.sql.api.r.SQLUtils", "createStructField", name, columns[[name]], TRUE)
  })

  invoke_static(sc, "org.apache.spark.sql.api.r.SQLUtils", "createStructType", fields)
}

spark_api_copy_data <- function(sc, df, name, repartition, local_file = TRUE) {
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
    df <- spark_api_read_csv(sc, tempfile, csvOptions = list(
      header = "true"
    ), columns = columns)

    if (repartition > 0) {
      df <- invoke(df, "repartition", as.integer(repartition))
    }
  } else {
    structType <- spark_api_build_types(sc, columns)
    
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
      sc,
      "utils",
      "createDataFrame",
      spark_context(sc),
      rows,
      as.integer(if (repartition <= 0) 1 else repartition)
    )

    df <- invoke(hive_context(sc), "createDataFrame", rdd, structType)
  }

  spark_register_temp_table(df, name)
}

spark_register_temp_table <- function(table, name) {
  invoke(table, "registerTempTable", name)
}

spark_drop_temp_table <- function(sc, name) {
  hive <- hive_context(sc)
  if (is_spark_v2(sc)) {
    context <- invoke(context, "wrapped")
  }
  
  invoke(context, "dropTempTable", name)
}

spark_print_schema <- function(sc, tableName) {
  result <- spark_api_sql(
    sc,
    paste("SELECT * FROM", tableName, "LIMIT 1")
  )

  invoke(
    result,
    "printSchema"
  )
}

