
setMethod(
  "dbWriteTable", "spark_connection",
  function(conn, name, value, temporary = getOption("sparklyr.dbwritetable.temp", FALSE), append = FALSE, repartition = 0, serializer = NULL) {
    found <- dbExistsTable(conn, name)
    if (found) {
      stop("Table ", name, " already exists")
    }

    temp_name <- if (identical(temporary, FALSE)) random_string("sparklyr_tmp_") else name

    spark_data_copy(conn, value, temp_name, repartition, serializer = serializer)

    if (identical(temporary, FALSE)) {
      spark_write_table(
        tbl(conn, temp_name),
        name,
        if (identical(append, TRUE)) "append" else NULL
      )
    }

    invisible(TRUE)
  }
)

setMethod(
  "dbReadTable", c("spark_connection", "character"),
  function(conn, name) {
    name <- dbQuoteIdentifier(conn, name)
    dbGetQuery(conn, paste("SELECT * FROM ", name))
  }
)


setMethod("dbListTables", "spark_connection", function(conn) {
  df <- df_from_sql(conn, "SHOW TABLES")

  if (nrow(df) <= 0) {
    character(0)
  }
  else {
    tableNames <- df$tableName
    filtered <- grep("^sparklyr_tmp_", tableNames, invert = TRUE, value = TRUE)
    sort(filtered)
  }
})


setMethod("dbExistsTable", c("spark_connection", "character"), function(conn, name) {
  name %in% dbListTables(conn)
})


setMethod(
  "dbRemoveTable", c("spark_connection", "character"),
  function(conn, name) {
    dbi_ensure_no_backtick(name)

    dbSendQuery(conn, paste0("DROP TABLE `", name, "`"))
    invisible(TRUE)
  }
)
