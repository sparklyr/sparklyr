
setMethod("dbWriteTable", "spark_connection",
  function(conn, name, value, temporary = TRUE, repartition = 0, serializer = NULL) {
    if (!temporary) {
      stop("Writting to non-temporary tables is not supported yet")
    }

    found <- dbExistsTable(conn, name)
    if (found) {
      stop("Table ", name, " already exists")
    }

    spark_data_copy(conn, value, name, repartition, serializer = serializer)

    invisible(TRUE)
  }
)

setMethod("dbReadTable", c("spark_connection", "character"),
  function(conn, name) {
    name <- dbQuoteIdentifier(conn, name)
    dbGetQuery(conn, paste("SELECT * FROM ", name))
  }
)


setMethod("dbListTables", "spark_connection", function(conn) {
  df <- df_from_sql(conn, "SHOW TABLES")

  tableNames <- df$tableName
  filtered <- grep("^sparklyr_tmp_", tableNames, invert = TRUE, value = TRUE)
  sort(filtered)
})


setMethod("dbExistsTable", c("spark_connection", "character"), function(conn, name) {
  name %in% dbListTables(conn)
})


setMethod("dbRemoveTable", c("spark_connection", "character"),
  function(conn, name) {
    hive <- hive_context(conn)
    if (is_spark_v2(conn)) {
      hive <- invoke(hive, "sqlContext")
    }
    invoke(hive, "dropTempTable", name)
    invisible(TRUE)
  }
)


#' @export
mutate_.tbl_spark <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)

  data <- .data
  lapply(seq_along(dots), function(i) {
    data <<- dplyr::add_op_single("mutate", data, dots = dots[i])
  })

  data
}
