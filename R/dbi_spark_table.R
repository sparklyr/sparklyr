
setMethod("dbWriteTable", "DBISparkConnection",
  function(conn, name, value, temporary = TRUE, repartition = 0, local_file = NULL) {
    if (!temporary) {
      stop("Writting to non-temporary tables is not supported yet")
    }
    
    if (!conn@scon$isLocal && identical(local_file, TRUE)) {
      stop("Using a local file to copy data is not supported for remote clusters")
    }
    
    local_file <- if (is.null(local_file)) conn@scon$isLocal else local_file

    found <- dbExistsTable(conn, name)
    if (found) {
      stop("Table ", name, " already exists")
    }

    spark_api_copy_data(conn@api, value, name, repartition, local_file)

    invisible(TRUE)
  }
)

setMethod("dbReadTable", c("DBISparkConnection", "character"),
  function(conn, name) {
    name <- dbQuoteIdentifier(conn, name)
    dbGetQuery(conn, paste("SELECT * FROM ", name))
  }
)


setMethod("dbListTables", "DBISparkConnection", function(conn) {
  df <- spark_api_sql_tables(conn@api)
  df$tableName
})


setMethod("dbExistsTable", c("DBISparkConnection", "character"), function(conn, name) {
  name %in% dbListTables(conn)
})


setMethod("dbRemoveTable", c("DBISparkConnection", "character"),
  function(conn, name) {
    spark_drop_temp_table(conn@api, name)

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
