#' DBI interface for Spark
#'
#' @param conn a \code{\linkS4class{DBISparkConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param name a character string specifying a table name.
#' @param value A data.frame to write to the database.
#' @inheritParams DBI::sqlCreateTable
#' @examples
#' \dontrun{
#' sc <- spark_connect()
#' con <- dbConnect(spark::DBISpark(sc))
#'
#' dbListTables(con)
#' dbWriteTable(con, "mtcars", mtcars, temporary = TRUE)
#' dbReadTable(con, "mtcars")
#'
#' dbListTables(con)
#' dbExistsTable(con, "mtcars")
#'
#' # A zero row data frame just creates a table definition.
#' dbWriteTable(con, "mtcars2", mtcars[0, ], temporary = TRUE)
#' dbReadTable(con, "mtcars2")
#'
#' dbDisconnect(con)
#' }
#' @name DBI-interface
#' @keywords internal
NULL

#' @export
#' @rdname DBI-interface
#' @param repartition Total of partitions used to distribute table or 0 (default) to avoid partitioning
#' @param local_file When TRUE, uses a local file to copy the data frame, this is only available in local installs.
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

#' @export
#' @rdname DBI-interface
setMethod("dbReadTable", c("DBISparkConnection", "character"),
  function(conn, name) {
    name <- dbQuoteIdentifier(conn, name)
    dbGetQuery(conn, paste("SELECT * FROM ", name))
  }
)

#' @export
#' @rdname DBI-interface
setMethod("dbListTables", "DBISparkConnection", function(conn) {
  df <- spark_api_sql_tables(conn@api)
  df$tableName
})

#' @export
#' @rdname DBI-interface
setMethod("dbExistsTable", c("DBISparkConnection", "character"), function(conn, name) {
  name %in% dbListTables(conn)
})

#' @export
#' @rdname DBI-interface
setMethod("dbRemoveTable", c("DBISparkConnection", "character"),
  function(conn, name) {
    spark_drop_temp_table(conn@api, name)

    invisible(TRUE)
  }
)

#' @export
#' @rdname DBI-interface
#' @param .data Data and operations references
#' @param ... Additional parameters
#' @param .dots Original parameters
mutate_.tbl_spark <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)

  data <- .data
  lapply(seq_along(dots), function(i) {
    data <<- dplyr::add_op_single("mutate", data, dots = dots[i])
  })

  data
}
