#' DBI Spark Table
#'
#' @param conn a \code{\linkS4class{DBISparkConnection}} object, produced by
#'   \code{\link[DBI]{dbConnect}}
#' @param name a character string specifying a table name.
#' @param value A data.frame to write to the database.
#' @inheritParams DBI::sqlCreateTable
#' @param overwrite a logical specifying whether to overwrite an existing table
#'   or not. Its default is \code{FALSE}.
#' @param append data to table. Its default is \code{FALSE}.
#' @param field.types character vector of named SQL field types where
#'   the names are the names of new table's columns. If missing, types inferred
#'   with \code{\link[DBI]{dbDataType}}).
#' @param Not supported.
#' @examples
#' @dontrun
#' con <- dbConnect(spark::DBISpark())
#'
#' setup_local()
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
#' @name dbi-spark-table
NULL

#' @export
#' @rdname dbi-spark-table
setMethod("dbWriteTable", "DBISparkConnection",
  function(conn, name, value, temporary = TRUE) {
    if (!temporary) {
      stop("Writting to non-temporary tables is not supported yet")
    }

    found <- dbExistsTable(conn, name)
    if (found) {
      stop("Table ", name, " already exists")
    }

    spark_api_copy_data(conn@con, value, name)

    TRUE
  }
)

#' @export
#' @rdname dbi-spark-table
setMethod("dbReadTable", c("DBISparkConnection", "character"),
  function(conn, name) {
    name <- dbQuoteIdentifier(conn, name)
    dbGetQuery(conn, paste("SELECT * FROM ", name))
  }
)

#' @export
#' @rdname dbi-spark-table
setMethod("dbListTables", "DBISparkConnection", function(conn) {
  df <- spark_api_sql_tables(conn@con)
  df$tableName
})

#' @export
#' @rdname dbi-spark-table
setMethod("dbExistsTable", c("DBISparkConnection", "character"), function(conn, name) {
  name %in% dbListTables(conn)
})

#' @export
#' @rdname dbi-spark-table
setMethod("dbRemoveTable", c("DBISparkConnection", "character"),
  function(conn, name) {
    spark_drop_temp_table(conn@con, name)

    TRUE
  }
)

#' @export
#' @rdname dbi-spark-table
mutate_.tbl_spark <- function(.data, ..., .dots) {
  dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)

  data <- .data
  lapply(seq_along(dots), function(i) {
    data <<- dplyr:::add_op_single("mutate", data, dots = dots[i])
  })

  data
}
