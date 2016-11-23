#' @import dplyr

#' @export
spark_connection.tbl_spark <- function(x, ...) {
  spark_connection(x$src)
}

#' @export
spark_connection.src_spark <- function(x, ...) {
  x$con
}

#' @export
src_desc.src_spark <- function(x) {
  sc <- spark_connection(x)
  paste("spark connection",
        paste("master", sc$master, sep = "="),
        paste("app", sc$app_name, sep = "="),
        paste("local", spark_connection_is_local(sc), sep = "="))
}

#' @export
db_explain.src_spark <- function(con, sql, ...) {
  ""
}

#' @export
tbl_vars.spark_jobj <- function(x) {
  as.character(invoke(x, "columns"))
}

#' @export
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
tbl.spark_connection <- function(src, from, ...) {
  src <- src_sql("spark", src)
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
src_tbls.spark_connection <- function(x, ...) {
  sql <- hive_context(x)
  tbls <- invoke(sql, "sql", "SHOW TABLES")
  tableNames <- sdf_read_column(tbls, "tableName")

  filtered <- grep("^sparklyr_tmp_", tableNames, invert = TRUE, value = TRUE)
  sort(filtered)
}

#' @export
db_data_type.src_spark <- function(...) {
}


#' Copy an R Data Frame to Spark
#'
#' Copy an R \code{data.frame} to Spark, and return a reference to the
#' generated Spark DataFrame as a \code{tbl_spark}. The returned object will
#' act as a \code{dplyr}-compatible interface to the underlying Spark table.
#'
#' @param dest A \code{spark_connection}.
#' @param df An \R \code{data.frame}.
#' @param name The name to assign to the copied table in Spark.
#' @param memory Boolean; should the table be cached into memory?
#' @param repartition The number of partitions to use when distributing the
#'   table across the Spark cluster. The default (0) can be used to avoid
#'   partitioning.
#' @param overwrite Boolean; overwrite a pre-existing table with the name \code{name}
#'   if one already exists?
#' @param ... Optional arguments; currently unused.
#'
#' @return A \code{tbl_spark}, representing a \code{dplyr}-compatible interface
#'   to a Spark DataFrame.
#'
#' @export
copy_to.spark_connection <- function(dest,
                                     df,
                                     name = deparse(substitute(df)),
                                     memory = TRUE,
                                     repartition = 0L,
                                     overwrite = FALSE,
                                     ...)
{
  sdf_copy_to(dest, df, name, memory, repartition, overwrite)
}

#' @export
copy_to.src_spark <- function(dest, df, name, ...) {
  copy_to(spark_connection(dest), df, name, ...)
}

#' @export
print.src_spark <- function(x, ...) {
  cat(src_desc(x))
  cat("\n\n")

  spark_log(spark_connection(x))
}

#' @export
db_save_query.spark_connection <- function(con, sql, name, temporary = TRUE, ...)
{
  df <- spark_dataframe(con, sql)
  invoke(df, "registerTempTable", name)
}
