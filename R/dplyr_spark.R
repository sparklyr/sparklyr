#' @export
spark_connection.tbl_spark <- function(x, ...) {
  spark_connection(x$src)
}

#' @export
spark_connection.src_spark <- function(x, ...) {

  # for development version of dplyr (>= 0.5.0.9000)
  if ("dplyr" %in% loadedNamespaces() &&
    exists("con_acquire", envir = asNamespace("dplyr"))) {
    acquire <- get("con_acquire", envir = asNamespace("dplyr"))
    return(acquire(x))
  }

  # older versions of dplyr (0.5.0 and below)
  x$con
}

#' @export
#' @importFrom dplyr db_desc
db_desc.src_spark <- function(x) {
  sc <- spark_connection(x)
  paste(
    "spark connection",
    paste("master", sc$master, sep = "="),
    paste("app", sc$app_name, sep = "="),
    paste("local", spark_connection_is_local(sc), sep = "=")
  )
}

#' @export
#' @importFrom dplyr db_explain
db_explain.spark_connection <- function(con, sql, ...) {
  explained <- DBI::dbGetQuery(con, paste("EXPLAIN", sql))

  message(explained$plan)
}

#' @export
#' @importFrom dplyr tbl_vars
tbl_vars.spark_jobj <- function(x) {
  as.character(invoke(x, "columns"))
}

#' @export
#' @importFrom dbplyr tbl_sql
tbl.src_spark <- function(src, from, ...) {
  tbl_sql("spark", src = src, from = from, ...)
}

#' @export
#' @importFrom dbplyr src_sql
#' @importFrom dbplyr tbl_sql
tbl.spark_connection <- function(src, from, ...) {
  src <- src_sql("spark", src)
  tbl_sql("spark", src = src, from = from, ...)
}


#' @export
#' @importFrom dplyr src_tbls
src_tbls.spark_connection <- function(x, ...) {
  sql <- hive_context(x)
  tbls <- invoke(sql, "sql", "SHOW TABLES")
  tableNames <- sdf_read_column(tbls, "tableName")

  filtered <- grep("^sparklyr_tmp_", tableNames, invert = TRUE, value = TRUE)
  sort(filtered)
}

#' @export
#' @importFrom dplyr db_data_type
db_data_type.spark_connection <- function(...) {
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
#' @importFrom dplyr copy_to
copy_to.spark_connection <- function(dest,
                                     df,
                                     name = spark_table_name(substitute(df)),
                                     overwrite = FALSE,
                                     memory = TRUE,
                                     repartition = 0L,
                                     ...) {
  sdf_copy_to(dest, df, name, memory, repartition, overwrite, ...)
}

#' @export
#' @importFrom dplyr copy_to
copy_to.src_spark <- function(dest, df, name, overwrite, ...) {
  copy_to(spark_connection(dest), df, name, ...)
}

#' @export
#' @importFrom dplyr db_desc
print.src_spark <- function(x, ...) {
  cat(db_desc(x))
  cat("\n\n")

  spark_log(spark_connection(x))
}

#' @export
#' @importFrom dplyr db_save_query
db_save_query.spark_connection <- function(con, sql, name, temporary = TRUE, ...) {
  df <- spark_dataframe(con, sql)
  sdf_register(df, name)

  # compute() is expected to preserve the query, cache as the closest mapping.
  tbl_cache(con, name)

  # dbplyr expects db_save_query to retrieve the table name
  name
}

#' @export
#' @importFrom dplyr db_analyze
#' @importFrom dbplyr build_sql
#' @importFrom DBI dbExecute
db_analyze.spark_connection <- function(con, table, ...) {
  if (spark_version(con) < "2.0.0") {
    return(NULL)
  }

  info <- dbGetQuery(con, build_sql("SHOW TABLES LIKE", table, con = con))
  if (nrow(info) > 0 && identical(info$isTemporary, FALSE)) {
    dbExecute(con, build_sql("ANALYZE TABLE", table, "COMPUTE STATISTICS", con = con))
  }
}

#' @export
#' @importFrom dplyr same_src
same_src.src_spark <- function(x, y) {
  if (!inherits(y, "src_spark")) {
    return(FALSE)
  }

  # By default, dplyr uses identical(x$con, y$con); however,
  # sparklyr connection might slightly change due to the state
  # it manages.

  identical(x$con$master, y$con$master) &&
    identical(x$con$app_name, y$con$app_name) &&
    identical(x$con$method, y$con$method)
}
